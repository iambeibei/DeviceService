using Bluetti.Device.Redis;
using Bluetti.Device.Redis.Models;
using DeviceService.Mqtt;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;

namespace DeviceService.Services;

/// <summary>
/// 按 AgingNumber 隔离的老化群控协调服务。
/// </summary>
public sealed class GroupControlService : BackgroundService
{
    private readonly GroupControlRepository _repository;
    private readonly DeviceRepository _devices;
    private readonly MqttConnection _mqtt;
    private readonly ILogger<GroupControlService> _logger;
    private readonly TimeSpan _retryDelay;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _rackLocks =
        new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, GroupControlState> _states =
        new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> _runRecoveryChecked =
        new(StringComparer.Ordinal);

    public GroupControlService(
        GroupControlRepository repository,
        DeviceRepository devices,
        MqttConnection mqtt,
        IConfiguration configuration,
        ILogger<GroupControlService> logger)
    {
        _repository = repository;
        _devices = devices;
        _mqtt = mqtt;
        _logger = logger;

        var retryMilliseconds = configuration.GetValue<int?>(
            "CommandProcessing:PublishRetryDelayMilliseconds") ?? 1000;
        if (retryMilliseconds <= 0)
            throw new InvalidOperationException(
                "CommandProcessing:PublishRetryDelayMilliseconds 必须大于 0");

        _retryDelay = TimeSpan.FromMilliseconds(retryMilliseconds);
    }

    /// <summary>
    /// 登记一条 Redis Stream start_aging 批次的候选参与设备。
    /// 这里只保存候选批次；只有设备真实上报 agingStart 后才会激活。
    /// 候选登记不依赖 mode 消息与命令的到达顺序；Mode=1 下不会激活群控批次。
    /// </summary>
    public async Task RegisterAgingBatchAsync(
        IReadOnlyList<CmdInfo> commands,
        CancellationToken cancellationToken)
    {
        if (commands.Count == 0)
            return;

        var groups = new Dictionary<(string AgingNumber, long RunSeq), HashSet<string>>();
        foreach (var command in commands)
        {
            if (!string.Equals(command.Action, "start_aging", StringComparison.OrdinalIgnoreCase) ||
                command.Seq <= 0)
            {
                continue;
            }

            var device = await _devices.GetAsync(command.DeviceId);
            if (device is null ||
                !string.Equals(device.DeviceType, "1", StringComparison.Ordinal) ||
                string.IsNullOrWhiteSpace(device.AgingNumber))
            {
                _logger.LogWarning(
                    "start_aging 批次设备缺少有效群控归属，不登记候选批次，DeviceId={DeviceId}, Seq={Seq}, Type={Type}, AgingNumber={AgingNumber}",
                    command.DeviceId,
                    command.Seq,
                    device?.DeviceType ?? "<empty>",
                    device?.AgingNumber ?? "<empty>");
                continue;
            }

            var agingNumber = RedisKeys.NormalizeAgingNumber(device.AgingNumber);
            var key = (agingNumber, command.Seq);
            if (!groups.TryGetValue(key, out var participants))
            {
                participants = new HashSet<string>(StringComparer.Ordinal);
                groups[key] = participants;
            }

            participants.Add(RedisKeys.Normalize(command.DeviceId));
        }

        foreach (var group in groups)
        {
            var (agingNumber, runSeq) = group.Key;
            var rackLock = GetRackLock(agingNumber);
            await rackLock.WaitAsync(cancellationToken);
            try
            {
                var runState = await _repository.GetRunStateAsync(agingNumber, runSeq)
                    ?? new GroupControlRunState
                    {
                        AgingNumber = agingNumber,
                        RunSeq = runSeq,
                        Status = GroupControlRunStatuses.Candidate
                    };

                var existing = await _repository.GetRunParticipantsAsync(agingNumber, runSeq);
                if (existing.Count > 0 && !existing.SetEquals(group.Value))
                {
                    _logger.LogWarning(
                        "重复登记的群控批次参与设备不一致，将保留已有成员并补充本次成员，AgingNumber={AgingNumber}, RunSeq={RunSeq}, Existing={Existing}, Incoming={Incoming}",
                        agingNumber,
                        runSeq,
                        string.Join(',', existing),
                        string.Join(',', group.Value));
                }

                await _repository.AddRunParticipantsAsync(agingNumber, runSeq, group.Value);
                await _repository.SaveRunStateAsync(agingNumber, runState);

                _logger.LogInformation(
                    "已登记群控候选批次，AgingNumber={AgingNumber}, RunSeq={RunSeq}, ParticipantCount={ParticipantCount}",
                    agingNumber,
                    runSeq,
                    group.Value.Count);
            }
            finally
            {
                rackLock.Release();
            }
        }
    }

    /// <summary>
    /// 设备真实老化状态驱动候选批次激活和当前批次结束。
    /// 命令已接收/已发布不代表批次生效，只有 agingStart 才激活。
    /// </summary>
    public async Task HandleAgingStateAsync(
        string deviceId,
        long? runSeq,
        string agingState)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        var normalizedState = agingState?.Trim().ToLowerInvariant();
        var isStart = normalizedState == "agingstart";
        var isTerminal = normalizedState is "agingidle" or "agingstop" or "agingerror";
        if (!isStart && !isTerminal)
            return;

        var device = await _devices.GetAsync(deviceId);
        if (device is null ||
            !string.Equals(device.DeviceType, "1", StringComparison.Ordinal) ||
            string.IsNullOrWhiteSpace(device.AgingNumber))
        {
            return;
        }

        var agingNumber = RedisKeys.NormalizeAgingNumber(device.AgingNumber);
        var rackLock = GetRackLock(agingNumber);
        await rackLock.WaitAsync();
        try
        {
            var state = await GetStateUnsafeAsync(agingNumber);
            if (state.Mode != 2)
                return;

            if (!runSeq.HasValue || runSeq.Value <= 0)
            {
                _logger.LogWarning(
                    "群控状态缺少可关联的 start_aging Seq，忽略批次状态更新，AgingNumber={AgingNumber}, DeviceId={DeviceId}, AgingState={AgingState}",
                    agingNumber,
                    deviceId,
                    agingState);
                return;
            }

            if (isStart)
            {
                var activated = await ActivateRunUnsafeAsync(
                    agingNumber,
                    state,
                    runSeq.Value,
                    deviceId,
                    "设备真实上报 agingStart");

                // 兼容升级前已经发布、但因 mode 消息到达较晚而没有登记候选批次的任务。
                // 恢复只依据真实 agingStart + 设备自身的 CurrentAgingSeq/TaskQueue，
                // 不使用在线列表，也不会由 TaskCP 反向创建批次。
                if (!activated && !state.CurrentRunSeq.HasValue)
                    await RecoverCurrentRunUnsafeAsync(agingNumber, state);

                return;
            }

            var runState = await _repository.GetRunStateAsync(agingNumber, runSeq.Value);
            var participants = await _repository.GetRunParticipantsAsync(agingNumber, runSeq.Value);
            if (runState is null || !participants.Contains(deviceId))
            {
                _logger.LogWarning(
                    "设备状态无法匹配已登记群控批次，AgingNumber={AgingNumber}, DeviceId={DeviceId}, RunSeq={RunSeq}, AgingState={AgingState}",
                    agingNumber,
                    deviceId,
                    runSeq.Value,
                    agingState);
                return;
            }

            if (state.CurrentRunSeq != runSeq.Value)
            {
                _logger.LogWarning(
                    "收到非当前群控批次的终态，不改变当前批次，AgingNumber={AgingNumber}, CurrentRunSeq={CurrentRunSeq}, IncomingRunSeq={IncomingRunSeq}, DeviceId={DeviceId}, AgingState={AgingState}",
                    agingNumber,
                    state.CurrentRunSeq?.ToString() ?? "<empty>",
                    runSeq.Value,
                    deviceId,
                    agingState);
                return;
            }

            await _repository.AddRunTerminalDeviceAsync(
                agingNumber,
                runSeq.Value,
                deviceId);
            var terminal = await _repository.GetRunTerminalDeviceIdsAsync(
                agingNumber,
                runSeq.Value);

            if (!participants.All(terminal.Contains))
                return;

            runState.Status = GroupControlRunStatuses.Finished;
            runState.FinishedAtUtc = DateTime.UtcNow;
            state.CurrentRunSeq = null;
            state.PendingNextStep = string.Empty;
            await _repository.ClearRunCompletedDevicesAsync(agingNumber, runSeq.Value);
            await _repository.SaveRunStateAsync(agingNumber, runState);
            await _repository.SaveStateAsync(agingNumber, state);

            _logger.LogInformation(
                "群控批次全部参与设备已进入终态，批次结束，AgingNumber={AgingNumber}, RunSeq={RunSeq}, ParticipantCount={ParticipantCount}",
                agingNumber,
                runSeq.Value,
                participants.Count);
        }
        finally
        {
            rackLock.Release();
        }
    }

    public async Task UpdateModeAsync(string agingNumber, int mode, long seq)
    {
        agingNumber = RedisKeys.NormalizeAgingNumber(agingNumber);
        if (mode is not (1 or 2))
            throw new ArgumentOutOfRangeException(nameof(mode), "Mode 只允许 1 或 2");
        if (seq <= 0)
            throw new ArgumentOutOfRangeException(nameof(seq), "Mode Seq 必须大于 0");

        var rackLock = GetRackLock(agingNumber);
        await rackLock.WaitAsync();
        try
        {
            var state = await GetStateUnsafeAsync(agingNumber);
            var changed = state.Mode != mode;

            if (changed && mode == 2)
                await _repository.ClearCompletedDevicesAsync(agingNumber);

            state.LastModeSeq = seq;
            if (mode == 1)
            {
                if (state.CurrentRunSeq.HasValue)
                {
                    var currentRun = await _repository.GetRunStateAsync(
                        agingNumber,
                        state.CurrentRunSeq.Value);
                    if (currentRun is not null)
                    {
                        currentRun.Status = GroupControlRunStatuses.Cancelled;
                        currentRun.FinishedAtUtc = DateTime.UtcNow;
                        await _repository.SaveRunStateAsync(agingNumber, currentRun);
                    }
                }

                state.Mode = 1;
                if (changed)
                    state.Generation++;
                ResetRackActivity(state);
            }
            else if (changed)
            {
                state.Mode = 2;
                state.Generation++;
                ResetRackActivity(state);
            }

            await _repository.SaveStateAsync(agingNumber, state);

            if (mode == 1)
            {
                await _repository.ClearCompletedDevicesAsync(agingNumber);
                _runRecoveryChecked.TryRemove(agingNumber, out _);
            }
            else
            {
                await RecoverCurrentRunUnsafeAsync(agingNumber, state);
                _runRecoveryChecked[agingNumber] = 0;
            }

            _logger.LogInformation(
                "老化架群控模式已更新，AgingNumber={AgingNumber}, Mode={Mode}, Seq={Seq}, Changed={Changed}, Generation={Generation}",
                agingNumber,
                mode,
                seq,
                changed,
                state.Generation);
        }
        finally
        {
            rackLock.Release();
        }
    }

    public async Task RecordTaskCompletionAsync(string deviceId, long seq)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        if (seq <= 0)
            throw new ArgumentOutOfRangeException(nameof(seq), "TaskCP Seq 必须大于 0");

        var device = await _devices.GetAsync(deviceId);
        if (string.IsNullOrWhiteSpace(device?.AgingNumber) ||
            !string.Equals(device.DeviceType, "1", StringComparison.Ordinal))
        {
            _logger.LogWarning(
                "非点位设备或缺少 AgingNumber 的 TaskCP 不参与群控，DeviceId={DeviceId}, Type={Type}, AgingNumber={AgingNumber}",
                deviceId,
                device?.DeviceType ?? "<empty>",
                device?.AgingNumber ?? "<empty>");
            return;
        }

        var agingNumber = RedisKeys.NormalizeAgingNumber(device.AgingNumber);
        var rackLock = GetRackLock(agingNumber);
        await rackLock.WaitAsync();
        try
        {
            var state = await GetStateUnsafeAsync(agingNumber);
            if (state.Mode != 2)
            {
                _logger.LogDebug(
                    "单控模式忽略 TaskCP，AgingNumber={AgingNumber}, DeviceId={DeviceId}, Seq={Seq}",
                    agingNumber,
                    deviceId,
                    seq);
                return;
            }

            var lastSeq = await _repository.GetLastTaskSeqAsync(agingNumber, deviceId);
            if (lastSeq.HasValue && seq <= lastSeq.Value)
            {
                _logger.LogInformation(
                    "忽略重复或乱序 TaskCP，AgingNumber={AgingNumber}, DeviceId={DeviceId}, Seq={Seq}, LastSeq={LastSeq}",
                    agingNumber,
                    deviceId,
                    seq,
                    lastSeq.Value);
                return;
            }

            if (!state.CurrentRunSeq.HasValue)
            {
                _logger.LogWarning(
                    "当前老化架没有已激活批次，忽略 TaskCP，AgingNumber={AgingNumber}, DeviceId={DeviceId}, Seq={Seq}",
                    agingNumber,
                    deviceId,
                    seq);
                return;
            }

            var runSeq = state.CurrentRunSeq.Value;
            var participants = await _repository.GetRunParticipantsAsync(agingNumber, runSeq);
            if (!participants.Contains(deviceId))
            {
                _logger.LogWarning(
                    "非当前批次参与设备的 TaskCP 不参与群控，AgingNumber={AgingNumber}, RunSeq={RunSeq}, DeviceId={DeviceId}, Seq={Seq}",
                    agingNumber,
                    runSeq,
                    deviceId,
                    seq);
                return;
            }

            await _repository.AddRunCompletedDeviceAsync(agingNumber, runSeq, deviceId);
            await _repository.SetLastTaskSeqAsync(agingNumber, deviceId, seq);

            _logger.LogInformation(
                "记录老化架阶段完成，AgingNumber={AgingNumber}, RunSeq={RunSeq}, DeviceId={DeviceId}, Seq={Seq}",
                agingNumber,
                runSeq,
                deviceId,
                seq);

            await EvaluateNextStepUnsafeAsync(
                agingNumber,
                state,
                CancellationToken.None);
        }
        finally
        {
            rackLock.Release();
        }
    }

    public async Task HandleAgingStageAsync(
        string agingNumber,
        string agingStage,
        long reportSeq)
    {
        agingNumber = RedisKeys.NormalizeAgingNumber(agingNumber);
        if (reportSeq <= 0)
            throw new ArgumentOutOfRangeException(nameof(reportSeq), "agingStage Seq 必须大于 0");

        var stage = NormalizeStage(agingStage);
        var rackLock = GetRackLock(agingNumber);
        await rackLock.WaitAsync();
        try
        {
            var state = await GetStateUnsafeAsync(agingNumber);
            if (state.Mode != 2)
            {
                _logger.LogDebug(
                    "单控模式忽略 agingStage，AgingNumber={AgingNumber}, Stage={Stage}, ReportSeq={ReportSeq}",
                    agingNumber,
                    stage,
                    reportSeq);
                return;
            }

            state.CurrentStage = stage;
            await _repository.SaveStateAsync(agingNumber, state);
            await EnsureCurrentPowerUnsafeAsync(
                agingNumber,
                state,
                reportSeq,
                CancellationToken.None);
        }
        finally
        {
            rackLock.Release();
        }
    }

    /// <summary>
    /// 点位或总控的架号/在线状态改变后，只重新评估受影响的老化架。
    /// </summary>
    public async Task ReevaluateMembershipAsync(
        string? previousAgingNumber,
        string? currentAgingNumber)
    {
        var rackNumbers = new HashSet<string>(StringComparer.Ordinal);
        TryAddRackNumber(rackNumbers, previousAgingNumber);
        TryAddRackNumber(rackNumbers, currentAgingNumber);

        await Task.WhenAll(rackNumbers.Select(number =>
            ProcessRackAsync(number, CancellationToken.None)));
    }

    private void TryAddRackNumber(HashSet<string> rackNumbers, string? agingNumber)
    {
        if (string.IsNullOrWhiteSpace(agingNumber))
            return;

        try
        {
            rackNumbers.Add(RedisKeys.NormalizeAgingNumber(agingNumber));
        }
        catch (ArgumentException ex)
        {
            _logger.LogWarning(
                ex,
                "忽略非法的老化架号，AgingNumber={AgingNumber}",
                agingNumber);
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "多老化架群控协调服务已启动，RetryDelayMs={RetryDelayMs}",
            _retryDelay.TotalMilliseconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var rackNumbers = await _repository.GetRackNumbersAsync();
                await Task.WhenAll(rackNumbers.Select(number =>
                    ProcessRackAsync(number, stoppingToken)));
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "多老化架群控状态恢复或 pending 重试失败");
            }

            try
            {
                await Task.Delay(_retryDelay, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    /// <summary>
    /// 激活已经登记的候选批次。调用方必须持有对应老化架锁。
    /// </summary>
    private async Task<bool> ActivateRunUnsafeAsync(
        string agingNumber,
        GroupControlState state,
        long runSeq,
        string triggerDeviceId,
        string activationReason)
    {
        var runState = await _repository.GetRunStateAsync(agingNumber, runSeq);
        var participants = await _repository.GetRunParticipantsAsync(agingNumber, runSeq);
        if (runState is null || !participants.Contains(triggerDeviceId))
        {
            _logger.LogWarning(
                "设备状态无法匹配已登记群控批次，AgingNumber={AgingNumber}, DeviceId={DeviceId}, RunSeq={RunSeq}, RunExists={RunExists}, ParticipantCount={ParticipantCount}",
                agingNumber,
                triggerDeviceId,
                runSeq,
                runState is not null,
                participants.Count);
            return false;
        }

        if (state.CurrentRunSeq.HasValue && state.CurrentRunSeq.Value != runSeq)
        {
            runState.Status = GroupControlRunStatuses.Conflict;
            await _repository.SaveRunStateAsync(agingNumber, runState);
            _logger.LogError(
                "当前群控批次尚未结束，拒绝切换到提前启动的新批次，AgingNumber={AgingNumber}, CurrentRunSeq={CurrentRunSeq}, IncomingRunSeq={IncomingRunSeq}, DeviceId={DeviceId}",
                agingNumber,
                state.CurrentRunSeq.Value,
                runSeq,
                triggerDeviceId);
            return false;
        }

        if (!state.CurrentRunSeq.HasValue)
        {
            if (runState.Status is GroupControlRunStatuses.Finished or
                GroupControlRunStatuses.Cancelled or
                GroupControlRunStatuses.Conflict)
            {
                _logger.LogWarning(
                    "已终止的群控批次不能再次激活，AgingNumber={AgingNumber}, RunSeq={RunSeq}, Status={Status}",
                    agingNumber,
                    runSeq,
                    runState.Status);
                return false;
            }

            await _repository.ClearRunCompletedDevicesAsync(agingNumber, runSeq);
            state.CurrentRunSeq = runSeq;
            state.PendingNextStep = string.Empty;
            runState.Status = GroupControlRunStatuses.Active;
            runState.ActivatedAtUtc ??= DateTime.UtcNow;
            await _repository.SaveRunStateAsync(agingNumber, runState);
            await _repository.SaveStateAsync(agingNumber, state);

            _logger.LogInformation(
                "{ActivationReason}，已激活群控批次，AgingNumber={AgingNumber}, RunSeq={RunSeq}, ParticipantCount={ParticipantCount}",
                activationReason,
                agingNumber,
                runSeq,
                participants.Count);
        }

        await _repository.AddRunStartedDeviceAsync(
            agingNumber,
            runSeq,
            triggerDeviceId);
        return true;
    }

    /// <summary>
    /// 恢复升级前漏登记或服务重启时尚未写入 CurrentRunSeq 的真实运行批次。
    /// 调用方必须持有对应老化架锁。
    /// </summary>
    private async Task RecoverCurrentRunUnsafeAsync(
        string agingNumber,
        GroupControlState state)
    {
        if (state.Mode != 2 || state.CurrentRunSeq.HasValue)
            return;

        var devices = await _devices.GetAllAsync();
        var rackDevices = new List<DeviceNode>();
        foreach (var device in devices)
        {
            if (!string.Equals(device.DeviceType, "1", StringComparison.Ordinal) ||
                string.IsNullOrWhiteSpace(device.AgingNumber))
            {
                continue;
            }

            try
            {
                if (string.Equals(
                        RedisKeys.NormalizeAgingNumber(device.AgingNumber),
                        agingNumber,
                        StringComparison.Ordinal))
                {
                    rackDevices.Add(device);
                }
            }
            catch (ArgumentException)
            {
                // 非法历史归属不参与批次恢复。
            }
        }

        var activeDevices = new List<(string DeviceId, long RunSeq)>();
        foreach (var device in rackDevices)
        {
            if (!string.Equals(
                    device.AgingState?.Trim(),
                    "agingStart",
                    StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var deviceRunSeq = await _devices.GetCurrentAgingTaskIdAsync(device.DeviceId);
            if (deviceRunSeq.HasValue && deviceRunSeq.Value > 0)
                activeDevices.Add((RedisKeys.Normalize(device.DeviceId), deviceRunSeq.Value));
        }

        var activeRunSeqs = activeDevices
            .Select(item => item.RunSeq)
            .Distinct()
            .ToArray();
        if (activeRunSeqs.Length == 0)
            return;

        if (activeRunSeqs.Length > 1)
        {
            _logger.LogError(
                "同一老化架检测到多个真实运行批次，拒绝自动恢复，AgingNumber={AgingNumber}, RunSeqs={RunSeqs}",
                agingNumber,
                string.Join(',', activeRunSeqs.OrderBy(value => value)));
            return;
        }

        var runSeq = activeRunSeqs[0];
        var runState = await _repository.GetRunStateAsync(agingNumber, runSeq);
        var participants = await _repository.GetRunParticipantsAsync(agingNumber, runSeq);
        if (runState is null || participants.Count == 0)
        {
            if (runState?.Status is GroupControlRunStatuses.Finished or
                GroupControlRunStatuses.Cancelled or
                GroupControlRunStatuses.Conflict)
            {
                _logger.LogWarning(
                    "真实运行设备关联到已终止群控批次，拒绝自动恢复，AgingNumber={AgingNumber}, RunSeq={RunSeq}, Status={Status}",
                    agingNumber,
                    runSeq,
                    runState.Status);
                return;
            }

            var recoveredParticipants = new HashSet<string>(StringComparer.Ordinal);
            foreach (var device in rackDevices)
            {
                if (await _devices.HasAgingTaskAsync(device.DeviceId, runSeq))
                    recoveredParticipants.Add(RedisKeys.Normalize(device.DeviceId));
            }

            foreach (var active in activeDevices.Where(item => item.RunSeq == runSeq))
                recoveredParticipants.Add(active.DeviceId);

            if (recoveredParticipants.Count == 0)
                return;

            runState ??= new GroupControlRunState
            {
                AgingNumber = agingNumber,
                RunSeq = runSeq,
                Status = GroupControlRunStatuses.Candidate
            };

            await _repository.AddRunParticipantsAsync(
                agingNumber,
                runSeq,
                recoveredParticipants);
            await _repository.SaveRunStateAsync(agingNumber, runState);
            participants = recoveredParticipants;

            _logger.LogWarning(
                "已从设备当前任务恢复漏登记的群控候选批次，AgingNumber={AgingNumber}, RunSeq={RunSeq}, ParticipantCount={ParticipantCount}",
                agingNumber,
                runSeq,
                recoveredParticipants.Count);
        }

        var triggerDeviceId = activeDevices
            .Where(item => item.RunSeq == runSeq)
            .Select(item => item.DeviceId)
            .FirstOrDefault(participants.Contains);
        if (triggerDeviceId is null)
        {
            _logger.LogError(
                "真实运行设备不在群控批次固定参与集合中，拒绝自动恢复，AgingNumber={AgingNumber}, RunSeq={RunSeq}, ActiveDevices={ActiveDevices}",
                agingNumber,
                runSeq,
                string.Join(',', activeDevices.Select(item => item.DeviceId)));
            return;
        }

        await ActivateRunUnsafeAsync(
            agingNumber,
            state,
            runSeq,
            triggerDeviceId,
            "从设备真实运行状态恢复");
    }

    private async Task ProcessRackAsync(string agingNumber, CancellationToken cancellationToken)
    {
        agingNumber = RedisKeys.NormalizeAgingNumber(agingNumber);
        var rackLock = GetRackLock(agingNumber);
        await rackLock.WaitAsync(cancellationToken);
        try
        {
            var state = await GetStateUnsafeAsync(agingNumber);
            if (state.Mode != 2)
                return;

            if (!_runRecoveryChecked.ContainsKey(agingNumber))
            {
                await RecoverCurrentRunUnsafeAsync(agingNumber, state);
                _runRecoveryChecked[agingNumber] = 0;
            }

            await EnsureCurrentPowerUnsafeAsync(
                agingNumber,
                state,
                reportSeq: null,
                cancellationToken);
            await EvaluateNextStepUnsafeAsync(agingNumber, state, cancellationToken);
        }
        finally
        {
            rackLock.Release();
        }
    }

    private SemaphoreSlim GetRackLock(string agingNumber)
        => _rackLocks.GetOrAdd(
            RedisKeys.NormalizeAgingNumber(agingNumber),
            _ => new SemaphoreSlim(1, 1));

    private async Task<GroupControlState> GetStateUnsafeAsync(string agingNumber)
    {
        if (_states.TryGetValue(agingNumber, out var cached))
            return cached;

        var state = await _repository.GetStateAsync(agingNumber);
        state.AgingNumber = agingNumber;
        state.CurrentStage ??= string.Empty;
        state.LastAppliedStage ??= string.Empty;
        state.LastAppliedControllerId ??= string.Empty;
        state.PendingNextStep ??= string.Empty;
        state.PendingPower ??= string.Empty;

        if (state.Mode is not (1 or 2))
            state.Mode = 1;

        if (state.Mode == 1)
        {
            ResetRackActivity(state);
            await _repository.ClearCompletedDevicesAsync(agingNumber);
        }

        await _repository.SaveStateAsync(agingNumber, state);
        _states[agingNumber] = state;

        _logger.LogInformation(
            "老化架群控状态已恢复，AgingNumber={AgingNumber}, Mode={Mode}, Generation={Generation}, CurrentStage={CurrentStage}",
            agingNumber,
            state.Mode,
            state.Generation,
            string.IsNullOrEmpty(state.CurrentStage) ? "<empty>" : state.CurrentStage);

        return state;
    }

    private async Task EnsureCurrentPowerUnsafeAsync(
        string agingNumber,
        GroupControlState state,
        long? reportSeq,
        CancellationToken cancellationToken)
    {
        if (state.Mode != 2 || string.IsNullOrWhiteSpace(state.CurrentStage))
            return;

        var controllers = await _repository.GetOnlineControllerIdsAsync(agingNumber);
        var pendingValue = state.PendingPower;
        var pending = ReadPending(state, pendingValue, isPower: true);
        var pendingChanged = !string.IsNullOrWhiteSpace(pendingValue) && pending is null;

        if (pending is not null &&
            !string.Equals(pending.Stage, state.CurrentStage, StringComparison.Ordinal))
        {
            pending = null;
            state.PendingPower = string.Empty;
            pendingChanged = true;
        }

        if (pending is null)
        {
            var stageWasApplied = string.Equals(
                state.LastAppliedStage,
                state.CurrentStage,
                StringComparison.Ordinal);
            var controllerIsCurrent = controllers.Count == 1 &&
                string.Equals(
                    state.LastAppliedControllerId,
                    controllers[0],
                    StringComparison.Ordinal) &&
                string.Equals(
                    state.LastAppliedStage,
                    state.CurrentStage,
                    StringComparison.Ordinal);

            if (controllerIsCurrent)
            {
                if (pendingChanged)
                    await _repository.SaveStateAsync(agingNumber, state);
                return;
            }

            // 阶段已经下发过但当前总控归属不唯一时，只保存 CurrentStage。
            // 待总控恢复为唯一后再判断是原总控回归还是确实发生了更换。
            if (stageWasApplied && controllers.Count != 1)
            {
                if (pendingChanged)
                    await _repository.SaveStateAsync(agingNumber, state);
                GetUniqueControllerId(agingNumber, controllers, "CPower");
                return;
            }

            var powerState = state.CurrentStage == "Recharge" ? 1 : 0;
            var seq = await _repository.NextControlSequenceAsync();
            pending = new PendingGroupControlCommand
            {
                Seq = seq,
                Stage = state.CurrentStage,
                Payload = CreatePayload(seq, "PowerState", powerState)
            };
            state.PendingPower = GroupControlRepository.SerializePending(pending);
            await _repository.SaveStateAsync(agingNumber, state);

            _logger.LogInformation(
                "已创建老化架电源动作，AgingNumber={AgingNumber}, Stage={Stage}, PowerState={PowerState}, Seq={Seq}, ReportSeq={ReportSeq}",
                agingNumber,
                state.CurrentStage,
                powerState,
                seq,
                reportSeq?.ToString() ?? "<recovery>");
        }

        await TryPublishPowerUnsafeAsync(
            agingNumber,
            state,
            pending,
            controllers,
            cancellationToken);
    }

    private async Task EvaluateNextStepUnsafeAsync(
        string agingNumber,
        GroupControlState state,
        CancellationToken cancellationToken)
    {
        if (state.Mode != 2)
            return;

        if (!state.CurrentRunSeq.HasValue)
            return;

        var runSeq = state.CurrentRunSeq.Value;
        var participants = await _repository.GetRunParticipantsAsync(agingNumber, runSeq);
        var completed = await _repository.GetRunCompletedDeviceIdsAsync(agingNumber, runSeq);
        var online = await _repository.GetOnlinePointDeviceIdsAsync(agingNumber);
        var canRelease = participants.Count > 0 && participants.All(completed.Contains);

        if (!canRelease)
        {
            if (!string.IsNullOrWhiteSpace(state.PendingNextStep))
            {
                state.PendingNextStep = string.Empty;
                await _repository.SaveStateAsync(agingNumber, state);
            }

            _logger.LogDebug(
                "当前群控批次阶段尚未全部完成，AgingNumber={AgingNumber}, RunSeq={RunSeq}, Participants={ParticipantCount}, Completed={CompletedCount}, OfflineParticipants={OfflineParticipantCount}",
                agingNumber,
                runSeq,
                participants.Count,
                participants.Count(completed.Contains),
                participants.Count(id => !online.Contains(id)));
            return;
        }

        var pending = ReadPending(state, state.PendingNextStep, isPower: false);
        if (pending is null)
        {
            var seq = await _repository.NextControlSequenceAsync();
            pending = new PendingGroupControlCommand
            {
                Seq = seq,
                Payload = CreatePayload(seq, "IsCanNext", 1)
            };
            state.PendingNextStep = GroupControlRepository.SerializePending(pending);
            await _repository.SaveStateAsync(agingNumber, state);

            _logger.LogInformation(
                "当前群控批次所有参与设备已完成，创建阶段放行指令，AgingNumber={AgingNumber}, RunSeq={RunSeq}, ParticipantCount={ParticipantCount}, Seq={Seq}",
                agingNumber,
                runSeq,
                participants.Count,
                seq);
        }

        await TryPublishNextStepUnsafeAsync(
            agingNumber,
            state,
            pending,
            cancellationToken);
    }

    private async Task TryPublishNextStepUnsafeAsync(
        string agingNumber,
        GroupControlState state,
        PendingGroupControlCommand pending,
        CancellationToken cancellationToken)
    {
        try
        {
            const string topic = "server/public/Cannextstep";
            await _mqtt.PublishAsync(topic, pending.Payload, cancellationToken);

            if (state.CurrentRunSeq.HasValue)
            {
                await _repository.ClearRunCompletedDevicesAsync(
                    agingNumber,
                    state.CurrentRunSeq.Value);
            }
            state.PendingNextStep = string.Empty;
            state.Generation++;
            await _repository.SaveStateAsync(agingNumber, state);

            _logger.LogInformation(
                "群控批次阶段已通过公共主题放行，AgingNumber={AgingNumber}, RunSeq={RunSeq}, Topic={Topic}, Seq={Seq}, NextGeneration={Generation}",
                agingNumber,
                state.CurrentRunSeq?.ToString() ?? "<empty>",
                topic,
                pending.Seq,
                state.Generation);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "老化架阶段放行发布失败，将保留原 Seq 重试，AgingNumber={AgingNumber}, Seq={Seq}",
                agingNumber,
                pending.Seq);
        }
    }

    private async Task TryPublishPowerUnsafeAsync(
        string agingNumber,
        GroupControlState state,
        PendingGroupControlCommand pending,
        IReadOnlyList<string> controllers,
        CancellationToken cancellationToken)
    {
        var controllerId = GetUniqueControllerId(agingNumber, controllers, "CPower");
        if (controllerId is null)
            return;

        try
        {
            var topic = $"server/{controllerId}/cmd/CPower";
            await _mqtt.PublishAsync(topic, pending.Payload, cancellationToken);
            state.LastAppliedStage = pending.Stage;
            state.LastAppliedControllerId = controllerId;
            state.PendingPower = string.Empty;
            await _repository.SaveStateAsync(agingNumber, state);

            _logger.LogInformation(
                "老化架电源指令已发布，AgingNumber={AgingNumber}, ControllerId={ControllerId}, Stage={Stage}, Seq={Seq}",
                agingNumber,
                controllerId,
                pending.Stage,
                pending.Seq);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "老化架电源指令发布失败，将保留原 Seq 重试，AgingNumber={AgingNumber}, Stage={Stage}, Seq={Seq}",
                agingNumber,
                pending.Stage,
                pending.Seq);
        }
    }

    private string? GetUniqueControllerId(
        string agingNumber,
        IReadOnlyList<string> controllers,
        string action)
    {
        if (controllers.Count == 1)
            return controllers[0];

        if (controllers.Count == 0)
        {
            _logger.LogInformation(
                "老化架没有在线总控，群控动作继续等待，AgingNumber={AgingNumber}, Action={Action}",
                agingNumber,
                action);
        }
        else
        {
            _logger.LogWarning(
                "老化架存在多个在线总控，禁止下发以避免控制混淆，AgingNumber={AgingNumber}, Action={Action}, Controllers={Controllers}",
                agingNumber,
                action,
                string.Join(',', controllers));
        }

        return null;
    }

    private PendingGroupControlCommand? ReadPending(
        GroupControlState state,
        string? value,
        bool isPower)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        try
        {
            var command = GroupControlRepository.DeserializePending(value);
            if (command is not null && command.Seq > 0 &&
                !string.IsNullOrWhiteSpace(command.Payload))
            {
                return command;
            }
        }
        catch (JsonException ex)
        {
            _logger.LogError(
                ex,
                "老化架群控 pending JSON 非法，AgingNumber={AgingNumber}, Type={Type}",
                state.AgingNumber,
                isPower ? "Power" : "NextStep");
        }

        if (isPower)
            state.PendingPower = string.Empty;
        else
            state.PendingNextStep = string.Empty;

        return null;
    }

    private static void ResetRackActivity(GroupControlState state)
    {
        state.CurrentRunSeq = null;
        state.CurrentStage = string.Empty;
        state.LastAppliedStage = string.Empty;
        state.LastAppliedControllerId = string.Empty;
        state.PendingNextStep = string.Empty;
        state.PendingPower = string.Empty;
    }

    private static string CreatePayload(long seq, string fieldName, int value)
        => new JObject
        {
            ["Seq"] = seq,
            ["Data"] = new JObject { [fieldName] = value }
        }.ToString(Formatting.None);

    public static string NormalizeStage(string agingStage)
    {
        var value = agingStage?.Trim().ToLowerInvariant();
        return value switch
        {
            "standing" => "Standing",
            "discharge" => "Discharge",
            "recharge" => "Recharge",
            _ => throw new ArgumentException(
                "AgingStage 只允许 Standing、Discharge 或 Recharge",
                nameof(agingStage))
        };
    }
}
