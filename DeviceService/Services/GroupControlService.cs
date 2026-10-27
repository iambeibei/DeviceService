using Bluetti.Device.Redis;
using Bluetti.Device.Redis.Models;
using DeviceService.Mqtt;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using StackExchange.Redis;
using System.Collections.Concurrent;

namespace DeviceService.Services;

/// <summary>
/// 按 AgingNumber 隔离的老化群控协调服务。
/// </summary>
public sealed class GroupControlService : BackgroundService
{
    /// <summary>
    /// 群控放行指令在命令状态记录中的 Action 名。
    /// 群控放行不进入命令 Stream，这里只用于让设备回复能匹配到命令记录。
    /// </summary>
    private const string ReleaseAction = "can_next";

    private readonly GroupControlRepository _repository;
    private readonly DeviceRepository _devices;
    private readonly CommandRepository _commands;
    private readonly MqttConnection _mqtt;
    private readonly RedisHelper _redis;
    private readonly ILogger<GroupControlService> _logger;
    private readonly TimeSpan _retryDelay;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _rackLocks =
        new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, GroupControlState> _states =
        new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> _runRecoveryChecked =
        new(StringComparer.Ordinal);

    // 阶段放行倒计时（第 2 项）。
    private readonly bool _stageTimeoutEnabled;
    private readonly TimeSpan _stageTimeout;
    private readonly bool _halfThresholdCeil;
    private readonly bool _raiseFaultOnStageTimeout;

    // 放行指令响应校验（第 3 项）。
    private readonly bool _releaseReplyVerificationEnabled;
    private readonly int _releaseReplyRetryCount;
    private readonly TimeSpan _releaseReplyTimeout;
    private readonly bool _raiseFaultOnReleaseReplyFailure;

    // 异常设备免等待（终态 / 掉线）后立即移出参与集合。
    private readonly bool _excludeOnAgingTerminal;
    private readonly bool _excludeOnDeviceOffline;
    private readonly bool _raiseFaultOnExclusion;

    // 被剔除设备重新上报 agingStart 后是否允许回归本批次，以及回归时限。
    private readonly bool _excludedRejoinEnabled;
    private readonly TimeSpan _excludedRejoinTimeout;

    // 阶段倒计时是否仍要求"完成数达到半数"才起表。
    private readonly bool _stageTimeoutRequireHalf;

    // 放行回复校验的后台任务去重键集合，避免同一放行指令被重复校验。
    private readonly ConcurrentDictionary<string, byte> _releaseVerifications =
        new(StringComparer.Ordinal);

    // 后台校验任务使用的停止信号；ExecuteAsync 启动后赋值。
    private CancellationToken _serviceToken = CancellationToken.None;

    public GroupControlService(
        GroupControlRepository repository,
        DeviceRepository devices,
        CommandRepository commands,
        MqttConnection mqtt,
        RedisHelper redis,
        IConfiguration configuration,
        ILogger<GroupControlService> logger)
    {
        _repository = repository;
        _devices = devices;
        _commands = commands;
        _mqtt = mqtt;
        _redis = redis;
        _logger = logger;

        var retryMilliseconds = configuration.GetValue<int?>(
            "CommandProcessing:PublishRetryDelayMilliseconds") ?? 1000;
        if (retryMilliseconds <= 0)
            throw new InvalidOperationException(
                "CommandProcessing:PublishRetryDelayMilliseconds 必须大于 0");

        _retryDelay = TimeSpan.FromMilliseconds(retryMilliseconds);

        _stageTimeoutEnabled = configuration.GetValue<bool?>(
            "GroupControl:StageTimeoutEnabled") ?? true;

        var stageTimeoutMinutes = configuration.GetValue<double?>(
            "GroupControl:StageTimeoutMinutes") ?? 10;
        if (stageTimeoutMinutes <= 0)
            throw new InvalidOperationException("GroupControl:StageTimeoutMinutes 必须大于 0");
        _stageTimeout = TimeSpan.FromMinutes(stageTimeoutMinutes);

        var thresholdMode = configuration.GetValue<string>(
            "GroupControl:HalfReleaseThresholdMode") ?? "Ceil";
        _halfThresholdCeil = !thresholdMode.Equals("Floor", StringComparison.OrdinalIgnoreCase);

        _raiseFaultOnStageTimeout = configuration.GetValue<bool?>(
            "GroupControl:RaiseFaultOnStageTimeout") ?? true;

        _releaseReplyVerificationEnabled = configuration.GetValue<bool?>(
            "GroupControl:ReleaseReplyVerificationEnabled") ?? true;

        var retryCount = configuration.GetValue<int?>(
            "GroupControl:ReleaseReplyRetryCount") ?? 1;
        _releaseReplyRetryCount = retryCount < 0 ? 0 : retryCount;

        var replyTimeoutSeconds = configuration.GetValue<double?>(
            "CommandProcessing:GroupControlReleaseReplyTimeoutSeconds") ?? 15;
        if (replyTimeoutSeconds <= 0)
            throw new InvalidOperationException(
                "CommandProcessing:GroupControlReleaseReplyTimeoutSeconds 必须大于 0");
        _releaseReplyTimeout = TimeSpan.FromSeconds(replyTimeoutSeconds);

        _raiseFaultOnReleaseReplyFailure = configuration.GetValue<bool?>(
            "GroupControl:RaiseFaultOnReleaseReplyFailure") ?? true;

        _excludeOnAgingTerminal = configuration.GetValue<bool?>(
            "GroupControl:ExcludeOnAgingTerminal") ?? true;

        _excludeOnDeviceOffline = configuration.GetValue<bool?>(
            "GroupControl:ExcludeOnDeviceOffline") ?? true;

        _raiseFaultOnExclusion = configuration.GetValue<bool?>(
            "GroupControl:RaiseFaultOnExclusion") ?? true;

        // 默认开启：网络抖动导致的短时掉线不应剥夺设备本批次资格。
        // 置 "None" 可恢复"剔除后本批次不再回归"的旧行为。
        var rejoinMode = configuration.GetValue<string>(
            "GroupControl:ExcludedRejoinMode") ?? "OfflineWithinTimeout";
        _excludedRejoinEnabled = !rejoinMode.Equals("None", StringComparison.OrdinalIgnoreCase);

        var rejoinTimeoutMinutes = configuration.GetValue<double?>(
            "GroupControl:ExcludedRejoinTimeoutMinutes") ?? 10;
        _excludedRejoinTimeout = rejoinTimeoutMinutes > 0
            ? TimeSpan.FromMinutes(rejoinTimeoutMinutes)
            : TimeSpan.Zero;

        // 默认 false：只要有任意一台完成就起表。设为 true 则恢复"必须过半数"的旧行为。
        _stageTimeoutRequireHalf = configuration.GetValue<bool?>(
            "GroupControl:StageTimeoutRequireHalf") ?? false;
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

                // 本批次已被剔除的设备重新上报 agingStart，说明下位机仍在跑本批任务
                // （网络抖动后恢复，而不是掉电重启后任务丢失）。此时才尝试让它回归本批次。
                // 判定点必须是这里而不是上线消息：runtime.AgingState 只在状态上报时更新，
                // 上线瞬间读到的仍是掉线前的残留值，区分不出这两种情况。
                if (!activated &&
                    state.CurrentRunSeq.HasValue &&
                    state.CurrentRunSeq.Value == runSeq.Value)
                {
                    var rejoined = await TryRejoinExcludedParticipantUnsafeAsync(
                        agingNumber,
                        state,
                        runSeq.Value,
                        deviceId);
                    if (rejoined)
                        return;
                }

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
            {
                // 该设备已退出老化，不会再上报本阶段完成。
                // 立即移出阶段放行屏障，否则整架都会被它一个人永久阻塞。
                // 它仍留在 terminal 集合中，批次结束判定不受影响。
                if (_excludeOnAgingTerminal)
                {
                    await ExcludeParticipantUnsafeAsync(
                        agingNumber,
                        runSeq.Value,
                        deviceId,
                        GroupControlRunExcludeReasons.AgingTerminal,
                        $"设备上报 {agingState} 退出老化，已移出当前批次阶段放行屏障，AgingNumber={agingNumber}, RunSeq={runSeq.Value}, Stage={state.CurrentStage}",
                        state.CurrentStage);

                    var remaining = await _repository.GetRunParticipantsAsync(
                        agingNumber,
                        runSeq.Value);
                    if (remaining.Count == 0)
                    {
                        await FinishRunUnsafeAsync(
                            agingNumber,
                            state,
                            runSeq.Value,
                            "参与设备已全部进入终态并移出");
                    }
                }

                return;
            }

            await FinishRunUnsafeAsync(
                agingNumber,
                state,
                runSeq.Value,
                "全部参与设备已进入终态");
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

            // 防旧：LastModeSeq 原先只写不读，导致迟到的旧模式上报能覆盖新模式。
            // 模式切换会重置整架活动与 Generation，被旧报文覆盖代价很大。
            if (state.LastModeSeq.HasValue && seq <= state.LastModeSeq.Value)
            {
                _logger.LogWarning(
                    "忽略过期或重复的群控模式上报，AgingNumber={AgingNumber}, Mode={Mode}, Seq={Seq}, LastModeSeq={LastModeSeq}",
                    agingNumber,
                    mode,
                    seq,
                    state.LastModeSeq.Value);
                return;
            }

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

            // 防旧：原先 reportSeq 只做 >0 校验就直接覆盖 CurrentStage，
            // 一条迟到的旧阶段上报会把阶段拉回去并误触 CPower 供电控制。
            if (state.LastStageSeq.HasValue && reportSeq <= state.LastStageSeq.Value)
            {
                _logger.LogWarning(
                    "忽略过期或重复的阶段上报，AgingNumber={AgingNumber}, Stage={Stage}, ReportSeq={ReportSeq}, LastStageSeq={LastStageSeq}",
                    agingNumber,
                    stage,
                    reportSeq,
                    state.LastStageSeq.Value);
                return;
            }

            state.CurrentStage = stage;
            state.LastStageSeq = reportSeq;
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
    /// 设备掉线（MQTT 遗嘱）后把它移出当前批次的阶段放行屏障。
    /// 掉线设备不会再上报 TaskCP，若不剔除会永久阻塞整架放行。
    /// 剔除后本批次不再等待它；若它在回归时限内重新上报 agingStart，
    /// 由 TryRejoinExcludedParticipantUnsafeAsync 重新纳入本批次，否则需等下一个 RunSeq。
    /// </summary>
    public async Task HandleDeviceOfflineAsync(string deviceId)
    {
        deviceId = RedisKeys.Normalize(deviceId);

        if (!_excludeOnDeviceOffline)
            return;

        var device = await _devices.GetAsync(deviceId);
        if (device is null ||
            !string.Equals(device.DeviceType, "1", StringComparison.Ordinal) ||
            string.IsNullOrWhiteSpace(device.AgingNumber))
        {
            return;
        }

        string agingNumber;
        try
        {
            agingNumber = RedisKeys.NormalizeAgingNumber(device.AgingNumber);
        }
        catch (ArgumentException ex)
        {
            _logger.LogWarning(
                ex,
                "设备架号非法，跳过掉线剔除，DeviceId={DeviceId}, AgingNumber={AgingNumber}",
                deviceId,
                device.AgingNumber);
            return;
        }

        var rackLock = GetRackLock(agingNumber);
        await rackLock.WaitAsync();
        try
        {
            var state = await GetStateUnsafeAsync(agingNumber);
            if (state.Mode != 2 || !state.CurrentRunSeq.HasValue)
                return;

            var runSeq = state.CurrentRunSeq.Value;
            var participants = await _repository.GetRunParticipantsAsync(agingNumber, runSeq);
            if (!participants.Contains(deviceId))
                return;

            await ExcludeParticipantUnsafeAsync(
                agingNumber,
                runSeq,
                deviceId,
                GroupControlRunExcludeReasons.DeviceOffline,
                $"设备离线，已移出当前批次阶段放行屏障，AgingNumber={agingNumber}, RunSeq={runSeq}, Stage={state.CurrentStage}",
                state.CurrentStage);

            // 兜底：参与集合被清空后既不会放行也不会结束，直接收尾。
            var remaining = await _repository.GetRunParticipantsAsync(agingNumber, runSeq);
            if (remaining.Count == 0)
            {
                await FinishRunUnsafeAsync(
                    agingNumber,
                    state,
                    runSeq,
                    "参与设备已全部掉线移出");
            }
        }
        finally
        {
            rackLock.Release();
        }
    }

    /// <summary>
    /// start_aging 连续多次下发都收不到设备 cmd/reply 时调用：判定该节点指令通道异常。
    ///
    /// 做两件事：
    /// 1. 写 event:fault（最新值 Hash + 历史 Stream）并 publish event:fault，供平台与运维感知；
    /// 2. 若设备属于当前 mode=2 群控批次的参与集合，立即剔除。
    ///    这一步必须做：RegisterAgingBatchAsync 在下发前就把设备登记进 participants，
    ///    失败设备若不移除，批次激活后会永久阻塞整架阶段放行屏障。
    ///
    /// 与 HandleDeviceOfflineAsync 的区别：设备可能仍在线（仍在 lgw:devices:online 中），
    /// 只是指令通道失效，因此剔除原因用 start_aging_no_reply，且不允许回归本批次。
    /// </summary>
    public async Task HandleStartAgingNoReplyAsync(string deviceId, long seq, string message)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        message ??= string.Empty;

        var agingNumber = string.Empty;
        long? runSeq = null;
        var excluded = false;
        var faultWritten = false;

        var device = await _devices.GetAsync(deviceId);
        if (device is not null &&
            string.Equals(device.DeviceType, "1", StringComparison.Ordinal) &&
            !string.IsNullOrWhiteSpace(device.AgingNumber))
        {
            try
            {
                agingNumber = RedisKeys.NormalizeAgingNumber(device.AgingNumber);
            }
            catch (ArgumentException ex)
            {
                _logger.LogWarning(
                    ex,
                    "设备架号非法，跳过群控剔除，DeviceId={DeviceId}, AgingNumber={AgingNumber}",
                    deviceId,
                    device.AgingNumber);
            }

            if (agingNumber.Length > 0)
            {
                var rackLock = GetRackLock(agingNumber);
                await rackLock.WaitAsync();
                try
                {
                    var state = await GetStateUnsafeAsync(agingNumber);
                    if (state.Mode == 2 && state.CurrentRunSeq.HasValue)
                    {
                        var currentRunSeq = state.CurrentRunSeq.Value;
                        var participants = await _repository.GetRunParticipantsAsync(agingNumber, currentRunSeq);
                        if (participants.Contains(deviceId))
                        {
                            runSeq = currentRunSeq;

                            await ExcludeParticipantUnsafeAsync(
                                agingNumber,
                                currentRunSeq,
                                deviceId,
                                GroupControlRunExcludeReasons.StartAgingNoReply,
                                message,
                                state.CurrentStage);

                            excluded = true;

                            // ExcludeParticipantUnsafeAsync 在 RaiseFaultOnExclusion 打开时已经写过故障，
                            // 避免同一次异常留下两条 event:fault。
                            faultWritten = _raiseFaultOnExclusion;

                            // 兜底：参与集合被清空后既不会放行也不会结束，直接收尾。
                            var remaining = await _repository.GetRunParticipantsAsync(agingNumber, currentRunSeq);
                            if (remaining.Count == 0)
                            {
                                await FinishRunUnsafeAsync(
                                    agingNumber,
                                    state,
                                    currentRunSeq,
                                    "参与设备已全部因 start_aging 无回复移出");
                            }
                        }
                    }
                }
                finally
                {
                    rackLock.Release();
                }
            }
        }

        if (!faultWritten)
        {
            var payload = BuildGroupControlFaultPayload(
                agingNumber,
                runSeq ?? 0,
                deviceId,
                GroupControlRunExcludeReasons.StartAgingNoReply,
                message,
                seq);

            await _devices.SaveGroupControlFaultAsync(deviceId, seq, message, payload);
            await PublishFaultAsync(deviceId, message);
        }

        _logger.LogError(
            "start_aging 连续下发无回复，节点判定异常，AgingNumber={AgingNumber}, RunSeq={RunSeq}, DeviceId={DeviceId}, Seq={Seq}, Excluded={Excluded}, FaultWritten={FaultWritten}, Message={Message}",
            agingNumber.Length == 0 ? "<empty>" : agingNumber,
            runSeq?.ToString() ?? "<empty>",
            deviceId,
            seq,
            excluded,
            faultWritten,
            message);
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
            "多老化架群控协调服务已启动，RetryDelayMs={RetryDelayMs}, StageTimeoutEnabled={StageTimeoutEnabled}, StageTimeoutMinutes={StageTimeoutMinutes}, ReleaseReplyVerification={ReleaseReplyVerification}",
            _retryDelay.TotalMilliseconds,
            _stageTimeoutEnabled,
            _stageTimeout.TotalMinutes,
            _releaseReplyVerificationEnabled);

        // 放行回复校验的后台任务复用本服务的停止信号。
        _serviceToken = stoppingToken;

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

        // 阶段倒计时到期先处理，随后才判断是否放行。
        // 顺序固定为：先超时剔除 → 再判放行 → 最后按需起表。
        if (_stageTimeoutEnabled &&
            state.StageDeadlineUtc.HasValue &&
            DateTime.UtcNow >= state.StageDeadlineUtc.Value)
        {
            await ExpireStageTimeoutUnsafeAsync(agingNumber, state, runSeq);
        }

        var participants = await _repository.GetRunParticipantsAsync(agingNumber, runSeq);

        // 参与集合被清空后：既不会放行（count == 0 恒不满足 All），也不会结束（没有设备再上报终态）。
        // 直接收尾，避免批次永久挂起。
        if (participants.Count == 0)
        {
            await FinishRunUnsafeAsync(agingNumber, state, runSeq, "阶段放行屏障已无参与设备");
            return;
        }

        var completed = await _repository.GetRunCompletedDeviceIdsAsync(agingNumber, runSeq);
        var online = await _repository.GetOnlinePointDeviceIdsAsync(agingNumber);
        var canRelease = participants.All(completed.Contains);

        if (!canRelease)
        {
            if (!string.IsNullOrWhiteSpace(state.PendingNextStep))
            {
                state.PendingNextStep = string.Empty;
                await _repository.SaveStateAsync(agingNumber, state);
            }

            // 尚未开始计时时启动倒计时。默认只要本阶段已有任意设备完成就起表；
            // StageTimeoutRequireHalf=true 时恢复"必须达到半数"的旧行为。
            // 已全覆盖的情况走不到这里，所以不会出现"刚起表就立即放行"。
            if (_stageTimeoutEnabled &&
                !state.StageDeadlineUtc.HasValue &&
                completed.Count > 0 &&
                (!_stageTimeoutRequireHalf ||
                 completed.Count >= ComputeHalfThreshold(participants.Count)))
            {
                state.StageDeadlineUtc = DateTime.UtcNow + _stageTimeout;
                await _repository.SaveStateAsync(agingNumber, state);

                _logger.LogInformation(
                    "群控阶段完成数已达半数，启动阶段放行倒计时，AgingNumber={AgingNumber}, RunSeq={RunSeq}, Completed={CompletedCount}, Participants={ParticipantCount}, Threshold={Threshold}, TimeoutMinutes={TimeoutMinutes}",
                    agingNumber,
                    runSeq,
                    completed.Count,
                    participants.Count,
                    ComputeHalfThreshold(participants.Count),
                    _stageTimeout.TotalMinutes);
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

        // 全员完成：清掉倒计时后走正常放行。
        if (state.StageDeadlineUtc.HasValue)
        {
            state.StageDeadlineUtc = null;
            await _repository.SaveStateAsync(agingNumber, state);
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
        string topic;
        try
        {
            topic = BuildNextStepTopic(agingNumber);
        }
        catch (ArgumentException ex)
        {
            // 架号非法时绝不退化成公共主题广播，否则会把放行指令送到其它老化架。
            // 保留 pending，由后台轮询以原 Seq 重试。
            _logger.LogError(
                ex,
                "老化架放行主题的 AgingNumber 无效，保留原 Seq 等待重试，AgingNumber={AgingNumber}, Seq={Seq}",
                agingNumber,
                pending.Seq);
            return;
        }

        var runSeq = state.CurrentRunSeq;
        var participants = runSeq.HasValue
            ? await _repository.GetRunParticipantsAsync(agingNumber, runSeq.Value)
            : (IReadOnlySet<string>)new HashSet<string>(StringComparer.Ordinal);

        // 离线设备收不到架广播，也不该被记成"未收到确认"，校验前先剔除。
        var verifiable = participants.Count > 0
            ? await FilterOnlineParticipantsAsync(agingNumber, participants)
            : (IReadOnlySet<string>)new HashSet<string>(StringComparer.Ordinal);

        if (participants.Count > 0 && verifiable.Count < participants.Count)
        {
            _logger.LogWarning(
                "群控放行回复校验跳过离线设备，AgingNumber={AgingNumber}, RunSeq={RunSeq}, Seq={Seq}, Participants={ParticipantCount}, Verifiable={VerifiableCount}",
                agingNumber,
                runSeq?.ToString() ?? "<empty>",
                pending.Seq,
                participants.Count,
                verifiable.Count);
        }

        // 发布前先登记命令状态，设备按 CmdSeq 回复时才能匹配到这条放行指令。
        if (_releaseReplyVerificationEnabled && runSeq.HasValue && verifiable.Count > 0)
            await StoreReleaseCommandsAsync(verifiable, pending.Seq);

        try
        {
            await _mqtt.PublishAsync(topic, pending.Payload, cancellationToken);

            if (runSeq.HasValue)
            {
                await _repository.ClearRunCompletedDevicesAsync(agingNumber, runSeq.Value);
            }
            state.PendingNextStep = string.Empty;
            state.StageDeadlineUtc = null;
            state.Generation++;
            await _repository.SaveStateAsync(agingNumber, state);

            _logger.LogInformation(
                "群控批次阶段已按老化架主题放行，AgingNumber={AgingNumber}, RunSeq={RunSeq}, Topic={Topic}, Seq={Seq}, NextGeneration={Generation}",
                agingNumber,
                runSeq?.ToString() ?? "<empty>",
                topic,
                pending.Seq,
                state.Generation);

            // 放行已生效，回复校验只在后台留痕与重试，不阻塞阶段推进。
            if (_releaseReplyVerificationEnabled && runSeq.HasValue && verifiable.Count > 0)
            {
                StartReleaseVerification(
                    agingNumber,
                    runSeq.Value,
                    pending.Seq,
                    verifiable,
                    state.Generation);
            }
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

    /// <summary>
    /// 把单台设备移出当前批次的阶段放行屏障。调用方必须持有对应老化架锁。
    /// 设备不再被等待 TaskCP，也不再阻塞其它设备放行；被移出的设备本批次不再回归，
    /// 只能等下一个 RunSeq。同时记入 excluded 供审计，并按配置写 event:fault。
    /// </summary>
    private async Task ExcludeParticipantUnsafeAsync(
        string agingNumber,
        long runSeq,
        string deviceId,
        string reason,
        string message,
        string? stage)
    {
        await _repository.RemoveRunParticipantsAsync(agingNumber, runSeq, [deviceId]);
        await _repository.AddRunExcludedDeviceAsync(agingNumber, runSeq, deviceId);

        // 剔除时刻与当时阶段决定这台设备后续能否回归本批次，必须同时落库。
        await _repository.SetRunExcludedMetaAsync(
            agingNumber,
            runSeq,
            new GroupControlRunExcludedMeta
            {
                DeviceId = RedisKeys.Normalize(deviceId),
                Reason = reason,
                Stage = stage ?? string.Empty,
                ExcludedAtUtc = DateTime.UtcNow
            });

        if (_raiseFaultOnExclusion)
        {
            var payload = BuildGroupControlFaultPayload(
                agingNumber,
                runSeq,
                deviceId,
                reason,
                message,
                seq: null);
            await _devices.SaveGroupControlFaultAsync(deviceId, runSeq, message, payload);
            await PublishFaultAsync(deviceId, message);
        }

        _logger.LogWarning(
            "设备已移出群控批次参与集合，AgingNumber={AgingNumber}, RunSeq={RunSeq}, DeviceId={DeviceId}, Reason={Reason}, FaultRaised={FaultRaised}",
            agingNumber,
            runSeq,
            deviceId,
            reason,
            _raiseFaultOnExclusion);
    }

    /// <summary>
    /// 设备被移出参与集合后重新上报 agingStart 时，尝试让它回归本批次。
    ///
    /// 只有「因离线被剔除」且「仍在回归时限内」的设备才允许回归：网络抖动导致的短时掉线
    /// 不应剥夺本批次资格，而长时间离线的设备已落后于全架进度，回归意义不大。
    /// 终态剔除与超时剔除不回归：前者已退出老化，后者是本阶段确实没做完。
    ///
    /// 阶段对齐：若剔除期间全架已推进到下一阶段，设备错过了本阶段放行，
    /// 直接按未完成者等待会永久阻塞整架（放行指令不会为单台补发）。
    /// 因此这种情况下把它计为当前阶段已完成，本阶段豁免，从下一阶段起正常参与。
    /// 调用方必须持有对应老化架锁。
    /// </summary>
    private async Task<bool> TryRejoinExcludedParticipantUnsafeAsync(
        string agingNumber,
        GroupControlState state,
        long runSeq,
        string deviceId)
    {
        if (!_excludedRejoinEnabled || _excludedRejoinTimeout <= TimeSpan.Zero)
            return false;

        var meta = await _repository.GetRunExcludedMetaAsync(agingNumber, runSeq, deviceId);
        if (meta is null)
            return false;

        if (!string.Equals(
                meta.Reason,
                GroupControlRunExcludeReasons.DeviceOffline,
                StringComparison.Ordinal))
        {
            _logger.LogInformation(
                "剔除原因不允许回归本批次，AgingNumber={AgingNumber}, RunSeq={RunSeq}, DeviceId={DeviceId}, Reason={Reason}",
                agingNumber,
                runSeq,
                deviceId,
                meta.Reason);
            return false;
        }

        // 反序列化后 Kind 未必是 Utc，统一折算再比较，避免本机时区把离线时长算短 8 小时。
        var excludedAtUtc = meta.ExcludedAtUtc.Kind == DateTimeKind.Local
            ? meta.ExcludedAtUtc.ToUniversalTime()
            : meta.ExcludedAtUtc;
        var offlineDuration = DateTime.UtcNow - excludedAtUtc;
        if (offlineDuration > _excludedRejoinTimeout)
        {
            _logger.LogWarning(
                "设备离线时长已超过回归时限，拒绝回归本批次，AgingNumber={AgingNumber}, RunSeq={RunSeq}, DeviceId={DeviceId}, OfflineMinutes={OfflineMinutes}, TimeoutMinutes={TimeoutMinutes}",
                agingNumber,
                runSeq,
                deviceId,
                offlineDuration.TotalMinutes,
                _excludedRejoinTimeout.TotalMinutes);
            return false;
        }

        var stageAdvanced = !string.Equals(
            meta.Stage,
            state.CurrentStage,
            StringComparison.Ordinal);

        await _repository.RemoveRunExcludedDeviceAsync(agingNumber, runSeq, deviceId);
        await _repository.AddRunParticipantsAsync(agingNumber, runSeq, [deviceId]);
        await _repository.AddRunStartedDeviceAsync(agingNumber, runSeq, deviceId);

        if (stageAdvanced)
            await _repository.AddRunCompletedDeviceAsync(agingNumber, runSeq, deviceId);

        _logger.LogWarning(
            "设备已回归当前群控批次，AgingNumber={AgingNumber}, RunSeq={RunSeq}, DeviceId={DeviceId}, OfflineSeconds={OfflineSeconds}, ExcludedStage={ExcludedStage}, CurrentStage={CurrentStage}, CurrentStageWaived={CurrentStageWaived}",
            agingNumber,
            runSeq,
            deviceId,
            offlineDuration.TotalSeconds,
            meta.Stage,
            state.CurrentStage,
            stageAdvanced);

        // 回归后重新评估本阶段：阶段已推进时它已计入完成，可能立刻满足放行。
        await EvaluateNextStepUnsafeAsync(
            agingNumber,
            state,
            CancellationToken.None);

        return true;
    }

    /// <summary>
    /// 结束当前批次并清理阶段状态。调用方必须持有对应老化架锁。
    /// </summary>
    private async Task FinishRunUnsafeAsync(
        string agingNumber,
        GroupControlState state,
        long runSeq,
        string reason)
    {
        var runState = await _repository.GetRunStateAsync(agingNumber, runSeq);
        if (runState is not null)
        {
            runState.Status = GroupControlRunStatuses.Finished;
            runState.FinishedAtUtc = DateTime.UtcNow;
            await _repository.SaveRunStateAsync(agingNumber, runState);
        }

        state.CurrentRunSeq = null;
        state.PendingNextStep = string.Empty;
        state.StageDeadlineUtc = null;
        await _repository.ClearRunCompletedDevicesAsync(agingNumber, runSeq);
        await _repository.SaveStateAsync(agingNumber, state);

        _logger.LogInformation(
            "群控批次结束，AgingNumber={AgingNumber}, RunSeq={RunSeq}, Reason={Reason}",
            agingNumber,
            runSeq,
            reason);
    }

    /// <summary>
    /// 阶段放行的半数阈值。
    /// Ceil：5 台取 3、6 台取 3；Floor：5 台取 2、6 台取 3。默认 Ceil。
    /// 参与集合为 1 台时 Ceil 阈值为 1，即"完成即起表"，但这种情况在起表前
    /// 已完成数必然等于参与数，会直接走全员放行，不会真正启动倒计时。
    /// </summary>
    private int ComputeHalfThreshold(int count)
    {
        if (count <= 0)
            return 0;

        return _halfThresholdCeil
            ? (int)Math.Ceiling(count / 2.0)
            : (int)Math.Floor(count / 2.0);
    }

    /// <summary>
    /// 阶段倒计时到期处理。调用方必须持有对应老化架锁。
    /// 未完成设备：从 participants 剔除 → 记入 excluded → 记故障（可选）。
    /// 剔除后本批次不再接受这些设备的 TaskCP（RecordTaskCompletionAsync 以 participants 为屏障），
    /// 它们只能等下一次 start_aging 批次重新参与，不在本批次复活。
    /// </summary>
    private async Task ExpireStageTimeoutUnsafeAsync(
        string agingNumber,
        GroupControlState state,
        long runSeq)
    {
        var participants = await _repository.GetRunParticipantsAsync(agingNumber, runSeq);
        var completed = await _repository.GetRunCompletedDeviceIdsAsync(agingNumber, runSeq);
        var expired = participants
            .Where(deviceId => !completed.Contains(deviceId))
            .OrderBy(deviceId => deviceId, StringComparer.Ordinal)
            .ToArray();

        // 无论是否真的剔除了设备，倒计时到期后都必须清掉 deadline，
        // 否则后续每一轮轮询都会重复进入本方法。
        state.StageDeadlineUtc = null;
        state.LastStageTimeoutSeq = runSeq;

        if (expired.Length == 0)
        {
            await _repository.SaveStateAsync(agingNumber, state);

            _logger.LogInformation(
                "群控阶段倒计时到期但参与设备已全部完成，倒计时已清空，AgingNumber={AgingNumber}, RunSeq={RunSeq}, Stage={Stage}",
                agingNumber,
                runSeq,
                state.CurrentStage);
            return;
        }

        await _repository.RemoveRunParticipantsAsync(agingNumber, runSeq, expired);
        foreach (var deviceId in expired)
        {
            await _repository.AddRunExcludedDeviceAsync(agingNumber, runSeq, deviceId);
            await _repository.SetRunExcludedMetaAsync(
                agingNumber,
                runSeq,
                new GroupControlRunExcludedMeta
                {
                    DeviceId = RedisKeys.Normalize(deviceId),
                    Reason = GroupControlRunExcludeReasons.StageTimeout,
                    Stage = state.CurrentStage,
                    ExcludedAtUtc = DateTime.UtcNow
                });
        }

        if (_raiseFaultOnStageTimeout)
        {
            foreach (var deviceId in expired)
            {
                var message =
                    $"群控阶段超时未完成，已剔除出当前批次，AgingNumber={agingNumber}, RunSeq={runSeq}, Stage={state.CurrentStage}";
                var payload = BuildGroupControlFaultPayload(
                    agingNumber,
                    runSeq,
                    deviceId,
                    "stage_timeout",
                    message,
                    seq: null);
                await _devices.SaveGroupControlFaultAsync(deviceId, runSeq, message, payload);
                await PublishFaultAsync(deviceId, message);
            }
        }

        await _repository.SaveStateAsync(agingNumber, state);

        _logger.LogWarning(
            "群控阶段倒计时到期，未完成设备已剔除出参与集合，AgingNumber={AgingNumber}, RunSeq={RunSeq}, Stage={Stage}, Excluded={Excluded}, Remaining={Remaining}, FaultRaised={FaultRaised}",
            agingNumber,
            runSeq,
            state.CurrentStage,
            string.Join(',', expired),
            participants.Count - expired.Length,
            _raiseFaultOnStageTimeout);
    }

    /// <summary>
    /// 放行前为每台参与设备登记命令记录，使设备按 CmdSeq 的回复能匹配到这条放行指令。
    /// 群控放行不进入命令 Stream，因此这里只补状态 Hash，不 Enqueue。
    /// EnsureStoredAsync 在记录已存在时不改变其状态，重复发布原 Seq 不会误清结果。
    /// </summary>
    private async Task StoreReleaseCommandsAsync(
        IReadOnlySet<string> participants,
        long seq)
    {
        foreach (var deviceId in participants)
        {
            await _commands.EnsureStoredAsync(new CmdInfo
            {
                Seq = seq,
                DeviceId = RedisKeys.Normalize(deviceId),
                Action = ReleaseAction,
                ExpectedReplyType = "point",
                Status = CommandStates.Published,
                CreatedAtUtc = DateTime.UtcNow,
                PublishedAtUtc = DateTime.UtcNow,
                Data = new JObject { ["IsCanNext"] = 1 }
            });
        }
    }

    /// <summary>
    /// 启动放行回复校验。校验在后台执行，不阻塞阶段推进（放行本身已生效）。
    /// 用 agingNumber/runSeq/generation/seq 组合去重，避免同一条放行指令被重复校验。
    /// </summary>
    private void StartReleaseVerification(
        string agingNumber,
        long runSeq,
        long seq,
        IReadOnlySet<string> participants,
        long generation)
    {
        var dedupKey = $"{agingNumber}:{runSeq}:{generation}:{seq}";
        if (!_releaseVerifications.TryAdd(dedupKey, 0))
        {
            _logger.LogDebug(
                "群控放行回复校验已存在，跳过重复启动，AgingNumber={AgingNumber}, RunSeq={RunSeq}, Seq={Seq}",
                agingNumber,
                runSeq,
                seq);
            return;
        }

        var deviceIds = participants
            .Select(RedisKeys.Normalize)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(deviceId => deviceId, StringComparer.Ordinal)
            .ToArray();

        var token = _serviceToken;
        _ = Task.Run(async () =>
        {
            try
            {
                await VerifyReleaseRepliesAsync(agingNumber, runSeq, seq, deviceIds, token);
            }
            catch (OperationCanceledException)
            {
                // 服务停止，正常退出。
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "群控放行回复校验异常终止，AgingNumber={AgingNumber}, RunSeq={RunSeq}, Seq={Seq}",
                    agingNumber,
                    runSeq,
                    seq);
            }
            finally
            {
                _releaseVerifications.TryRemove(dedupKey, out _);
            }
        });
    }

    /// <summary>
    /// 逐台等待放行回复：校验 Code 是否为成功值（1），超时未回复或 Code 非 1 都计为失败。
    /// 失败后按配置次数重发架广播（复用原 Seq 保证设备端幂等）并重新等待；
    /// 重发前必须 ResetForRetryAsync 把命令状态改回 published，否则第二次等待会立即返回。
    /// 仍失败才写故障并发布告警。校验结果不改变批次放行判定（放行已生效）。
    /// </summary>
    private async Task VerifyReleaseRepliesAsync(
        string agingNumber,
        long runSeq,
        long seq,
        IReadOnlyList<string> deviceIds,
        CancellationToken cancellationToken)
    {
        var pending = new List<string>(deviceIds);
        var clearedByOffline = false;

        for (var attempt = 0; attempt <= _releaseReplyRetryCount && pending.Count > 0; attempt++)
        {
            // 每轮开始前剔除离线设备：它们收不到架广播，
            // 等待它们回复只会产生"未收到确认"的假故障。
            var onlineNow = await _repository.GetOnlinePointDeviceIdsAsync(agingNumber);
            var offlined = pending.Where(deviceId => !onlineNow.Contains(deviceId)).ToArray();
            if (offlined.Length > 0)
            {
                pending = pending.Where(onlineNow.Contains).ToList();

                _logger.LogWarning(
                    "群控放行回复校验跳过已离线设备，AgingNumber={AgingNumber}, RunSeq={RunSeq}, Seq={Seq}, Offline={Offline}, Attempt={Attempt}",
                    agingNumber,
                    runSeq,
                    seq,
                    string.Join(',', offlined),
                    attempt);

                if (pending.Count == 0)
                {
                    clearedByOffline = true;
                    break;
                }
            }

            if (attempt > 0)
            {
                // 必须先复位、再重发。
                // 原实现是"先重发、后 ResetForRetryAsync"，顺序反了：
                // ResetForRetryAsync 会清空 ResultCode / ReplyJson / CompletedAtUtc，
                // 若设备在重发与复位之间就回了确认，这次确认会被随后的复位直接抹掉，
                // 于是永远等不到回复、白白多一次重发。
                // 复位用意是让 WaitForCompletionAsync 不再遇终态立即返回（否则重试形同虚设）。
                foreach (var deviceId in pending)
                    await _commands.ResetForRetryAsync(deviceId, seq);

                // 重发必须复用原 Seq：设备端按 Seq/CmdSeq 幂等去重，换 Seq 会造成重复放行。
                try
                {
                    await _mqtt.PublishAsync(
                        BuildNextStepTopic(agingNumber),
                        CreatePayload(seq, "IsCanNext", 1),
                        cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "群控放行指令重发失败，AgingNumber={AgingNumber}, RunSeq={RunSeq}, Seq={Seq}, Attempt={Attempt}",
                        agingNumber,
                        runSeq,
                        seq,
                        attempt);
                }
            }

            // 每台设备各自等待、并发进行；单台异常不影响其它设备。
            var attempts = await Task.WhenAll(pending.Select(async deviceId =>
            {
                try
                {
                    return (
                        DeviceId: deviceId,
                        Result: await _commands.WaitForCompletionAsync(
                            deviceId, seq, _releaseReplyTimeout, cancellationToken),
                        Error: (Exception?)null);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    return (DeviceId: deviceId, Result: (CmdInfo?)null, Error: (Exception?)ex);
                }
            }));

            var stillPending = new List<string>();
            foreach (var (deviceId, result, error) in attempts)
            {
                if (error is not null)
                {
                    _logger.LogWarning(
                        error,
                        "等待群控放行回复异常，AgingNumber={AgingNumber}, RunSeq={RunSeq}, DeviceId={DeviceId}, Seq={Seq}",
                        agingNumber,
                        runSeq,
                        deviceId,
                        seq);
                    stillPending.Add(deviceId);
                    continue;
                }

                if (result is not null &&
                    string.Equals(result.Status, CommandStates.Completed, StringComparison.OrdinalIgnoreCase) &&
                    result.ResultCode == 1)
                {
                    continue;
                }

                // null 表示超时未回复；failed 表示设备回复了但 Code 非 1。
                // 两者都先补记 timed_out，保证命令状态有终态可追溯。
                var command = result ?? await _commands.GetAsync(deviceId, seq);
                if (command is not null && !CommandStates.IsTerminal(command.Status))
                    await _commands.MarkTimedOutAsync(command, _releaseReplyTimeout);

                _logger.LogWarning(
                    "群控放行回复校验未通过，AgingNumber={AgingNumber}, RunSeq={RunSeq}, DeviceId={DeviceId}, Seq={Seq}, Status={Status}, Code={Code}, Msg={Msg}, Attempt={Attempt}",
                    agingNumber,
                    runSeq,
                    deviceId,
                    seq,
                    result?.Status ?? "<no-reply>",
                    result?.ResultCode?.ToString() ?? "<empty>",
                    result?.ResultMessage ?? "<empty>",
                    attempt);

                stillPending.Add(deviceId);
            }

            pending = stillPending;
        }

        if (pending.Count == 0)
        {
            if (clearedByOffline)
            {
                _logger.LogWarning(
                    "群控放行回复校验结束：待校验设备已全部离线，不再等待确认，AgingNumber={AgingNumber}, RunSeq={RunSeq}, Seq={Seq}, DeviceCount={DeviceCount}",
                    agingNumber,
                    runSeq,
                    seq,
                    deviceIds.Count);
            }
            else
            {
                _logger.LogInformation(
                    "群控放行指令已收到全部设备确认，AgingNumber={AgingNumber}, RunSeq={RunSeq}, Seq={Seq}, DeviceCount={DeviceCount}",
                    agingNumber,
                    runSeq,
                    seq,
                    deviceIds.Count);
            }

            return;
        }

        _logger.LogError(
            "群控放行指令仍有设备未确认，AgingNumber={AgingNumber}, RunSeq={RunSeq}, Seq={Seq}, Unconfirmed={Unconfirmed}",
            agingNumber,
            runSeq,
            seq,
            string.Join(',', pending));

        if (!_raiseFaultOnReleaseReplyFailure)
            return;

        foreach (var deviceId in pending)
        {
            var message =
                $"群控放行指令未收到设备确认，AgingNumber={agingNumber}, RunSeq={runSeq}, Seq={seq}";
            var payload = BuildGroupControlFaultPayload(
                agingNumber,
                runSeq,
                deviceId,
                "release_reply_unconfirmed",
                message,
                seq);
            await _devices.SaveGroupControlFaultAsync(deviceId, seq, message, payload);
            await PublishFaultAsync(deviceId, message);
        }
    }

    /// <summary>
    /// 取参与集合中当前仍在线的点位设备。离线设备收不到架广播，
    /// 放行回复校验不应等待它们，也不应因其"未回复"而记故障。
    /// </summary>
    private async Task<IReadOnlySet<string>> FilterOnlineParticipantsAsync(
        string agingNumber,
        IReadOnlySet<string> participants)
    {
        var online = await _repository.GetOnlinePointDeviceIdsAsync(agingNumber);
        return new HashSet<string>(
            participants.Where(online.Contains),
            StringComparer.Ordinal);
    }

    /// <summary>
    /// 服务端主动产生的群控故障载荷，写入设备 event/fault 历史用于追溯。
    /// </summary>
    private static string BuildGroupControlFaultPayload(
        string agingNumber,
        long runSeq,
        string deviceId,
        string reason,
        string message,
        long? seq)
        => new JObject
        {
            ["AgingNumber"] = agingNumber,
            ["RunSeq"] = runSeq,
            ["DeviceId"] = deviceId,
            ["Seq"] = seq?.ToString() ?? string.Empty,
            ["Reason"] = reason,
            ["Message"] = message,
            ["OccurredAtUtc"] = DateTime.UtcNow.ToString("O")
        }.ToString(Formatting.None);

    /// <summary>
    /// 发布故障告警，格式与设备自发故障保持一致（DeviceID / type / Message / ReceivedAtUtc）。
    /// </summary>
    private Task PublishFaultAsync(string deviceId, string message)
        => _redis.PublishAsync(
            RedisKeys.channelFault,
            new JObject
            {
                ["DeviceID"] = deviceId,
                ["type"] = "fault",
                ["Message"] = message,
                ["ReceivedAtUtc"] = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")
            }.ToString(Formatting.None));

    /// <summary>
    /// 阶段放行主题按老化架隔离：server/rack/{AgingNumber}/CanNextstep。
    /// AgingNumber 必须先规范化再拼进主题，避免出现空段或非法字符破坏订阅匹配。
    /// 大小写与设备端订阅串强绑定，改动必须同步固件。
    /// </summary>
    private static string BuildNextStepTopic(string agingNumber)
        => $"server/rack/{RedisKeys.NormalizeAgingNumber(agingNumber)}/CanNextstep";

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
        state.StageDeadlineUtc = null;
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
