using Bluetti.Device.Redis;
using Bluetti.Device.Redis.Models;
using DeviceService.Mqtt;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;

namespace DeviceService.Services;

/// <summary>
/// OTA 分层升级协调服务。
///
/// 流程：收到多设备 ota 指令后，按 Mesh 层级从 1（根节点）开始逐层下发，
/// 每层等设备**上报 OTACP（固件下载完成）**后，再等
/// `LevelStabilizeSeconds`（当前 60 秒）进入下一层。
///
/// **完成判据（2026-10-22 变更）**：新固件升级后设备**不再重启**，
/// 固件版本号可能根本不变，因此不再用"版本号变化"推断成功，
/// 改为设备下载完成后主动上报 `device/{id}/event/OTACP`
/// （`Data.IsComplete=1`），由 <c>OtaCompletionHandler</c> 标记 upgraded。
/// 版本基线 `level:{层}:versions` 与 mesh 稳定计数判定都已随之删除。
///
/// 升级任务表 = 平台 DeviceInfos ∩ 该架在线列表 `lgw:ota:{AgingNumber}`：
/// 只用 DeviceInfos 会给离线设备白等一轮超时；只用在线列表会丢 RecordId
/// （命令状态回写与老化关联都依赖它）且会升级平台没点名的设备。
/// 真正下发时还要与分层清单 `lgw:ota:{AgingNumber}:{MeshLevel}` 取交集。
///
/// 单设备 ota 指令不进入本服务，由 RedisCmdWorker 按普通命令直接转发。
/// </summary>
public sealed class OtaUpgradeService : BackgroundService
{
    private readonly OtaUpgradeRepository _repository;
    private readonly DeviceRepository _devices;
    private readonly MqttConnection _mqtt;
    private readonly RedisHelper _redis;
    private readonly ILogger<OtaUpgradeService> _logger;

    private readonly bool _enabled;
    private readonly TimeSpan _baseTimeout;
    private readonly TimeSpan _timeoutIncrement;
    private readonly TimeSpan? _maxLevelTimeout;
    private readonly TimeSpan? _firstLevelTimeout;
    private readonly TimeSpan _pollInterval;
    private readonly TimeSpan _levelStabilize;
    private readonly int _levelGroupSize;
    private readonly int _secondLevelGroupSize;
    private readonly TimeSpan _groupDelay;
    private readonly TimeSpan _resendInterval;
    private readonly int _maxMeshLevel;
    private readonly bool _rejectConcurrentBatch;
    private readonly bool _raiseFaultOnIncomplete;
    private readonly bool _abortOnLevelTimeout;
    private readonly bool _abortOnFirstLevelTimeout;
    private readonly bool _sendCanRestartOnComplete;
    private readonly TimeSpan _restartWaitTimeout;

    private readonly ConcurrentDictionary<string, SemaphoreSlim> _rackLocks =
        new(StringComparer.Ordinal);

    public OtaUpgradeService(
        OtaUpgradeRepository repository,
        DeviceRepository devices,
        MqttConnection mqtt,
        RedisHelper redis,
        IConfiguration configuration,
        ILogger<OtaUpgradeService> logger)
    {
        _repository = repository;
        _devices = devices;
        _mqtt = mqtt;
        _redis = redis;
        _logger = logger;

        _enabled = configuration.GetValue<bool?>("OtaUpgrade:Enabled") ?? true;

        var baseMinutes = configuration.GetValue<double?>(
            "OtaUpgrade:BaseTimeoutMinutes") ?? 10;
        if (baseMinutes <= 0)
            throw new InvalidOperationException("OtaUpgrade:BaseTimeoutMinutes 必须大于 0");
        _baseTimeout = TimeSpan.FromMinutes(baseMinutes);

        var incrementMinutes = configuration.GetValue<double?>(
            "OtaUpgrade:TimeoutIncrementMinutes") ?? 10;
        _timeoutIncrement = incrementMinutes > 0
            ? TimeSpan.FromMinutes(incrementMinutes)
            : TimeSpan.Zero;

        // 单层超时的上限：层数越深等待越久，但不能无上限增长。
        // 配成与"第 2 层的理论值"相同即可得到"第 1 层一个值、之后各层固定值"的效果，
        // 例如 Base=5 / Increment=10 / Max=10 → L1=5、L2 起一律 10。
        // 缺省或 ≤0 表示不设上限。
        var maxLevelMinutes = configuration.GetValue<double?>(
            "OtaUpgrade:MaxLevelTimeoutMinutes") ?? 0;
        _maxLevelTimeout = maxLevelMinutes > 0
            ? TimeSpan.FromMinutes(maxLevelMinutes)
            : null;

        // 第 1 层单独的超时窗口；0 或不配表示沿用 BaseTimeoutMinutes。
        var firstLevelMinutes = configuration.GetValue<double?>(
            "OtaUpgrade:FirstLevelTimeoutMinutes") ?? 0;
        _firstLevelTimeout = firstLevelMinutes > 0
            ? TimeSpan.FromMinutes(firstLevelMinutes)
            : null;

        var pollMilliseconds = configuration.GetValue<int?>(
            "OtaUpgrade:PollIntervalMilliseconds") ?? 1000;
        if (pollMilliseconds <= 0)
            throw new InvalidOperationException(
                "OtaUpgrade:PollIntervalMilliseconds 必须大于 0");
        _pollInterval = TimeSpan.FromMilliseconds(pollMilliseconds);

        var stabilizeSeconds = configuration.GetValue<double?>(
            "OtaUpgrade:LevelStabilizeSeconds") ?? 60;
        _levelStabilize = stabilizeSeconds > 0
            ? TimeSpan.FromSeconds(stabilizeSeconds)
            : TimeSpan.Zero;

        // 层内分组大小：每层按此切块，一组处理完才发下一组（不足一组也单独成组）。
        _levelGroupSize = configuration.GetValue<int?>("OtaUpgrade:LevelGroupSize") ?? 3;
        if (_levelGroupSize <= 0)
            throw new InvalidOperationException("OtaUpgrade:LevelGroupSize 必须大于 0");

        // 第 2 层单独的分组大小（默认 1 = 逐台串行）。
        // 第 2 层节点全部经根节点中继，并发下载会把根节点的转发压垮，所以强制一台一台来。
        _secondLevelGroupSize = configuration.GetValue<int?>(
            "OtaUpgrade:SecondLevelGroupSize") ?? 1;
        if (_secondLevelGroupSize <= 0)
            throw new InvalidOperationException("OtaUpgrade:SecondLevelGroupSize 必须大于 0");

        // 组间延时：一组处理完后等这么久再下发下一组，给设备端留收尾时间。
        // 0 表示不延时（下一轮 1 秒轮询就会发下一组）。
        var groupDelaySeconds = configuration.GetValue<double?>(
            "OtaUpgrade:GroupDelaySeconds") ?? 5;
        _groupDelay = groupDelaySeconds > 0
            ? TimeSpan.FromSeconds(groupDelaySeconds)
            : TimeSpan.Zero;

        // 补发间隔：当前小组有设备迟迟不上报 OTACP 时，每隔这么久把升级指令重发一次。
        // 0 表示不补发（只等本组超时）。
        var resendMinutes = configuration.GetValue<double?>(
            "OtaUpgrade:ResendIntervalMinutes") ?? 5;
        _resendInterval = resendMinutes > 0
            ? TimeSpan.FromMinutes(resendMinutes)
            : TimeSpan.Zero;

        _maxMeshLevel = configuration.GetValue<int?>("OtaUpgrade:MaxMeshLevel") ?? 10;
        if (_maxMeshLevel <= 0)
            throw new InvalidOperationException("OtaUpgrade:MaxMeshLevel 必须大于 0");

        _rejectConcurrentBatch = configuration.GetValue<bool?>(
            "OtaUpgrade:RejectConcurrentBatchPerRack") ?? true;

        _raiseFaultOnIncomplete = configuration.GetValue<bool?>(
            "OtaUpgrade:RaiseFaultOnIncomplete") ?? true;

        _abortOnLevelTimeout = configuration.GetValue<bool?>(
            "OtaUpgrade:AbortOnLevelTimeout") ?? false;

        // 第 1 层（根节点）超时 = 本次 OTA 判定失败，直接取消后续所有层级。
        _abortOnFirstLevelTimeout = configuration.GetValue<bool?>(
            "OtaUpgrade:AbortOnFirstLevelTimeout") ?? true;

        // 分层全部结束后是否广播 server/rack/{架}/CanRestart 让设备重启加载新固件。
        _sendCanRestartOnComplete = configuration.GetValue<bool?>(
            "OtaUpgrade:SendCanRestartOnComplete") ?? true;

        // 收尾时若仍有设备没升级完成，先等它们补齐再重启；窗口到期仍没齐也照常重启。
        var restartWaitMinutes = configuration.GetValue<double?>(
            "OtaUpgrade:RestartWaitTimeoutMinutes") ?? 20;
        _restartWaitTimeout = restartWaitMinutes > 0
            ? TimeSpan.FromMinutes(restartWaitMinutes)
            : TimeSpan.Zero;
    }

    /// <summary>是否接管多设备 ota 指令。关闭时所有 ota 都按普通命令直接转发。</summary>
    public bool Enabled => _enabled;

    /// <summary>
    /// 尝试接管一条多设备 ota 指令。调用方只在 DeviceInfos 数量大于 1 时调用。
    /// 接管成功后命令流不再逐台下发，改由本服务按 Mesh 层级调度。
    /// </summary>
    public async Task<TakeOverDecision> TryTakeOverAsync(IReadOnlyList<CmdInfo> commands)
    {
        if (commands.Count == 0)
            return TakeOverDecision.Reject("ota 指令没有有效 DeviceInfos");

        var first = commands[0];
        var agingNumber = await ResolveAgingNumberAsync(first.DeviceId);
        if (agingNumber is null)
        {
            return TakeOverDecision.Reject(
                "首节点缺少有效老化架号，无法确定 OTA 批次归属");
        }

        var rackLock = GetRackLock(agingNumber);
        await rackLock.WaitAsync();
        try
        {
            var active = await _repository.GetActiveBatchSeqAsync(agingNumber);
            if (_rejectConcurrentBatch && active.HasValue && active.Value != first.Seq)
            {
                return TakeOverDecision.Reject(
                    $"老化架已有未完成的 OTA 批次 Seq={active.Value}");
            }

            await _repository.RegisterRackAsync(agingNumber);
            await _repository.SetActiveBatchAsync(agingNumber, first.Seq);

            // 任务表 = 平台 DeviceInfos ∩ 该架在线列表 lgw:ota:{AgingNumber}。
            // 只用 DeviceInfos 会把离线设备也算进来、白等一轮超时；
            // 只用在线列表则会丢掉 RecordId（命令状态回写与老化关联都依赖它），
            // 并且会升级平台没点名的设备。取交集两头都保住。
            var online = await ReadRackOnlineDeviceIdsAsync(agingNumber);
            var requested = commands
                .Select(command => RedisKeys.Normalize(command.DeviceId))
                .Distinct(StringComparer.Ordinal)
                .ToArray();

            var targets = requested.Where(online.Contains).ToArray();
            var excluded = requested.Length - targets.Length;

            if (targets.Length == 0)
            {
                await _repository.ClearActiveBatchAsync(agingNumber);

                _logger.LogWarning(
                    "OTA 指令的设备全部不在该架在线列表中，拒绝接管，AgingNumber={AgingNumber}, Seq={Seq}, Requested={Requested}",
                    agingNumber,
                    first.Seq,
                    requested.Length);

                return TakeOverDecision.Reject(
                    $"ota 指令的 {requested.Length} 台设备均不在 lgw:ota:{agingNumber} 在线列表中");
            }

            var state = new OtaBatchState
            {
                Seq = first.Seq,
                CurrentLevel = 1,
                LevelStatus = OtaLevelStatuses.Pending,
                TotalCount = targets.Length,
                DataJson = first.Data?.ToString(Formatting.None) ?? string.Empty
            };
            await _repository.SaveBatchStateAsync(agingNumber, state);

            await _repository.SetTargetsAsync(
                agingNumber,
                first.Seq,
                targets,
                OtaTargetStatuses.Pending);

            foreach (var command in commands)
            {
                var deviceId = RedisKeys.Normalize(command.DeviceId);
                if (!targets.Contains(deviceId))
                    continue;

                await _repository.SetRecordIdAsync(
                    agingNumber,
                    first.Seq,
                    deviceId,
                    command.RecordId);
            }

            _logger.LogWarning(
                "OTA 分层批次已接管，AgingNumber={AgingNumber}, Seq={Seq}, Requested={Requested}, Targets={TargetCount}, ExcludedByOffline={Excluded}, MaxMeshLevel={MaxMeshLevel}",
                agingNumber,
                first.Seq,
                requested.Length,
                targets.Length,
                excluded,
                _maxMeshLevel);

            return TakeOverDecision.Accept(agingNumber);
        }
        finally
        {
            rackLock.Release();
        }
    }

    /// <summary>
    /// 由 <c>OtaCompletionHandler</c> 调用：设备上报 OTACP，固件已下载完成。
    ///
    /// 新固件升级**不再重启**，所以版本号可能不变，"版本号变化"已不能作为完成依据，
    /// 改由设备主动上报这条事件，这是当前唯一的完成判据。
    ///
    /// OTACP **不携带 ota 指令 Seq**（payload 里的 Seq 是设备自己的消息序号），
    /// 因此批次归属只能按 deviceId 反查该架的活跃批次。
    /// </summary>
    /// <param name="deviceId">上报设备。</param>
    /// <param name="agingNumber">上报里的架号；为空时按设备 runtime 反查。</param>
    /// <returns>是否命中并标记了某个活跃批次中的设备。</returns>
    public async Task<bool> MarkDeviceUpgradedAsync(string deviceId, string? agingNumber)
    {
        var normalized = RedisKeys.Normalize(deviceId);

        var rack = agingNumber;
        if (string.IsNullOrWhiteSpace(rack))
            rack = await ResolveAgingNumberAsync(normalized);

        if (string.IsNullOrWhiteSpace(rack))
        {
            _logger.LogDebug(
                "OTACP 上报无法归属老化架，忽略，DeviceId={DeviceId}",
                normalized);
            return false;
        }

        var rackLock = GetRackLock(rack);
        await rackLock.WaitAsync();
        try
        {
            var seq = await _repository.GetActiveBatchSeqAsync(rack);
            if (!seq.HasValue)
            {
                _logger.LogDebug(
                    "OTACP 上报时该架没有活跃 OTA 批次，忽略，DeviceId={DeviceId}, AgingNumber={AgingNumber}",
                    normalized,
                    rack);
                return false;
            }

            var targets = await _repository.GetTargetsAsync(rack, seq.Value);
            if (!targets.TryGetValue(normalized, out var status))
            {
                _logger.LogDebug(
                    "OTACP 上报设备不在本批升级名单中，忽略，DeviceId={DeviceId}, AgingNumber={AgingNumber}, Seq={Seq}",
                    normalized,
                    rack,
                    seq.Value);
                return false;
            }

            // 幂等：设备在超时重试时可能重复上报。
            if (string.Equals(status, OtaTargetStatuses.Upgraded, StringComparison.Ordinal))
                return true;

            await _repository.SetTargetStatusAsync(
                rack,
                seq.Value,
                normalized,
                OtaTargetStatuses.Upgraded);

            _logger.LogInformation(
                "OTA 设备上报固件下载完成，标记升级完成，AgingNumber={AgingNumber}, Seq={Seq}, DeviceId={DeviceId}, OldStatus={OldStatus}",
                rack,
                seq.Value,
                normalized,
                status);

            return true;
        }
        finally
        {
            rackLock.Release();
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "OTA 分层升级协调服务已启动，Enabled={Enabled}, BaseTimeoutMinutes={BaseTimeoutMinutes}, FirstLevelTimeoutMinutes={FirstLevelTimeoutMinutes}, TimeoutIncrementMinutes={TimeoutIncrementMinutes}, MaxLevelTimeoutMinutes={MaxLevelTimeoutMinutes}, LevelGroupSize={LevelGroupSize}, SecondLevelGroupSize={SecondLevelGroupSize}, GroupDelaySeconds={GroupDelaySeconds}, ResendIntervalMinutes={ResendIntervalMinutes}, LevelStabilizeSeconds={LevelStabilizeSeconds}, MaxMeshLevel={MaxMeshLevel}, AbortOnLevelTimeout={AbortOnLevelTimeout}, AbortOnFirstLevelTimeout={AbortOnFirstLevelTimeout}, SendCanRestartOnComplete={SendCanRestartOnComplete}, RestartWaitTimeoutMinutes={RestartWaitTimeoutMinutes}",
            _enabled,
            _baseTimeout.TotalMinutes,
            _firstLevelTimeout?.TotalMinutes,
            _timeoutIncrement.TotalMinutes,
            _maxLevelTimeout?.TotalMinutes,
            _levelGroupSize,
            _secondLevelGroupSize,
            _groupDelay.TotalSeconds,
            _resendInterval.TotalMinutes,
            _levelStabilize.TotalSeconds,
            _maxMeshLevel,
            _abortOnLevelTimeout,
            _abortOnFirstLevelTimeout,
            _sendCanRestartOnComplete,
            _restartWaitTimeout.TotalMinutes);

        if (!_enabled)
            return;

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
                _logger.LogError(ex, "OTA 分层批次扫描失败");
            }

            try
            {
                await Task.Delay(_pollInterval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private async Task ProcessRackAsync(string agingNumber, CancellationToken cancellationToken)
    {
        var rackLock = GetRackLock(agingNumber);
        await rackLock.WaitAsync(cancellationToken);
        try
        {
            var seq = await _repository.GetActiveBatchSeqAsync(agingNumber);
            if (!seq.HasValue)
                return;

            var state = await _repository.GetBatchStateAsync(agingNumber, seq.Value);
            if (state is null)
            {
                await _repository.ClearActiveBatchAsync(agingNumber);
                return;
            }

            if (string.Equals(state.Status, OtaBatchStatuses.Finished, StringComparison.Ordinal))
            {
                await _repository.ClearActiveBatchAsync(agingNumber);
                return;
            }

            switch (state.LevelStatus)
            {
                case OtaLevelStatuses.Pending:
                    await StartLevelAsync(agingNumber, state, cancellationToken);
                    break;
                case OtaLevelStatuses.Upgrading:
                    await VerifyLevelAsync(agingNumber, state);
                    break;
                case OtaLevelStatuses.Stabilizing:
                    await VerifyStabilizeAsync(agingNumber, state);
                    break;
                case OtaLevelStatuses.Finalizing:
                    await FinalizeDispatchAsync(agingNumber, state, cancellationToken);
                    break;
                case OtaLevelStatuses.Restarting:
                    await VerifyRestartAsync(agingNumber, state);
                    break;
                default:
                    await AdvanceLevelAsync(agingNumber, state);
                    break;
            }
        }
        finally
        {
            rackLock.Release();
        }
    }

    /// <summary>
    /// 下发当前层里**当前小组**的设备。
    ///
    /// 层内分组：该层设备按 `LevelGroupSize`（当前 3 台）切成若干组，不足一组也单独成组。
    /// 一组全部处理完（都上报 OTACP 或超时标 failed）才下发下一组，
    /// 本层所有组都处理完才进入下一层。
    ///
    /// 分组依据是**该层清单的稳定快照**（targets ∩ 层清单，按 DeviceId 排序），
    /// 不能用"仍待升级的设备"来切——每完成一台它就从名单里消失，组边界会漂移，
    /// 组号就对不上了。
    /// </summary>
    private async Task StartLevelAsync(
        string agingNumber,
        OtaBatchState state,
        CancellationToken cancellationToken)
    {
        if (state.CurrentLevel > _maxMeshLevel)
        {
            state.LevelStatus = OtaLevelStatuses.Finalizing;
            state.StabilizeUntilUtc = null;
            await _repository.SaveBatchStateAsync(agingNumber, state);
            return;
        }

        var level = state.CurrentLevel;
        var targets = await _repository.GetTargetsAsync(agingNumber, state.Seq);
        var groups = await BuildLevelGroupsAsync(agingNumber, state, targets);

        if (groups.Count == 0)
        {
            // 空层快跳：本层清单里没有任何名单设备，直接往上一层找，不等层间延时。
            _logger.LogInformation(
                "OTA 第 {Level} 层没有名单设备，直接进入下一层，AgingNumber={AgingNumber}, Seq={Seq}, Dispatchable={DispatchableCount}",
                level,
                agingNumber,
                state.Seq,
                targets.Values.Count(IsDispatchable));

            if (targets.Values.Count(IsDispatchable) == 0)
            {
                await FinishBatchAsync(agingNumber, state, "名单中已没有待升级设备");
                return;
            }

            await AdvanceLevelAsync(agingNumber, state);
            return;
        }

        if (state.CurrentGroupIndex < 1)
            state.CurrentGroupIndex = 1;

        if (state.CurrentGroupIndex > groups.Count)
        {
            // 本层所有小组都处理完了 → 本层完成，等层间延时后进入下一层。
            // 这里不受组间延时影响：最后一组后面没有"下一组"了。
            await CompleteLevelAsync(
                agingNumber,
                state,
                $"本层 {groups.Count} 组全部处理完成");
            return;
        }

        // 组间延时：上一组刚处理完，先等满 GroupDelaySeconds 再下发下一组。
        if (state.GroupDelayUntilUtc.HasValue &&
            DateTime.UtcNow < state.GroupDelayUntilUtc.Value)
        {
            return;
        }

        var group = groups[state.CurrentGroupIndex - 1];

        // dispatched 只记录当前小组的下发情况，纯诊断用途；
        // 完成判定看的是该组成员在 targets 里的状态。
        await _repository.ClearDispatchedAsync(agingNumber, state.Seq);

        var candidates = group
            .Where(deviceId => targets.TryGetValue(deviceId, out var status) && IsDispatchable(status))
            .ToArray();

        var dispatched = new List<string>();
        foreach (var deviceId in candidates)
        {
            // 不再记录"下发前的固件版本号"：完成判据已改为设备上报 OTACP，
            // 且新固件不再重启，版本号可能不变，比对了也没意义。
            if (!await _devices.IsDeviceOnlineAsync(deviceId))
            {
                _logger.LogWarning(
                    "OTA 目标设备不在线，跳过本次下发，AgingNumber={AgingNumber}, Seq={Seq}, Level={Level}, DeviceId={DeviceId}",
                    agingNumber,
                    state.Seq,
                    level,
                    deviceId);

                await _repository.SetTargetStatusAsync(
                    agingNumber,
                    state.Seq,
                    deviceId,
                    OtaTargetStatuses.Skipped);
                continue;
            }

            // 先置为 pending：skipped 设备补发时由此重置，避免它在后续层被重复下发。
            await _repository.SetTargetStatusAsync(
                agingNumber,
                state.Seq,
                deviceId,
                OtaTargetStatuses.Pending);

            var recordId = await _repository.GetRecordIdAsync(agingNumber, state.Seq, deviceId);
            try
            {
                await _mqtt.PublishAsync(
                    BuildOtaTopic(deviceId),
                    BuildOtaPayload(state, recordId),
                    cancellationToken);

                await _repository.AddDispatchedAsync(agingNumber, state.Seq, deviceId);
                await _repository.AddDispatchedAllAsync(agingNumber, state.Seq, deviceId);
                dispatched.Add(deviceId);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                // 发布失败不算已下发，退回 skipped 留给后续层重试。
                await _repository.SetTargetStatusAsync(
                    agingNumber,
                    state.Seq,
                    deviceId,
                    OtaTargetStatuses.Skipped);

                _logger.LogWarning(
                    ex,
                    "OTA 指令下发失败，本轮不下发该设备，AgingNumber={AgingNumber}, Seq={Seq}, Level={Level}, DeviceId={DeviceId}",
                    agingNumber,
                    state.Seq,
                    level,
                    deviceId);
            }
        }

        if (dispatched.Count == 0)
        {
            var remaining = await _repository.GetTargetsAsync(agingNumber, state.Seq);
            var dispatchable = remaining.Values.Count(IsDispatchable);

            _logger.LogInformation(
                "OTA 第 {Level} 层第 {GroupIndex}/{GroupCount} 组没有可下发设备，直接进入下一组，AgingNumber={AgingNumber}, Seq={Seq}, GroupSize={GroupSize}, Dispatchable={DispatchableCount}",
                level,
                state.CurrentGroupIndex,
                groups.Count,
                agingNumber,
                state.Seq,
                group.Length,
                dispatchable);

            // 名单里确实没有待升级设备时才收尾。
            if (dispatchable == 0)
            {
                await FinishBatchAsync(agingNumber, state, "名单中已没有待升级设备");
                return;
            }

            // 本组无可下发设备（全部离线 / 已升级 / 发布失败）也算处理完，立即进下一组。
            await AdvanceGroupAsync(agingNumber, state);
            return;
        }

        state.LevelStatus = OtaLevelStatuses.Upgrading;
        state.LevelStartedUtc = DateTime.UtcNow;
        // 每小组独立计时：前一组卡住不会吃掉后面组的时间预算。
        state.LevelDeadlineUtc = DateTime.UtcNow + LevelTimeout(level);

        // 新小组：补发计数清零，第一次补发在 ResendInterval 之后。
        state.ResendCount = 0;
        state.NextResendUtc = _resendInterval > TimeSpan.Zero
            ? DateTime.UtcNow + _resendInterval
            : null;

        await _repository.SaveBatchStateAsync(agingNumber, state);

        _logger.LogWarning(
            "OTA 第 {Level} 层第 {GroupIndex}/{GroupCount} 组已下发，AgingNumber={AgingNumber}, Seq={Seq}, GroupSize={GroupSize}, Dispatched={DispatchedCount}, DeadlineUtc={DeadlineUtc}",
            level,
            state.CurrentGroupIndex,
            groups.Count,
            agingNumber,
            state.Seq,
            group.Length,
            dispatched.Count,
            state.LevelDeadlineUtc?.ToString("O") ?? "<empty>");
    }

    /// <summary>
    /// 构造当前层的分组：取本层**成员快照**按 DeviceId 排序后切块。
    ///
    /// 快照在进入该层时算一次并落库（`OtaBatchLevelMembers`），本层内所有判定都用它。
    /// **绝不能每次按实时的 `lgw:ota:{架}:{层}` 清单重算**：
    /// 设备一掉线就会被移出该清单，成员数缩水甚至变 0，
    /// 于是 `CurrentGroupIndex > groups.Count` 成立 → 本层被误判成"全部完成"，
    /// 而设备其实还没升级，重新上线后也不会再被下发（实测踩过这个坑）。
    ///
    /// 同样不能用"仍待升级的设备"来切：每完成一台就少一台，组边界会漂移。
    /// </summary>
    private async Task<List<string[]>> BuildLevelGroupsAsync(
        string agingNumber,
        OtaBatchState state,
        Dictionary<string, string> targets)
    {
        var level = state.CurrentLevel;

        var members = await _repository.GetLevelMembersAsync(agingNumber, state.Seq, level);

        if (members.Length == 0)
        {
            // 该层还没有快照：按「targets ∩ 实时层清单」建立一次并落库。
            var levelDevices = await _repository.GetMeshLevelDeviceIdsAsync(agingNumber, level);

            var snapshot = targets.Keys
                .Where(levelDevices.Contains)
                .OrderBy(deviceId => deviceId, StringComparer.Ordinal)
                .ToArray();

            if (snapshot.Length > 0)
            {
                await _repository.SetLevelMembersAsync(agingNumber, state.Seq, level, snapshot);

                _logger.LogInformation(
                    "OTA 建立第 {Level} 层成员快照，AgingNumber={AgingNumber}, Seq={Seq}, Members={MemberCount}",
                    level,
                    agingNumber,
                    state.Seq,
                    snapshot.Length);
            }

            members = snapshot;
        }

        var ordered = members
            .OrderBy(deviceId => deviceId, StringComparer.Ordinal)
            .ToArray();

        // 第 2 层单独配（默认逐台），其余层用 LevelGroupSize。
        var groupSize = level == 2 ? _secondLevelGroupSize : _levelGroupSize;

        return ordered.Chunk(groupSize).ToList();
    }

    /// <summary>
    /// 补发：把当前小组里**仍未上报 OTACP** 的设备的升级指令重发一次。
    ///
    /// 触发条件：距离上次下发/补发已过 `ResendIntervalMinutes`（默认 5 分钟），
    /// 且本组还没到 30 分钟超时。到超时就不再补发，直接判定失败。
    ///
    /// payload 里的 `Seq` 是批次 Seq（`BuildOtaPayload` 固定用 `state.Seq`），
    /// **补发不会改变 Seq**，设备端可以按 Seq 幂等去重。
    ///
    /// 日志里以"补发第 N 次"明确标记，便于和首次下发区分。
    /// </summary>
    private async Task ResendGroupAsync(
        string agingNumber,
        OtaBatchState state,
        string[] unfinished,
        int groupIndex,
        int groupCount)
    {
        // 先推进计时并落库：即使下面全部发布失败，也不会在下一秒立刻又触发一次。
        state.ResendCount++;
        state.NextResendUtc = DateTime.UtcNow + _resendInterval;
        await _repository.SaveBatchStateAsync(agingNumber, state);

        var resent = 0;
        var offline = 0;

        foreach (var deviceId in unfinished)
        {
            if (!await _devices.IsDeviceOnlineAsync(deviceId))
            {
                // 离线的补发也没用，等它自己回来或本组超时。
                offline++;
                continue;
            }

            var recordId = await _repository.GetRecordIdAsync(agingNumber, state.Seq, deviceId);

            try
            {
                await _mqtt.PublishAsync(
                    BuildOtaTopic(deviceId),
                    BuildOtaPayload(state, recordId));

                await _repository.AddDispatchedAllAsync(agingNumber, state.Seq, deviceId);
                resent++;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "OTA 补发下发失败，AgingNumber={AgingNumber}, Seq={Seq}, Level={Level}, DeviceId={DeviceId}",
                    agingNumber,
                    state.Seq,
                    state.CurrentLevel,
                    deviceId);
            }
        }

        _logger.LogWarning(
            "OTA 补发第 {ResendCount} 次（第 {Level} 层 {GroupIndex}/{GroupCount} 组），已重发={Resent}, 离线跳过={Offline}, 待完成={Pending}, NextResendUtc={NextResendUtc}, AgingNumber={AgingNumber}, Seq={Seq}, DeviceIds={DeviceIds}",
            state.ResendCount,
            state.CurrentLevel,
            groupIndex,
            groupCount,
            resent,
            offline,
            unfinished.Length,
            state.NextResendUtc?.ToString("O") ?? "<empty>",
            agingNumber,
            state.Seq,
            string.Join(",", unfinished));
    }

    /// <summary>
    /// 当前小组处理完（全部上报 / 超时标 failed / 无可下发设备），进入下一组。
    /// 组间固定延时 `GroupDelaySeconds`（当前 5 秒）后再下发下一组，
    /// 由 `StartLevelAsync` 读取 <c>GroupDelayUntilUtc</c> 执行。
    ///
    /// 若已是本层最后一组，下一次轮询会由 `StartLevelAsync` 判定"组号超过组数"并走
    /// `CompleteLevelAsync`——那条路径**不受组间延时影响**，直接进层间延时。
    /// </summary>
    private async Task AdvanceGroupAsync(string agingNumber, OtaBatchState state)
    {
        state.CurrentGroupIndex++;
        state.LevelStatus = OtaLevelStatuses.Pending;
        state.LevelStartedUtc = null;
        state.LevelDeadlineUtc = null;
        state.ResendCount = 0;
        state.NextResendUtc = null;
        state.GroupDelayUntilUtc = _groupDelay > TimeSpan.Zero
            ? DateTime.UtcNow + _groupDelay
            : null;

        await _repository.SaveBatchStateAsync(agingNumber, state);

        _logger.LogInformation(
            "OTA 第 {Level} 层第 {GroupIndex} 组处理完成，延时 {DelaySeconds}s 后进入下一组，AgingNumber={AgingNumber}, Seq={Seq}",
            state.CurrentLevel,
            state.CurrentGroupIndex - 1,
            _groupDelay.TotalSeconds,
            agingNumber,
            state.Seq);
    }

    /// <summary>
    /// 等待**当前小组**各设备上报 OTACP（固件下载完成）。
    ///
    /// 判定粒度是小组：该组成员在 targets 里都不再是 `pending`（即已 upgraded / failed / skipped）
    /// 就算本组处理完，立即进入下一组；本层所有组处理完才走层间延时进入下一层。
    ///
    /// 状态由 <c>OtaCompletionHandler</c> 依据设备主动上报的 OTACP 写入，
    /// **不再比对固件版本号**：新固件升级后设备不再重启，版本号可能根本不变。
    /// </summary>
    private async Task VerifyLevelAsync(string agingNumber, OtaBatchState state)
    {
        var level = state.CurrentLevel;
        var targets = await _repository.GetTargetsAsync(agingNumber, state.Seq);
        var groups = await BuildLevelGroupsAsync(agingNumber, state, targets);

        var groupIndex = state.CurrentGroupIndex < 1 ? 1 : state.CurrentGroupIndex;

        if (groupIndex > groups.Count)
        {
            await CompleteLevelAsync(agingNumber, state, $"本层 {groups.Count} 组全部处理完成");
            return;
        }

        var group = groups[groupIndex - 1];

        // 本组"还没定论"的设备 = 仍为 pending 的。
        // 已 upgraded（上报完成）/ failed（超时）/ skipped（离线）都算本组已处理。
        var unfinished = group
            .Where(id => targets.TryGetValue(id, out var status) &&
                         string.Equals(status, OtaTargetStatuses.Pending, StringComparison.Ordinal))
            .ToArray();

        if (unfinished.Length == 0)
        {
            // 本组完成，进下一组（组间有 GroupDelaySeconds 延时）。
            await AdvanceGroupAsync(agingNumber, state);
            return;
        }

        var deadlineReached =
            state.LevelDeadlineUtc.HasValue && DateTime.UtcNow >= state.LevelDeadlineUtc.Value;

        if (!deadlineReached)
        {
            // 还没到本组超时：按 ResendIntervalMinutes 周期补发。
            // 只补发仍是 pending 的设备——已 upgraded 的不会再发，避免把下完的设备重刷一遍。
            if (_resendInterval > TimeSpan.Zero &&
                (!state.NextResendUtc.HasValue || DateTime.UtcNow >= state.NextResendUtc.Value))
            {
                await ResendGroupAsync(agingNumber, state, unfinished, groupIndex, groups.Count);
            }

            return;
        }

        foreach (var deviceId in unfinished)
        {
            _logger.LogWarning(
                "OTA 设备未在时限内完成升级，标记失败并留给更高层重试，AgingNumber={AgingNumber}, Seq={Seq}, Level={Level}, DeviceId={DeviceId}",
                agingNumber,
                state.Seq,
                level,
                deviceId);

            // 这里只标 failed，不写 fault：failed 设备允许在后续层重新下发，
            // 每层都写会重复告警；统一在批次收尾时给"最终仍未成功"的设备写一次。
            await _repository.SetTargetStatusAsync(
                agingNumber,
                state.Seq,
                deviceId,
                OtaTargetStatuses.Failed);
        }

        // 超时即判定本次 OTA 失败，取消后续所有操作（不进下一组、不进补发、不广播重启）。
        // 两个触发条件：
        // ① 第 1 层（根节点）超时——根节点失败后续层级建不起可用 mesh，直接作废；
        // ② AbortOnLevelTimeout=true——任意层超时都作废（当前就是这么配的）。
        var firstLevelTimeout = _abortOnFirstLevelTimeout && level <= 1;

        if (firstLevelTimeout || _abortOnLevelTimeout)
        {
            await FinishBatchAsync(
                agingNumber,
                state,
                firstLevelTimeout
                    ? $"第 1 层（根节点）第 {groupIndex}/{groups.Count} 组升级超时，判定 OTA 升级失败，取消后续所有层级"
                    : $"第 {level} 层第 {groupIndex}/{groups.Count} 组升级超时，判定 OTA 升级失败，取消后续所有层级",
                // 超时=失败，整批作废，不再让设备重启。
                allowRestart: false);

            return;
        }

        // 本组有设备没在时限内上报完成，直接进下一组（不等层间延时）。
        // failed 设备若后续 MeshLevel 上移，会在更高层重新进入下发名单。
        _logger.LogWarning(
            "OTA 第 {Level} 层第 {GroupIndex}/{GroupCount} 组等待超时，进入下一组，AgingNumber={AgingNumber}, Seq={Seq}, Failed={FailedCount}",
            level,
            groupIndex,
            groups.Count,
            agingNumber,
            state.Seq,
            unfinished.Length);

        await AdvanceGroupAsync(agingNumber, state);
    }

    /// <summary>
    /// 层间等待：本层**所有小组**都处理完成后，固定等待 `LevelStabilizeSeconds`
    /// （当前 10 秒）再进入下一层。
    ///
    /// 原先这里的"在线数回到基准即认为 mesh 稳定"判定已删除：
    /// 新固件升级**不再重启**，设备不会掉线、mesh 不会重组，
    /// 在线数从头到尾恒定，那个判定永远是"已稳定"、等于没有。
    /// 现在保留固定延时只是给设备留出收尾的时间。
    /// </summary>
    private async Task VerifyStabilizeAsync(string agingNumber, OtaBatchState state)
    {
        if (state.StabilizeUntilUtc.HasValue && DateTime.UtcNow < state.StabilizeUntilUtc.Value)
            return;

        _logger.LogInformation(
            "OTA 第 {Level} 层已完成，层间等待结束，进入下一层，AgingNumber={AgingNumber}, Seq={Seq}",
            state.CurrentLevel,
            agingNumber,
            state.Seq);

        await AdvanceLevelAsync(agingNumber, state);
    }

    private async Task CompleteLevelAsync(string agingNumber, OtaBatchState state, string reason)
    {
        state.LevelStatus = OtaLevelStatuses.Stabilizing;
        state.LevelStartedUtc = null;
        state.LevelDeadlineUtc = null;
        state.GroupDelayUntilUtc = null;
        state.StabilizeUntilUtc = _levelStabilize > TimeSpan.Zero
            ? DateTime.UtcNow + _levelStabilize
            : null;
        await _repository.SaveBatchStateAsync(agingNumber, state);

        _logger.LogInformation(
            "OTA 第 {Level} 层处理结束，{Reason}，AgingNumber={AgingNumber}, Seq={Seq}, StabilizeSeconds={StabilizeSeconds}",
            state.CurrentLevel,
            reason,
            agingNumber,
            state.Seq,
            _levelStabilize.TotalSeconds);
    }

    private async Task AdvanceLevelAsync(string agingNumber, OtaBatchState state)
    {
        state.CurrentLevel++;
        state.CurrentGroupIndex = 1;   // 新层从第 1 组重新开始
        state.LevelStatus = OtaLevelStatuses.Pending;
        state.StabilizeUntilUtc = null;
        state.GroupDelayUntilUtc = null;   // 新层第 1 组不继承上一层的组间延时

        if (state.CurrentLevel > _maxMeshLevel)
        {
            // 层都走完了，但名单里可能还有设备从未被下发过（节点重启后 MeshLevel 上移，
            // 分层清单里始终没出现过它）。先进入补发阶段把它们补齐，再收尾。
            state.LevelStatus = OtaLevelStatuses.Finalizing;
            state.StabilizeUntilUtc = null;
            await _repository.SaveBatchStateAsync(agingNumber, state);
            return;
        }

        await _repository.SaveBatchStateAsync(agingNumber, state);
    }

    /// <summary>
    /// 收尾补发：所有 Mesh 层级都已走完，把 targets 里"从未被下发过"的设备直接发一次
    /// ota（不再按 mesh 分层），发完立即收尾。
    /// </summary>
    private async Task FinalizeDispatchAsync(
        string agingNumber,
        OtaBatchState state,
        CancellationToken cancellationToken)
    {
        var targets = await _repository.GetTargetsAsync(agingNumber, state.Seq);
        var dispatched = await _repository.GetDispatchedAllAsync(agingNumber, state.Seq);

        var missing = targets
            .Where(item => IsDispatchable(item.Value) && !dispatched.Contains(item.Key))
            .Select(item => item.Key)
            .OrderBy(deviceId => deviceId, StringComparer.Ordinal)
            .ToArray();

        if (missing.Length == 0)
        {
            // 没有需要补发的设备，但名单里可能仍有 failed 的设备没完成，
            // 同样要走"是否等剩余设备补齐再重启"的判定。
            await EnterRestartPhaseAsync(agingNumber, state, "分层下发结束，无需补发");
            return;
        }

        _logger.LogWarning(
            "OTA 分层下发结束，仍有 {MissingCount} 台设备从未被下发，开始兜底补发，AgingNumber={AgingNumber}, Seq={Seq}, DeviceIds={DeviceIds}",
            missing.Length,
            agingNumber,
            state.Seq,
            string.Join(",", missing));

        foreach (var deviceId in missing)
        {
            // 补发后不等确认，因此不记录版本基线（记录了也没人比对）。
            if (!await _devices.IsDeviceOnlineAsync(deviceId))
            {
                _logger.LogWarning(
                    "OTA 兜底补发目标设备不在线，跳过，AgingNumber={AgingNumber}, Seq={Seq}, DeviceId={DeviceId}",
                    agingNumber,
                    state.Seq,
                    deviceId);

                await _repository.SetTargetStatusAsync(
                    agingNumber,
                    state.Seq,
                    deviceId,
                    OtaTargetStatuses.Skipped);
                continue;
            }

            var recordId = await _repository.GetRecordIdAsync(agingNumber, state.Seq, deviceId);
            try
            {
                await _mqtt.PublishAsync(
                    BuildOtaTopic(deviceId),
                    BuildOtaPayload(state, recordId),
                    cancellationToken);

                await _repository.AddDispatchedAsync(agingNumber, state.Seq, deviceId);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "OTA 兜底补发下发失败，AgingNumber={AgingNumber}, Seq={Seq}, DeviceId={DeviceId}",
                    agingNumber,
                    state.Seq,
                    deviceId);
            }
        }

        // 补发完不直接收尾：此时可能还有设备没升级完成（failed/skipped/刚补发的 pending）。
        // 若有未完成设备，先给一个等待窗口，等它们补齐再广播重启；
        // 窗口到期仍没齐也照常重启。详见 EnterRestartPhaseAsync。
        await EnterRestartPhaseAsync(agingNumber, state, "分层下发与兜底补发结束");
    }

    /// <summary>
    /// 决定"立即重启"还是"等剩余设备补齐后再重启"。
    ///
    /// 规则：
    /// - 没有未完成设备（全部 upgraded）→ 直接收尾，由 FinishBatchAsync 广播重启；
    /// - 有未完成设备 → 进入 `Restarting` 阶段，起 `RestartWaitTimeoutMinutes`（默认 20 分钟）窗口：
    ///   窗口内全部补齐就立即重启，到期仍有未完成也照常重启（不再等）；
    /// - 一台都没升级成功 → 重启没有意义，直接收尾（FinishBatchAsync 内部会跳过广播）。
    ///
    /// 注意：不能在这里就把 pending 标成 skipped，否则窗口内上报 OTACP 的设备会被
    /// 当成"从未完成"，统计也不准。pending → skipped 的统一转换留在 FinishBatchAsync。
    /// </summary>
    private async Task EnterRestartPhaseAsync(
        string agingNumber,
        OtaBatchState state,
        string reason)
    {
        var targets = await _repository.GetTargetsAsync(agingNumber, state.Seq);

        var upgraded = targets.Values.Count(
            status => string.Equals(status, OtaTargetStatuses.Upgraded, StringComparison.Ordinal));
        var unfinished = targets.Count - upgraded;

        if (unfinished == 0)
        {
            await FinishBatchAsync(agingNumber, state, $"{reason}，名单设备全部升级完成");
            return;
        }

        if (upgraded == 0)
        {
            // 一台都没成，重启只会打断可能正在进行的下载，直接收尾（FinishBatchAsync 会跳过广播）。
            await FinishBatchAsync(agingNumber, state, $"{reason}，无任何设备升级完成，跳过重启等待");
            return;
        }

        if (!_sendCanRestartOnComplete || _restartWaitTimeout <= TimeSpan.Zero)
        {
            await FinishBatchAsync(agingNumber, state, $"{reason}，未开启重启等待");
            return;
        }

        state.LevelStatus = OtaLevelStatuses.Restarting;
        state.StabilizeUntilUtc = null;
        state.RestartDeadlineUtc = DateTime.UtcNow + _restartWaitTimeout;
        await _repository.SaveBatchStateAsync(agingNumber, state);

        _logger.LogWarning(
            "OTA 分层下发结束，仍有 {UnfinishedCount} 台未升级完成，暂不广播重启，等待窗口={WaitMinutes} 分钟，AgingNumber={AgingNumber}, Seq={Seq}, DeadlineUtc={DeadlineUtc}",
            unfinished,
            _restartWaitTimeout.TotalMinutes,
            agingNumber,
            state.Seq,
            state.RestartDeadlineUtc.Value.ToString("O"));
    }

    /// <summary>
    /// `Restarting` 阶段：等剩余设备补齐。
    /// 窗口内全部完成 → 立即重启；窗口到期 → 不论是否补齐都重启。
    /// </summary>
    private async Task VerifyRestartAsync(string agingNumber, OtaBatchState state)
    {
        var targets = await _repository.GetTargetsAsync(agingNumber, state.Seq);

        var upgraded = targets.Values.Count(
            status => string.Equals(status, OtaTargetStatuses.Upgraded, StringComparison.Ordinal));
        var unfinished = targets.Count - upgraded;

        if (unfinished == 0)
        {
            _logger.LogInformation(
                "OTA 剩余设备已在重启等待窗口内全部升级完成，立即广播重启，AgingNumber={AgingNumber}, Seq={Seq}",
                agingNumber,
                state.Seq);

            await FinishBatchAsync(agingNumber, state, "剩余设备在等待窗口内全部升级完成");
            return;
        }

        if (state.RestartDeadlineUtc.HasValue && DateTime.UtcNow < state.RestartDeadlineUtc.Value)
            return;

        _logger.LogWarning(
            "OTA 重启等待窗口已到期，仍有 {UnfinishedCount} 台未完成，按计划广播重启，AgingNumber={AgingNumber}, Seq={Seq}",
            unfinished,
            agingNumber,
            state.Seq);

        await FinishBatchAsync(agingNumber, state, "重启等待窗口到期，仍有设备未完成");
    }

    /// <param name="allowRestart">
    /// false 时不广播重启。用于"第 1 层超时判定整批失败"这类场景——
    /// 此时取消后续所有操作，不应再让设备重启。
    /// </param>
    private async Task FinishBatchAsync(
        string agingNumber,
        OtaBatchState state,
        string reason,
        bool allowRestart = true)
    {
        var targets = await _repository.GetTargetsAsync(agingNumber, state.Seq);

        foreach (var item in targets
            .Where(x => string.Equals(x.Value, OtaTargetStatuses.Pending, StringComparison.Ordinal))
            .OrderBy(x => x.Key, StringComparer.Ordinal)
            .ToArray())
        {
            await _repository.SetTargetStatusAsync(
                agingNumber,
                state.Seq,
                item.Key,
                OtaTargetStatuses.Skipped);

            _logger.LogWarning(
                "OTA 名单设备从未被下发，标记为跳过，AgingNumber={AgingNumber}, Seq={Seq}, DeviceId={DeviceId}",
                agingNumber,
                state.Seq,
                item.Key);
        }

        state.Status = OtaBatchStatuses.Finished;
        state.FinishedAtUtc = DateTime.UtcNow;
        state.LevelStatus = OtaLevelStatuses.Pending;
        state.LevelStartedUtc = null;
        state.LevelDeadlineUtc = null;
        state.StabilizeUntilUtc = null;
        state.GroupDelayUntilUtc = null;
        state.ResendCount = 0;
        state.NextResendUtc = null;
        state.RestartDeadlineUtc = null;
        await _repository.SaveBatchStateAsync(agingNumber, state);
        await _repository.ClearActiveBatchAsync(agingNumber);

        var final = await _repository.GetTargetsAsync(agingNumber, state.Seq);
        var upgraded = final.Count(x =>
            string.Equals(x.Value, OtaTargetStatuses.Upgraded, StringComparison.Ordinal));
        var failed = final.Count(x =>
            string.Equals(x.Value, OtaTargetStatuses.Failed, StringComparison.Ordinal));
        var skipped = final.Count(x =>
            string.Equals(x.Value, OtaTargetStatuses.Skipped, StringComparison.Ordinal));

        // failed 设备在整个批次里可能已被重试过多次（每次层级上移都会再发一次），
        // 逐层写 fault 会重复告警，因此只在批次收尾时对"最终仍未成功"的设备写一次。
        if (_raiseFaultOnIncomplete)
        {
            var message =
                $"OTA 分层批次结束，设备仍未完成升级，AgingNumber={agingNumber}, Seq={state.Seq}";

            foreach (var item in final
                .Where(x => string.Equals(x.Value, OtaTargetStatuses.Failed, StringComparison.Ordinal))
                .OrderBy(x => x.Key, StringComparer.Ordinal)
                .ToArray())
            {
                await SaveFaultAsync(
                    agingNumber,
                    item.Key,
                    state.Seq,
                    "ota_upgrade_failed",
                    message);
            }
        }

        // 全部层级走完（含收尾补发）后广播重启，让设备加载已下载的新固件。
        if (allowRestart)
            await PublishCanRestartAsync(agingNumber, upgraded);
        else
            _logger.LogWarning(
                "OTA 批次按失败收尾，跳过重启广播，AgingNumber={AgingNumber}, Seq={Seq}",
                agingNumber,
                state.Seq);

        _logger.LogWarning(
            "OTA 分层批次结束，AgingNumber={AgingNumber}, Seq={Seq}, Reason={Reason}, Total={Total}, Upgraded={Upgraded}, Failed={Failed}, Skipped={Skipped}",
            agingNumber,
            state.Seq,
            reason,
            state.TotalCount,
            upgraded,
            failed,
            skipped);
    }

    /// <summary>
    /// 分层下发全部结束后，向该架广播重启指令，让设备加载已下载的新固件。
    ///
    /// 主题 <c>server/rack/{AgingNumber}/CanRestart</c>，
    /// payload <c>{"Seq":&lt;unix秒&gt;,"Data":{"Restart":1}}</c>；
    /// Seq 用当前 Unix 秒（与设备端时间戳风格一致），设备端按 Seq 幂等去重。
    ///
    /// **为什么放在 FinishBatchAsync 而不是"最后一层完成"那一刻**：
    /// 层数超过 MaxMeshLevel 后还会进入 finalizing 阶段，给"从未被下发过"的设备补发 ota；
    /// 如果一完成最后一层就广播重启，补发的设备会没下完就重启，等于白补。
    ///
    /// **为什么要求 upgraded &gt; 0**：一台都没确认完成时广播重启没有任何意义，
    /// 只会让设备白重启一次、打断正在进行的下载。
    /// </summary>
    private async Task PublishCanRestartAsync(string agingNumber, int upgradedCount)
    {
        if (!_sendCanRestartOnComplete)
        {
            _logger.LogInformation(
                "OTA 重启广播已关闭（OtaUpgrade:SendCanRestartOnComplete=false），AgingNumber={AgingNumber}",
                agingNumber);
            return;
        }

        if (upgradedCount <= 0)
        {
            _logger.LogInformation(
                "OTA 批次没有任何设备确认完成，跳过重启广播，AgingNumber={AgingNumber}",
                agingNumber);
            return;
        }

        var topic = $"server/rack/{agingNumber}/CanRestart";
        var payload = JsonConvert.SerializeObject(new
        {
            Seq = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            Data = new { Restart = 1 }
        });

        try
        {
            await _mqtt.PublishAsync(topic, payload);

            _logger.LogWarning(
                "OTA 分层下发全部结束，已广播重启指令，AgingNumber={AgingNumber}, Upgraded={Upgraded}, Topic={Topic}, Payload={Payload}",
                agingNumber,
                upgradedCount,
                topic,
                payload);
        }
        catch (Exception ex)
        {
            // 重启广播失败不影响批次收尾：状态已落库、active 已释放，
            // 只记录错误，避免因为一次发布失败把已经结束的批次重新拖回异常状态。
            _logger.LogError(
                ex,
                "OTA 重启广播发布失败，AgingNumber={AgingNumber}, Topic={Topic}",
                agingNumber,
                topic);
        }
    }

    /// <summary>
    /// 单层超时 = 基准超时 + (层-1)×TimeoutIncrementMinutes，再按 MaxLevelTimeoutMinutes 封顶。
    ///
    /// 第 1 层（根节点）可以用 `FirstLevelTimeoutMinutes` 单独给一个更长的窗口：
    /// 根节点升级失败就没有后续层级可言，值得多等一会儿；
    /// 现有公式（Base + 递增）表达不了"第 1 层比后面长"，因此单独配一项。
    /// </summary>
    private TimeSpan LevelTimeout(int level)
    {
        var baseTimeout = level <= 1 && _firstLevelTimeout.HasValue
            ? _firstLevelTimeout.Value
            : _baseTimeout;

        var timeout = baseTimeout +
            TimeSpan.FromTicks(_timeoutIncrement.Ticks * Math.Max(0, level - 1));

        return _maxLevelTimeout.HasValue && timeout > _maxLevelTimeout.Value
            ? _maxLevelTimeout.Value
            : timeout;
    }

    /// <summary>
    /// 是否还能下发：只要"还没升级成功"就再给一次机会。
    /// pending = 尚未下发过；skipped = 曾因离线或发布失败未下发；
    /// failed = 下发过但在时限内没确认版本号变化，允许在后续层重试。
    ///
    /// 之所以允许重试 failed：节点重启后 MeshLevel 可能上移，它会在更高层重新进入清单；
    /// 若 MeshLevel 没变，它就不会出现在更高层清单里，自然不会重复下发，
    /// 因此重试次数被"真实的层级变化"天然限制住，不需要额外的重试计数。
    /// 只有 upgraded（版本号已变化，确认升级成功）才真正不再下发。
    /// </summary>
    private static bool IsDispatchable(string status)
        => !string.Equals(status, OtaTargetStatuses.Upgraded, StringComparison.Ordinal);

    // 已删除 IsUpgraded：组完成判定改为"该组成员都不再是 pending"，
    // 不再需要单独判断"是否 upgraded"。

    // 已删除 VersionChanged（版本号变化判定）：新固件升级后设备不再重启，
    // 版本号可能根本不变，用它推断"升级完成"既不可靠也容易误判。
    // 完成判据已改为设备主动上报 OTACP（见 OtaCompletionHandler）。

    /// <summary>读取该架在线列表 lgw:ota:{AgingNumber} 的设备 ID（不分设备类型）。</summary>
    private Task<IReadOnlySet<string>> ReadRackOnlineDeviceIdsAsync(string agingNumber)
        => _devices.GetOtaRackOnlineDeviceIdsAsync(agingNumber);

    private async Task<string?> ResolveAgingNumberAsync(string deviceId)
    {
        var device = await _devices.GetAsync(deviceId);
        if (string.IsNullOrWhiteSpace(device?.AgingNumber))
            return null;

        try
        {
            return RedisKeys.NormalizeAgingNumber(device.AgingNumber);
        }
        catch (ArgumentException)
        {
            return null;
        }
    }

    private async Task SaveFaultAsync(
        string agingNumber,
        string deviceId,
        long seq,
        string reason,
        string message)
    {
        var payload = new JObject
        {
            ["AgingNumber"] = agingNumber,
            ["Seq"] = seq,
            ["DeviceId"] = deviceId,
            ["Reason"] = reason,
            ["Message"] = message,
            ["OccurredAtUtc"] = DateTime.UtcNow.ToString("O")
        }.ToString(Formatting.None);

        // 服务端自产故障必须走这个入口：SaveEventAsync 会 MarkSeen 把离线设备复活成在线。
        await _devices.SaveGroupControlFaultAsync(deviceId, seq, message, payload);
        await PublishFaultAsync(deviceId, message);
    }

    private Task PublishFaultAsync(string deviceId, string message)
        => _redis.PublishAsync(
            RedisKeys.channelFault,
            new JObject
            {
                ["DeviceID"] = deviceId,
                ["type"] = "fault",
                ["Message"] = message,
                ["ReceivedAtUtc"] = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss")
            }.ToString(Formatting.None));

    private SemaphoreSlim GetRackLock(string agingNumber)
        => _rackLocks.GetOrAdd(agingNumber, _ => new SemaphoreSlim(1, 1));

    private static string BuildOtaTopic(string deviceId)
        => $"server/{RedisKeys.Normalize(deviceId)}/cmd/ota";

    /// <summary>
    /// 与 RedisCmdWorker.PublishCommandAsync 保持一致的报文结构：
    /// 无参数指令也必须显式带 "Data": null，并原样带出 RecordId。
    /// </summary>
    private static string BuildOtaPayload(OtaBatchState state, string? recordId)
    {
        JToken? data = null;
        if (!string.IsNullOrWhiteSpace(state.DataJson))
        {
            try
            {
                data = JToken.Parse(state.DataJson);
            }
            catch (JsonException)
            {
                data = null;
            }
        }

        return new JObject
        {
            ["Seq"] = state.Seq,
            ["Data"] = data ?? JValue.CreateNull(),
            ["RecordId"] = recordId ?? string.Empty
        }.ToString(Formatting.None);
    }

    public readonly record struct TakeOverDecision(bool Accepted, string AgingNumber, string Reason)
    {
        public static TakeOverDecision Accept(string agingNumber)
            => new(true, agingNumber, string.Empty);

        public static TakeOverDecision Reject(string reason)
            => new(false, string.Empty, reason);
    }
}
