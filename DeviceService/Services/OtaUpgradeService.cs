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
/// 与旧方案（整架一次性下发、全部下载完成后广播 CanRestart）的区别：
/// 收到多设备 ota 指令后，按 Mesh 层级从 1（根节点）开始逐层下发，
/// 每层靠"设备固件版本号是否变化"确认该层真的升级重启成功，再进入下一层。
/// 根节点先升级重建 mesh，下面的节点才不会在升级过程中失联。
///
/// 升级名单是平台下发的 DeviceInfos 全集（成功率统计的分母），
/// 与分层清单 lgw:ota:{AgingNumber}:{MeshLevel} 取交集后才真正下发。
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
    private readonly TimeSpan _pollInterval;
    private readonly TimeSpan _levelStabilize;
    private readonly int _maxMeshLevel;
    private readonly bool _rejectConcurrentBatch;
    private readonly bool _raiseFaultOnIncomplete;
    private readonly bool _abortOnLevelTimeout;

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

        _maxMeshLevel = configuration.GetValue<int?>("OtaUpgrade:MaxMeshLevel") ?? 10;
        if (_maxMeshLevel <= 0)
            throw new InvalidOperationException("OtaUpgrade:MaxMeshLevel 必须大于 0");

        _rejectConcurrentBatch = configuration.GetValue<bool?>(
            "OtaUpgrade:RejectConcurrentBatchPerRack") ?? true;

        _raiseFaultOnIncomplete = configuration.GetValue<bool?>(
            "OtaUpgrade:RaiseFaultOnIncomplete") ?? true;

        _abortOnLevelTimeout = configuration.GetValue<bool?>(
            "OtaUpgrade:AbortOnLevelTimeout") ?? false;
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

            var state = new OtaBatchState
            {
                Seq = first.Seq,
                CurrentLevel = 1,
                LevelStatus = OtaLevelStatuses.Pending,
                TotalCount = commands.Count,
                DataJson = first.Data?.ToString(Formatting.None) ?? string.Empty
            };
            await _repository.SaveBatchStateAsync(agingNumber, state);

            await _repository.SetTargetsAsync(
                agingNumber,
                first.Seq,
                commands.Select(command => command.DeviceId),
                OtaTargetStatuses.Pending);

            foreach (var command in commands)
            {
                await _repository.SetRecordIdAsync(
                    agingNumber,
                    first.Seq,
                    command.DeviceId,
                    command.RecordId);
            }

            _logger.LogWarning(
                "OTA 分层批次已接管，AgingNumber={AgingNumber}, Seq={Seq}, TotalCount={TotalCount}, MaxMeshLevel={MaxMeshLevel}",
                agingNumber,
                first.Seq,
                commands.Count,
                _maxMeshLevel);

            return TakeOverDecision.Accept(agingNumber);
        }
        finally
        {
            rackLock.Release();
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "OTA 分层升级协调服务已启动，Enabled={Enabled}, BaseTimeoutMinutes={BaseTimeoutMinutes}, TimeoutIncrementMinutes={TimeoutIncrementMinutes}, MaxLevelTimeoutMinutes={MaxLevelTimeoutMinutes}, LevelStabilizeSeconds={LevelStabilizeSeconds}, MaxMeshLevel={MaxMeshLevel}, AbortOnLevelTimeout={AbortOnLevelTimeout}",
            _enabled,
            _baseTimeout.TotalMinutes,
            _timeoutIncrement.TotalMinutes,
            _maxLevelTimeout?.TotalMinutes,
            _levelStabilize.TotalSeconds,
            _maxMeshLevel,
            _abortOnLevelTimeout);

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
    /// 开始下发当前层：取「名单中仍待升级」∩「该层清单」的设备，记录旧版本号后并行下发。
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

        // dispatched 只记录当前层：上一层的记录在这里清掉，
        // 本层完成判定只比对 targets 里本层这批设备的最新状态。
        await _repository.ClearDispatchedAsync(agingNumber, state.Seq);

        var levelDevices = await _repository.GetMeshLevelDeviceIdsAsync(agingNumber, level);
        var targets = await _repository.GetTargetsAsync(agingNumber, state.Seq);
        var candidates = targets
            .Where(item => IsDispatchable(item.Value))
            .Select(item => item.Key)
            .Where(levelDevices.Contains)
            .OrderBy(deviceId => deviceId, StringComparer.Ordinal)
            .ToArray();

        // 首次真正下发时记录按架在线列表的个数，作为后续 mesh 稳定判定的基准。
        // 必须在设备开始重启前读，读晚了个数已经掉下去了。
        if (state.ExpectedOnlineCount is null && candidates.Length > 0)
        {
            state.ExpectedOnlineCount =
                (int)await _devices.GetOtaRackOnlineCountAsync(agingNumber);

            _logger.LogInformation(
                "OTA 记录 mesh 稳定基准在线数，AgingNumber={AgingNumber}, Seq={Seq}, Level={Level}, ExpectedOnlineCount={ExpectedOnlineCount}",
                agingNumber,
                state.Seq,
                level,
                state.ExpectedOnlineCount.Value);
        }

        var dispatched = new List<string>();
        foreach (var deviceId in candidates)
        {
            var oldVersion = await _devices.GetFirmwareVersionAsync(deviceId);
            await _repository.SetLevelVersionAsync(
                agingNumber,
                state.Seq,
                level,
                deviceId,
                oldVersion ?? string.Empty);

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
                "OTA 第 {Level} 层没有可下发设备，AgingNumber={AgingNumber}, Seq={Seq}, Candidates={CandidateCount}, Dispatchable={DispatchableCount}",
                level,
                agingNumber,
                state.Seq,
                candidates.Length,
                dispatchable);

            // 节点重启后 MeshLevel 可能上移，本层找不到不代表没有更高层。
            // 只有名单里确实没有待升级设备时才收尾，否则直接往上一层找（不等 mesh 稳定期）。
            if (dispatchable == 0)
            {
                await FinishBatchAsync(agingNumber, state, "名单中已没有待升级设备");
                return;
            }

            await AdvanceLevelAsync(agingNumber, state);
            return;
        }

        state.LevelStatus = OtaLevelStatuses.Upgrading;
        state.LevelStartedUtc = DateTime.UtcNow;
        state.LevelDeadlineUtc = DateTime.UtcNow + LevelTimeout(level);
        await _repository.SaveBatchStateAsync(agingNumber, state);

        _logger.LogWarning(
            "OTA 第 {Level} 层已下发，AgingNumber={AgingNumber}, Seq={Seq}, Dispatched={DispatchedCount}, DeadlineUtc={DeadlineUtc}",
            level,
            agingNumber,
            state.Seq,
            dispatched.Count,
            state.LevelDeadlineUtc?.ToString("O") ?? "<empty>");
    }

    /// <summary>
    /// 等待本层各设备版本号变化。
    /// 完成判据是「本层 dispatched 里的设备在 targets 中全部是 upgraded」，
    /// 而不是「本层的确认记录清空」——后者会被 mesh 层级漂移卡死：
    /// 设备在低层下发后若跑到更高层被重新下发并升级成功，
    /// 低层的确认记录永远不会清，只能靠超时推进。
    /// 比对 targets 的最新状态就没有这个问题。
    /// </summary>
    private async Task VerifyLevelAsync(string agingNumber, OtaBatchState state)
    {
        var level = state.CurrentLevel;
        var dispatched = await _repository.GetDispatchedAsync(agingNumber, state.Seq);

        // 只对本层下发过、但 targets 里还没升级成功的设备比对版本号。
        var targets = await _repository.GetTargetsAsync(agingNumber, state.Seq);
        foreach (var deviceId in dispatched
            .Where(id => !IsUpgraded(targets, id))
            .OrderBy(id => id, StringComparer.Ordinal)
            .ToArray())
        {
            var current = await _devices.GetFirmwareVersionAsync(deviceId);
            var old = await _repository.GetLevelVersionAsync(
                agingNumber,
                state.Seq,
                level,
                deviceId);

            if (!VersionChanged(old, current))
                continue;

            await _repository.SetTargetStatusAsync(
                agingNumber,
                state.Seq,
                deviceId,
                OtaTargetStatuses.Upgraded);

            _logger.LogInformation(
                "OTA 设备固件版本已变化，标记升级完成，AgingNumber={AgingNumber}, Seq={Seq}, Level={Level}, DeviceId={DeviceId}, OldVersion={OldVersion}, NewVersion={NewVersion}",
                agingNumber,
                state.Seq,
                level,
                deviceId,
                string.IsNullOrWhiteSpace(old) ? "<empty>" : old,
                current ?? "<empty>");
        }

        // 上面刚写过状态，这里必须重读一次 targets 再判定。
        targets = await _repository.GetTargetsAsync(agingNumber, state.Seq);
        var unfinished = dispatched
            .Where(id => !IsUpgraded(targets, id))
            .OrderBy(id => id, StringComparer.Ordinal)
            .ToArray();

        if (unfinished.Length == 0)
        {
            await CompleteLevelAsync(agingNumber, state, "本层下发设备均已升级完成");
            return;
        }

        if (!state.LevelDeadlineUtc.HasValue || DateTime.UtcNow < state.LevelDeadlineUtc.Value)
            return;

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

        if (_abortOnLevelTimeout)
        {
            await FinishBatchAsync(agingNumber, state, "某层等待超时，中止整个批次");
            return;
        }

        // 本层没有设备确认升级，mesh 就不会重组，等稳定期是白等，
        // 因此与"空层快跳"一致直接进下一层，不等 LevelStabilizeSeconds。
        // failed 设备若重启后 MeshLevel 上移，会在更高层重新进入下发名单。
        _logger.LogInformation(
            "OTA 第 {Level} 层等待超时，跳过本层直接进入下一层，AgingNumber={AgingNumber}, Seq={Seq}, Failed={FailedCount}",
            level,
            agingNumber,
            state.Seq,
            unfinished.Length);

        await AdvanceLevelAsync(agingNumber, state);
    }

    /// <summary>
    /// 层间 mesh 稳定等待：不再死等固定时长，而是每次轮询读按架在线列表
    /// lgw:ota:{AgingNumber} 的个数，回到首次下发时的水平即认为设备都回来了、
    /// mesh 已重建完成，立即进入下一层；超过 `LevelStabilizeSeconds` 仍未达标
    /// 则按超时直接进入下一层，不再继续等。
    /// </summary>
    private async Task VerifyStabilizeAsync(string agingNumber, OtaBatchState state)
    {
        var current = await _devices.GetOtaRackOnlineCountAsync(agingNumber);
        var expected = state.ExpectedOnlineCount;

        // 用 >= 而不是 ==：等待期间若有新设备上线，个数超过基准同样说明网络已恢复，
        // 不该因为"多出来一台"而卡到超时。
        if (expected.HasValue && current >= expected.Value)
        {
            _logger.LogInformation(
                "OTA mesh 已稳定，在线数回到基准，直接进入下一层，AgingNumber={AgingNumber}, Seq={Seq}, Level={Level}, OnlineCount={OnlineCount}, Expected={Expected}",
                agingNumber,
                state.Seq,
                state.CurrentLevel,
                current,
                expected.Value);

            await AdvanceLevelAsync(agingNumber, state);
            return;
        }

        if (state.StabilizeUntilUtc.HasValue && DateTime.UtcNow < state.StabilizeUntilUtc.Value)
            return;

        _logger.LogWarning(
            "OTA mesh 稳定等待超时（或无基准在线数），直接进入下一层，AgingNumber={AgingNumber}, Seq={Seq}, Level={Level}, OnlineCount={OnlineCount}, Expected={Expected}",
            agingNumber,
            state.Seq,
            state.CurrentLevel,
            current,
            expected?.ToString() ?? "<empty>");

        await AdvanceLevelAsync(agingNumber, state);
    }

    private async Task CompleteLevelAsync(string agingNumber, OtaBatchState state, string reason)
    {
        state.LevelStatus = OtaLevelStatuses.Stabilizing;
        state.LevelStartedUtc = null;
        state.LevelDeadlineUtc = null;
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
        state.LevelStatus = OtaLevelStatuses.Pending;
        state.StabilizeUntilUtc = null;

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
            await FinishBatchAsync(agingNumber, state, "分层下发结束，名单设备均已被下发过");
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

        // 按约定补发后不等设备确认，直接收尾：刚下发的设备仍是 pending，
        // FinishBatchAsync 会把它们统一标成 skipped，补发名单在上面这条日志里可追溯。
        await FinishBatchAsync(agingNumber, state, "分层下发结束，已对未下发设备兜底补发");
    }

    private async Task FinishBatchAsync(string agingNumber, OtaBatchState state, string reason)
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
    /// 单层超时 = BaseTimeoutMinutes + (层-1)×TimeoutIncrementMinutes，
    /// 再按 MaxLevelTimeoutMinutes 封顶（未配置则不封顶）。
    /// </summary>
    private TimeSpan LevelTimeout(int level)
    {
        var timeout = _baseTimeout +
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

    /// <summary>名单里该设备是否已确认升级成功。</summary>
    private static bool IsUpgraded(Dictionary<string, string> targets, string deviceId)
        => targets.TryGetValue(deviceId, out var status) &&
           string.Equals(status, OtaTargetStatuses.Upgraded, StringComparison.Ordinal);

    /// <summary>
    /// 旧版本为空（设备从未上报过 Version）时，以"升级后版本号变为非空"判定成功。
    /// </summary>
    private static bool VersionChanged(string? oldVersion, string? currentVersion)
    {
        if (string.IsNullOrWhiteSpace(oldVersion))
            return !string.IsNullOrWhiteSpace(currentVersion);

        return !string.Equals(oldVersion, currentVersion, StringComparison.Ordinal);
    }

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
