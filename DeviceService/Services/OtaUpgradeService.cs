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
    private readonly TimeSpan _pollInterval;
    private readonly TimeSpan _levelStabilize;
    private readonly int _maxMeshLevel;
    private readonly bool _rejectConcurrentBatch;
    private readonly bool _raiseFaultOnIncomplete;
    private readonly bool _abortOnLevelTimeout;
    private readonly bool _abortOnFirstLevelFailure;

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

        // 根节点（MeshLevel 1）是整条 mesh 的入口：它没起来，后面各层即使下发也收不到包。
        // 因此首层任一节点升级失败就直接结束整批，不再逐层空等超时。
        _abortOnFirstLevelFailure = configuration.GetValue<bool?>(
            "OtaUpgrade:AbortOnFirstLevelFailure") ?? true;
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
            "OTA 分层升级协调服务已启动，Enabled={Enabled}, BaseTimeoutMinutes={BaseTimeoutMinutes}, TimeoutIncrementMinutes={TimeoutIncrementMinutes}, LevelStabilizeSeconds={LevelStabilizeSeconds}, MaxMeshLevel={MaxMeshLevel}, AbortOnLevelTimeout={AbortOnLevelTimeout}, AbortOnFirstLevelFailure={AbortOnFirstLevelFailure}",
            _enabled,
            _baseTimeout.TotalMinutes,
            _timeoutIncrement.TotalMinutes,
            _levelStabilize.TotalSeconds,
            _maxMeshLevel,
            _abortOnLevelTimeout,
            _abortOnFirstLevelFailure);

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

            // 层间 mesh 稳定等待：上一层重启会让下层设备短暂掉线，
            // 等设备重新回到分层清单后再取下一层交集，否则会被误判成不在名单里。
            if (state.StabilizeUntilUtc.HasValue &&
                DateTime.UtcNow < state.StabilizeUntilUtc.Value)
            {
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
            await FinishBatchAsync(agingNumber, state, "已达到 Mesh 层级上限");
            return;
        }

        var level = state.CurrentLevel;
        var levelDevices = await _repository.GetMeshLevelDeviceIdsAsync(agingNumber, level);
        var targets = await _repository.GetTargetsAsync(agingNumber, state.Seq);
        var candidates = targets
            .Where(item => IsDispatchable(item.Value))
            .Select(item => item.Key)
            .Where(levelDevices.Contains)
            .OrderBy(deviceId => deviceId, StringComparer.Ordinal)
            .ToArray();

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

                await _repository.AddLevelPendingAsync(agingNumber, state.Seq, level, deviceId);
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
    /// 等待本层各设备版本号变化。全部确认即本层完成；超时按配置跳过或中止。
    /// </summary>
    private async Task VerifyLevelAsync(string agingNumber, OtaBatchState state)
    {
        var level = state.CurrentLevel;
        var pending = await _repository.GetLevelPendingAsync(agingNumber, state.Seq, level);

        foreach (var deviceId in pending.OrderBy(x => x, StringComparer.Ordinal).ToArray())
        {
            var current = await _devices.GetFirmwareVersionAsync(deviceId);
            var old = await _repository.GetLevelVersionAsync(
                agingNumber,
                state.Seq,
                level,
                deviceId);

            if (!VersionChanged(old, current))
                continue;

            await _repository.RemoveLevelPendingAsync(agingNumber, state.Seq, level, deviceId);
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

        pending = await _repository.GetLevelPendingAsync(agingNumber, state.Seq, level);
        if (pending.Count == 0)
        {
            await CompleteLevelAsync(agingNumber, state, "本层设备均已确认升级");
            return;
        }

        if (!state.LevelDeadlineUtc.HasValue || DateTime.UtcNow < state.LevelDeadlineUtc.Value)
            return;

        var message =
            $"OTA 第 {level} 层等待超时，设备未完成升级，AgingNumber={agingNumber}, Seq={state.Seq}";

        foreach (var deviceId in pending.OrderBy(x => x, StringComparer.Ordinal).ToArray())
        {
            _logger.LogWarning(
                "OTA 设备未在时限内完成升级，AgingNumber={AgingNumber}, Seq={Seq}, Level={Level}, DeviceId={DeviceId}",
                agingNumber,
                state.Seq,
                level,
                deviceId);

            await _repository.SetTargetStatusAsync(
                agingNumber,
                state.Seq,
                deviceId,
                OtaTargetStatuses.Failed);

            if (_raiseFaultOnIncomplete)
                await SaveFaultAsync(agingNumber, deviceId, state.Seq, "ota_level_timeout", message);
        }

        await _repository.ClearLevelPendingAsync(agingNumber, state.Seq, level);

        // 首层门禁：第 1 层（根节点）没升起来，后续层再下发也没意义，直接收尾整批。
        // 只认"已下发但未确认"的超时失败；空层快跳（candidates 为 0）走的是
        // StartLevelAsync → AdvanceLevelAsync，不经过这里，不会被误判成失败。
        if (_abortOnFirstLevelFailure && level == 1)
        {
            _logger.LogWarning(
                "OTA 首层节点升级失败，中止整个批次，后续层不再下发，AgingNumber={AgingNumber}, Seq={Seq}, Failed={FailedCount}",
                agingNumber,
                state.Seq,
                pending.Count);

            await FinishBatchAsync(agingNumber, state, "首层节点升级失败，中止整批");
            return;
        }

        if (_abortOnLevelTimeout)
        {
            await FinishBatchAsync(agingNumber, state, "某层等待超时，中止整个批次");
            return;
        }

        await CompleteLevelAsync(agingNumber, state, "本层等待超时，跳过并继续下一层");
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
            await _repository.SaveBatchStateAsync(agingNumber, state);
            await FinishBatchAsync(agingNumber, state, "已达到 Mesh 层级上限");
            return;
        }

        await _repository.SaveBatchStateAsync(agingNumber, state);
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

    private TimeSpan LevelTimeout(int level)
        => _baseTimeout + TimeSpan.FromTicks(_timeoutIncrement.Ticks * Math.Max(0, level - 1));

    /// <summary>
    /// 是否还能下发。pending 是尚未下发过的，skipped 是曾因离线或发布失败而未下发、
    /// 允许在后续层重试的；upgraded 已成功、failed 已下发但未确认，都不再重复下发。
    /// </summary>
    private static bool IsDispatchable(string status)
        => string.Equals(status, OtaTargetStatuses.Pending, StringComparison.Ordinal) ||
           string.Equals(status, OtaTargetStatuses.Skipped, StringComparison.Ordinal);

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
