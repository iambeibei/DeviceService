using Bluetti.Device.Redis;
using Bluetti.Device.Redis.Models;
using DeviceService.Mqtt;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;

namespace DeviceService.Services;

/// <summary>
/// OTA 批量升级协调服务。
///
/// 一条 ota 指令对应一个批次（若指令跨多个老化架则按架拆成多个批次，各自计数、各自广播）。
/// 逐台下发后等待各设备上报下载完成（device/{id}/event/OTACP），
/// 全部完成或超过时限后按架广播 server/rack/{AgingNumber}/CanRestart，
/// 保证同架设备不会各自提前重启，避免升级不完整。
///
/// 批次号直接复用 ota 指令 Seq；设备与批次的归属由服务端按 deviceId 记录，
/// 因为 OTACP 上报携带的是设备自身消息序号，与 ota 指令 Seq 无关，无法用它反查批次。
/// </summary>
public sealed class OtaUpgradeService : BackgroundService
{
    private readonly OtaUpgradeRepository _repository;
    private readonly DeviceRepository _devices;
    private readonly MqttConnection _mqtt;
    private readonly RedisHelper _redis;
    private readonly ILogger<OtaUpgradeService> _logger;

    private readonly bool _enabled;
    private readonly TimeSpan _completionTimeout;
    private readonly TimeSpan _pollInterval;
    private readonly bool _raiseFaultOnIncomplete;
    private readonly bool _rejectConcurrentBatch;

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

        var timeoutMinutes = configuration.GetValue<double?>(
            "OtaUpgrade:CompletionTimeoutMinutes") ?? 30;
        if (timeoutMinutes <= 0)
            throw new InvalidOperationException(
                "OtaUpgrade:CompletionTimeoutMinutes 必须大于 0");
        _completionTimeout = TimeSpan.FromMinutes(timeoutMinutes);

        var pollMilliseconds = configuration.GetValue<int?>(
            "OtaUpgrade:PollIntervalMilliseconds") ?? 1000;
        if (pollMilliseconds <= 0)
            throw new InvalidOperationException(
                "OtaUpgrade:PollIntervalMilliseconds 必须大于 0");
        _pollInterval = TimeSpan.FromMilliseconds(pollMilliseconds);

        _raiseFaultOnIncomplete = configuration.GetValue<bool?>(
            "OtaUpgrade:RaiseFaultOnIncomplete") ?? true;

        _rejectConcurrentBatch = configuration.GetValue<bool?>(
            "OtaUpgrade:RejectConcurrentBatchPerRack") ?? true;
    }

    /// <summary>
    /// 下发前的准入判定：确定设备所属老化架，并拒绝与既有批次冲突的指令。
    /// 返回 Allowed=false 时调用方不应下发该设备的 ota 指令。
    /// </summary>
    public async Task<DispatchDecision> BeginDispatchAsync(string deviceId, long seq)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        if (!_enabled)
            return DispatchDecision.Accept(string.Empty);

        var agingNumber = await ResolveAgingNumberAsync(deviceId);
        if (agingNumber is null)
        {
            return new DispatchDecision(
                false,
                string.Empty,
                "设备缺少有效老化架号，无法确定 OTA 批次归属");
        }

        var rackLock = GetRackLock(agingNumber);
        await rackLock.WaitAsync();
        try
        {
            if (_rejectConcurrentBatch)
            {
                var active = await _repository.GetActiveBatchSeqAsync(agingNumber);
                if (active.HasValue && active.Value != seq)
                {
                    return new DispatchDecision(
                        false,
                        agingNumber,
                        $"老化架已有未完成的 OTA 批次 Seq={active.Value}");
                }
            }

            await _repository.RegisterRackAsync(agingNumber);
            return DispatchDecision.Accept(agingNumber);
        }
        finally
        {
            rackLock.Release();
        }
    }

    /// <summary>
    /// ota 指令 MQTT 发布成功后调用：把设备计入本批"已下发"，并启动批次倒计时。
    /// </summary>
    public async Task RecordDispatchAsync(string deviceId, long seq)
    {
        if (!_enabled)
            return;

        deviceId = RedisKeys.Normalize(deviceId);
        var agingNumber = await ResolveAgingNumberAsync(deviceId);
        if (agingNumber is null)
        {
            _logger.LogWarning(
                "ota 指令已发布但设备缺少架号，不计入 OTA 批次，DeviceId={DeviceId}, Seq={Seq}",
                deviceId,
                seq);
            return;
        }

        var rackLock = GetRackLock(agingNumber);
        await rackLock.WaitAsync();
        try
        {
            await _repository.RegisterRackAsync(agingNumber);

            // 并发保护关闭时可能已有别的活跃批次，此时只保证本批自己的状态，不抢占 active。
            var active = await _repository.GetActiveBatchSeqAsync(agingNumber);
            if (!active.HasValue)
                await _repository.SetActiveBatchAsync(agingNumber, seq);

            var state = await _repository.GetBatchStateAsync(agingNumber, seq)
                ?? new OtaBatchState { Seq = seq };

            // 同一 Seq 的指令被重放时不再重开批次，否则会重复广播重启指令。
            if (string.Equals(state.Status, OtaBatchStatuses.Finished, StringComparison.Ordinal))
            {
                _logger.LogWarning(
                    "OTA 批次已结束，忽略重复指令的下发计数，AgingNumber={AgingNumber}, Seq={Seq}, DeviceId={DeviceId}",
                    agingNumber,
                    seq,
                    deviceId);
                return;
            }

            var now = DateTime.UtcNow;
            state.FirstDispatchUtc ??= now;
            state.DeadlineUtc = state.FirstDispatchUtc.Value + _completionTimeout;
            state.Status = OtaBatchStatuses.Active;

            await _repository.AddDispatchedAsync(agingNumber, seq, deviceId);
            await _repository.SaveBatchStateAsync(agingNumber, state);

            var dispatched = await _repository.GetDispatchedAsync(agingNumber, seq);
            _logger.LogInformation(
                "OTA 指令已下发，AgingNumber={AgingNumber}, Seq={Seq}, DeviceId={DeviceId}, Dispatched={DispatchedCount}, DeadlineUtc={DeadlineUtc}",
                agingNumber,
                seq,
                deviceId,
                dispatched.Count,
                state.DeadlineUtc?.ToString("O") ?? "<empty>");

            await EvaluateBatchUnsafeAsync(agingNumber, state, CancellationToken.None);
        }
        finally
        {
            rackLock.Release();
        }
    }

    /// <summary>
    /// 设备上报 OTA 下载完成（device/{id}/event/OTACP）。
    /// 批次归属一律按服务端记录的 deviceId 判定，上报里的 AgingNumber 只做一致性校验。
    /// </summary>
    public async Task RecordCompletionAsync(
        string deviceId,
        bool isComplete,
        string? reportedAgingNumber)
    {
        if (!_enabled)
            return;

        deviceId = RedisKeys.Normalize(deviceId);
        var agingNumber = await ResolveAgingNumberAsync(deviceId);
        if (agingNumber is null)
        {
            _logger.LogWarning(
                "OTACP 上报设备缺少架号，无法归属 OTA 批次，DeviceId={DeviceId}",
                deviceId);
            return;
        }

        if (!string.IsNullOrWhiteSpace(reportedAgingNumber))
        {
            var normalized = TryNormalizeAgingNumber(reportedAgingNumber);
            if (normalized is not null &&
                !string.Equals(normalized, agingNumber, StringComparison.Ordinal))
            {
                _logger.LogWarning(
                    "OTACP 上报架号与设备归属架号不一致，以服务端归属为准，DeviceId={DeviceId}, Reported={Reported}, Resolved={Resolved}",
                    deviceId,
                    reportedAgingNumber,
                    agingNumber);
            }
        }

        var rackLock = GetRackLock(agingNumber);
        await rackLock.WaitAsync();
        try
        {
            var seq = await _repository.GetActiveBatchSeqAsync(agingNumber);
            if (!seq.HasValue)
            {
                _logger.LogWarning(
                    "OTACP 上报找不到该架的活跃 OTA 批次，AgingNumber={AgingNumber}, DeviceId={DeviceId}",
                    agingNumber,
                    deviceId);
                return;
            }

            var state = await _repository.GetBatchStateAsync(agingNumber, seq.Value);
            if (state is null)
            {
                _logger.LogWarning(
                    "OTACP 上报对应的 OTA 批次状态不存在，AgingNumber={AgingNumber}, Seq={Seq}, DeviceId={DeviceId}",
                    agingNumber,
                    seq.Value,
                    deviceId);
                return;
            }

            if (string.Equals(state.Status, OtaBatchStatuses.Finished, StringComparison.Ordinal))
            {
                _logger.LogInformation(
                    "OTA 批次已广播重启，忽略迟到的 OTACP 上报，AgingNumber={AgingNumber}, Seq={Seq}, DeviceId={DeviceId}",
                    agingNumber,
                    seq.Value,
                    deviceId);
                return;
            }

            var dispatched = await _repository.GetDispatchedAsync(agingNumber, seq.Value);
            if (!dispatched.Contains(deviceId))
            {
                _logger.LogWarning(
                    "OTACP 上报设备不在本批次下发集合内，忽略计数，AgingNumber={AgingNumber}, Seq={Seq}, DeviceId={DeviceId}",
                    agingNumber,
                    seq.Value,
                    deviceId);
                return;
            }

            if (!isComplete)
            {
                var failureMessage =
                    $"设备上报 OTA 未完成，AgingNumber={agingNumber}, Seq={seq.Value}";
                _logger.LogWarning(
                    "OTACP 上报 IsComplete 不为 1，不计入完成数，AgingNumber={AgingNumber}, Seq={Seq}, DeviceId={DeviceId}",
                    agingNumber,
                    seq.Value,
                    deviceId);

                if (_raiseFaultOnIncomplete)
                    await SaveFaultAsync(
                        agingNumber,
                        deviceId,
                        seq.Value,
                        "ota_incomplete",
                        failureMessage);

                return;
            }

            await _repository.AddCompletedAsync(agingNumber, seq.Value, deviceId);

            var completed = await _repository.GetCompletedAsync(agingNumber, seq.Value);
            _logger.LogInformation(
                "OTA 下载完成上报已记录，AgingNumber={AgingNumber}, Seq={Seq}, DeviceId={DeviceId}, Completed={CompletedCount}, Dispatched={DispatchedCount}",
                agingNumber,
                seq.Value,
                deviceId,
                completed.Count,
                dispatched.Count);

            await EvaluateBatchUnsafeAsync(agingNumber, state, CancellationToken.None);
        }
        finally
        {
            rackLock.Release();
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "OTA 批量升级协调服务已启动，Enabled={Enabled}, CompletionTimeoutMinutes={CompletionTimeoutMinutes}, PollIntervalMs={PollIntervalMs}, RejectConcurrentBatch={RejectConcurrentBatch}",
            _enabled,
            _completionTimeout.TotalMinutes,
            _pollInterval.TotalMilliseconds,
            _rejectConcurrentBatch);

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
                _logger.LogError(ex, "OTA 批次超时扫描或重启指令重试失败");
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

    /// <summary>
    /// 单个老化架的周期处理：优先补发上次未成功的重启指令，其次处理批次超时。
    /// </summary>
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

            if (!string.IsNullOrWhiteSpace(state.PendingRestart))
            {
                await TryPublishRestartUnsafeAsync(
                    agingNumber,
                    state,
                    "重试此前未发布成功的重启指令",
                    cancellationToken);
                return;
            }

            if (state.DeadlineUtc.HasValue && DateTime.UtcNow >= state.DeadlineUtc.Value)
            {
                await RaiseIncompleteFaultsAsync(agingNumber, state);
                await TryPublishRestartUnsafeAsync(
                    agingNumber,
                    state,
                    "批次等待超时，按兜底策略放行重启",
                    cancellationToken);
            }
        }
        finally
        {
            rackLock.Release();
        }
    }

    /// <summary>
    /// 完成数覆盖下发数时立即广播重启。调用方必须持有对应老化架锁。
    /// </summary>
    private async Task EvaluateBatchUnsafeAsync(
        string agingNumber,
        OtaBatchState state,
        CancellationToken cancellationToken)
    {
        if (string.Equals(state.Status, OtaBatchStatuses.Finished, StringComparison.Ordinal))
            return;

        var dispatched = await _repository.GetDispatchedAsync(agingNumber, state.Seq);
        var completed = await _repository.GetCompletedAsync(agingNumber, state.Seq);

        if (dispatched.Count == 0 || !dispatched.All(completed.Contains))
            return;

        await TryPublishRestartUnsafeAsync(
            agingNumber,
            state,
            "本批次所有已下发设备均已上报下载完成",
            cancellationToken);
    }

    private async Task TryPublishRestartUnsafeAsync(
        string agingNumber,
        OtaBatchState state,
        string reason,
        CancellationToken cancellationToken)
    {
        var pending = GroupControlRepository.DeserializePending(state.PendingRestart);
        if (pending is null)
        {
            var restartSeq = await _repository.NextSequenceAsync();
            pending = new PendingGroupControlCommand
            {
                Seq = restartSeq,
                Payload = CreateRestartPayload(restartSeq)
            };
            state.PendingRestart = GroupControlRepository.SerializePending(pending);
            await _repository.SaveBatchStateAsync(agingNumber, state);

            _logger.LogInformation(
                "OTA 批次已创建重启指令，AgingNumber={AgingNumber}, Seq={Seq}, RestartSeq={RestartSeq}, Reason={Reason}",
                agingNumber,
                state.Seq,
                pending.Seq,
                reason);
        }

        var topic = BuildRestartTopic(agingNumber);
        try
        {
            await _mqtt.PublishAsync(topic, pending.Payload, cancellationToken);

            state.Status = OtaBatchStatuses.Finished;
            state.RestartedAtUtc = DateTime.UtcNow;
            state.PendingRestart = string.Empty;
            await _repository.SaveBatchStateAsync(agingNumber, state);
            await _repository.ClearActiveBatchAsync(agingNumber);

            _logger.LogWarning(
                "OTA 批次已按老化架主题广播重启指令，AgingNumber={AgingNumber}, Seq={Seq}, Topic={Topic}, RestartSeq={RestartSeq}, Reason={Reason}",
                agingNumber,
                state.Seq,
                topic,
                pending.Seq,
                reason);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "OTA 重启指令发布失败，将保留原 Seq 重试，AgingNumber={AgingNumber}, Seq={Seq}, RestartSeq={RestartSeq}",
                agingNumber,
                state.Seq,
                pending.Seq);
        }
    }

    /// <summary>
    /// 超时放行时给"已下发但未上报完成"的设备留痕。
    /// 这些设备会被整架广播一并重启，必须能事后追溯。
    /// </summary>
    private async Task RaiseIncompleteFaultsAsync(string agingNumber, OtaBatchState state)
    {
        var dispatched = await _repository.GetDispatchedAsync(agingNumber, state.Seq);
        var completed = await _repository.GetCompletedAsync(agingNumber, state.Seq);
        var missing = dispatched
            .Where(deviceId => !completed.Contains(deviceId))
            .OrderBy(deviceId => deviceId, StringComparer.Ordinal)
            .ToArray();

        if (missing.Length == 0)
            return;

        var message =
            $"OTA 批次等待超时，设备未完成下载即被放行重启，AgingNumber={agingNumber}, Seq={state.Seq}";

        foreach (var deviceId in missing)
        {
            _logger.LogWarning(
                "OTA 批次超时仍未上报下载完成，AgingNumber={AgingNumber}, Seq={Seq}, DeviceId={DeviceId}",
                agingNumber,
                state.Seq,
                deviceId);

            if (_raiseFaultOnIncomplete)
                await SaveFaultAsync(
                    agingNumber,
                    deviceId,
                    state.Seq,
                    "ota_timeout",
                    message);
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
            ["RunSeq"] = seq,
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

    private async Task<string?> ResolveAgingNumberAsync(string deviceId)
    {
        var device = await _devices.GetAsync(deviceId);
        if (string.IsNullOrWhiteSpace(device?.AgingNumber))
            return null;

        return TryNormalizeAgingNumber(device.AgingNumber);
    }

    private static string? TryNormalizeAgingNumber(string agingNumber)
    {
        try
        {
            return RedisKeys.NormalizeAgingNumber(agingNumber);
        }
        catch (ArgumentException)
        {
            return null;
        }
    }

    private SemaphoreSlim GetRackLock(string agingNumber)
        => _rackLocks.GetOrAdd(agingNumber, _ => new SemaphoreSlim(1, 1));

    /// <summary>
    /// 重启主题按老化架隔离：server/rack/{AgingNumber}/CanRestart。
    /// 架号必须先规范化再拼进主题，避免出现空段或非法字符破坏订阅匹配。
    /// 大小写与设备端订阅串强绑定，改动必须同步固件。
    /// </summary>
    private static string BuildRestartTopic(string agingNumber)
        => $"server/rack/{RedisKeys.NormalizeAgingNumber(agingNumber)}/CanRestart";

    private static string CreateRestartPayload(long seq)
        => new JObject
        {
            ["Seq"] = seq,
            ["Data"] = new JObject { ["Restart"] = 1 }
        }.ToString(Formatting.None);

    public readonly record struct DispatchDecision(bool Allowed, string AgingNumber, string Reason)
    {
        public static DispatchDecision Accept(string agingNumber)
            => new(true, agingNumber, string.Empty);
    }
}
