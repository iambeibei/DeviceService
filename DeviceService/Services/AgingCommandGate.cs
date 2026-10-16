using Bluetti.Device.Redis;
using System.Collections.Concurrent;

namespace DeviceService.Services;

/// <summary>
/// 仅用于控制 start_aging 的发送节奏。
///
/// 规则：
/// 1. start_aging 真正准备发送前先占用设备的老化发送门；
/// 2. agingStart 表示设备正在老化，发送门保持关闭；
/// 3. agingStop / agingError / agingIdle 表示设备不再处于老化中，释放发送门；
/// 4. 其他普通命令不经过该发送门，不会被长时间老化任务阻塞；
/// 5. 服务启动后首次使用某设备时，会从 Redis runtime.AgingState 与 CurrentAgingSeq 恢复当前状态；
///    即使 start_aging 已回复超时，只要 CurrentAgingSeq 仍存在，也继续保持发送门关闭。
/// </summary>
public sealed class AgingCommandGate
{
    private readonly DeviceRepository _devices;
    private readonly ILogger<AgingCommandGate> _logger;

    // 兜底：设备掉线后不再上报终态，等待超过该时长且设备已离线时强制释放发送门，
    // 并允许一条 start_aging 穿透 agingStart；AgingState 本身保持不变。
    private readonly bool _offlineReleaseEnabled;
    private readonly TimeSpan _offlineReleaseTimeout;

    private readonly ConcurrentDictionary<string, DeviceGate> _gates =
        new(StringComparer.Ordinal);

    public AgingCommandGate(
        DeviceRepository devices,
        IConfiguration configuration,
        ILogger<AgingCommandGate> logger)
    {
        _devices = devices;
        _logger = logger;

        var offlineReleaseMinutes = configuration.GetValue<double?>(
            "AgingCommandGate:OfflineReleaseTimeoutMinutes") ?? 30;

        _offlineReleaseEnabled = offlineReleaseMinutes > 0;
        _offlineReleaseTimeout = _offlineReleaseEnabled
            ? TimeSpan.FromMinutes(offlineReleaseMinutes)
            : TimeSpan.Zero;
    }

    /// <summary>
    /// 等待该设备允许发送新的 start_aging，并在返回前原子地占用发送门。
    /// 占用发生在 MQTT Publish 之前，可避免上一条 start_aging 的回复先到、agingStart 后到时
    /// 下一条命令从时间窗口中提前穿透。
    /// </summary>
    public async Task WaitAndReserveAsync(string deviceId, CancellationToken cancellationToken)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        var gate = _gates.GetOrAdd(deviceId, _ => new DeviceGate());

        await EnsureInitializedAsync(deviceId, gate, cancellationToken);

        while (true)
        {
            Task stateChangedTask;
            string? state;
            bool reserved;

            lock (gate.SyncRoot)
            {
                state = gate.AgingState;
                reserved = gate.Reserved;

                // 兜底释放后允许本轮穿透一次 agingStart。
                // 只穿透一条：占用门的同时清掉标记，排队的后续命令继续按真实状态排队。
                var forceReleased = gate.ForceReleased;
                if (!reserved && (!IsAgingStart(state) || forceReleased))
                {
                    gate.Reserved = true;
                    gate.ForceReleased = false;
                    _logger.LogInformation(
                        "start_aging 已取得发送资格，DeviceId={DeviceId}, AgingState={AgingState}, ForceReleased={ForceReleased}",
                        deviceId,
                        state ?? "<empty>",
                        forceReleased);
                    return;
                }

                stateChangedTask = gate.StateChanged.Task;
            }

            _logger.LogInformation(
                "start_aging 暂存等待，DeviceId={DeviceId}, AgingState={AgingState}, Reserved={Reserved}",
                deviceId,
                state ?? "<empty>",
                reserved);

            if (!_offlineReleaseEnabled)
            {
                await stateChangedTask.WaitAsync(cancellationToken);
                continue;
            }

            // 同时等待状态变化与兜底超时：设备掉线后不会再有终态事件，
            // 只等事件会让该设备的 start_aging 永久排队。
            await Task.WhenAny(
                stateChangedTask,
                Task.Delay(_offlineReleaseTimeout, cancellationToken));
            cancellationToken.ThrowIfCancellationRequested();

            if (stateChangedTask.IsCompleted)
                continue;

            // 超时仍未等到终态：只有设备确已离线才强制释放，
            // 设备在线时老化可能仍在进行，继续等真实终态，不能强行插队。
            await TryForceReleaseOfflineAsync(deviceId, gate);
        }
    }

    /// <summary>
    /// 兜底释放：确认设备已离线后强制打开发送门。
    /// 返回 true 表示已释放；false 表示设备仍在线，调用方应继续等待。
    /// </summary>
    private async Task<bool> TryForceReleaseOfflineAsync(string deviceId, DeviceGate gate)
    {
        // 在线状态以全量在线 List 为准：runtime.Online 会被任意业务消息刷新成 true，
        // 用它判断会把已经离线的设备误判成在线，发送门继续无限等待。
        if (await _devices.IsDeviceOnlineAsync(deviceId))
            return false;

        lock (gate.SyncRoot)
        {
            gate.Reserved = false;

            // 仅清 Reserved 不足以放行：放行条件还要求 AgingState 不是 agingStart，
            // 而掉线设备不会再上报终态，内存里会一直是 agingStart。
            // 这里打一个"本轮豁免"标记让一条命令穿透，AgingState 本身保持不变，
            // 避免影响 RedisCmdWorker 依据该状态判断"是否重发已发布的老化命令"。
            gate.ForceReleased = true;
            Pulse(gate);
        }

        _logger.LogWarning(
            "start_aging 发送门等待超时且设备已离线，强制释放发送门并允许本轮穿透，DeviceId={DeviceId}, TimeoutMinutes={TimeoutMinutes}, AgingState={AgingState}",
            deviceId,
            _offlineReleaseTimeout.TotalMinutes,
            gate.AgingState ?? "<empty>");

        return true;
    }

    /// <summary>
    /// 设备 state 事件到达时调用。
    /// agingStart 保持发送门关闭；agingStop / agingError / agingIdle 释放发送门并唤醒下一条 start_aging。
    /// </summary>
    public void NotifyAgingState(string deviceId, string? agingState)
    {
        if (string.IsNullOrWhiteSpace(deviceId) || string.IsNullOrWhiteSpace(agingState))
            return;

        deviceId = RedisKeys.Normalize(deviceId);
        var normalizedState = NormalizeState(agingState);
        var gate = _gates.GetOrAdd(deviceId, _ => new DeviceGate());

        lock (gate.SyncRoot)
        {
            gate.Initialized = true;
            gate.AgingState = agingState.Trim();

            // 真实状态到达后一律以它为准，之前的兜底豁免立即作废。
            // 设备重新上线若仍上报 agingStart，门会再次关闭，不会打断正在进行的老化。
            gate.ForceReleased = false;

            if (normalizedState == "agingstart")
            {
                // 明确处于老化中：无论此前内存状态如何，都保持占用。
                gate.Reserved = true;
            }
            else if (normalizedState is "agingstop" or "agingerror" or "agingidle")
            {
                gate.Reserved = false;
            }
            else
            {
                _logger.LogWarning(
                    "收到未识别的老化状态，不改变 start_aging 发送门，DeviceId={DeviceId}, AgingState={AgingState}",
                    deviceId,
                    agingState);
            }

            Pulse(gate);
        }

        _logger.LogInformation(
            "更新老化状态门，DeviceId={DeviceId}, AgingState={AgingState}, StartAgingAllowed={Allowed}",
            deviceId,
            agingState,
            normalizedState is "agingstop" or "agingerror" or "agingidle");
    }

    /// <summary>
    /// start_aging 收到下位机“明确失败回复”时调用。
    /// 回复超时不能调用本方法，因为超时只能说明上位机未收到回复，不能证明 ESP32 未执行。
    /// 如果设备此时已经明确上报 agingStart，则继续保持发送门关闭；否则释放。
    /// </summary>
    public async Task ReleaseAfterCommandFailureAsync(
        string deviceId,
        CancellationToken cancellationToken)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        var gate = _gates.GetOrAdd(deviceId, _ => new DeviceGate());
        await EnsureInitializedAsync(deviceId, gate, cancellationToken);

        lock (gate.SyncRoot)
        {
            if (!IsAgingStart(gate.AgingState))
            {
                gate.Reserved = false;
                gate.ForceReleased = false;
                Pulse(gate);
            }
        }
    }

    /// <summary>
    /// 返回当前已知的 AgingState。首次调用会从 Redis runtime 恢复。
    /// </summary>
    public async Task<string?> GetAgingStateAsync(
        string deviceId,
        CancellationToken cancellationToken)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        var gate = _gates.GetOrAdd(deviceId, _ => new DeviceGate());
        await EnsureInitializedAsync(deviceId, gate, cancellationToken);

        lock (gate.SyncRoot)
            return gate.AgingState;
    }

    private async Task EnsureInitializedAsync(
        string deviceId,
        DeviceGate gate,
        CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref gate.Initialized))
            return;

        await gate.InitializeLock.WaitAsync(cancellationToken);
        try
        {
            if (gate.Initialized)
                return;

            var state = await _devices.GetAgingStateAsync(deviceId);
            var currentAgingSeq = await _devices.GetCurrentAgingTaskIdAsync(deviceId);
            var shouldReserve = IsAgingStart(state) || currentAgingSeq.HasValue;

            lock (gate.SyncRoot)
            {
                // 初始化读取 Redis 的过程中，MQTT state 事件可能已经先一步更新了 gate。
                // 此时必须保留更新后的实时状态，不能再用初始化前读取到的旧 Redis 值覆盖。
                if (gate.Initialized)
                    return;

                gate.AgingState = state;
                // CurrentAgingSeq 是 start_aging 已经真正 Publish 的持久化证据。
                // 即使此时 AgingState 还是 agingIdle（例如 cmd/reply 超时且状态上报也延迟），
                // 服务重启后也必须继续锁住下一条 start_aging，直到收到明确退出状态。
                gate.Reserved = shouldReserve;
                Volatile.Write(ref gate.Initialized, true);
                Pulse(gate);
            }

            _logger.LogInformation(
                "从 Redis 恢复老化发送门，DeviceId={DeviceId}, AgingState={AgingState}, CurrentAgingSeq={CurrentAgingSeq}, Reserved={Reserved}",
                deviceId,
                state ?? "<empty>",
                currentAgingSeq?.ToString() ?? "<empty>",
                shouldReserve);
        }
        finally
        {
            gate.InitializeLock.Release();
        }
    }

    private static bool IsAgingStart(string? state)
        => NormalizeState(state) == "agingstart";

    private static string NormalizeState(string? state)
        => state?.Trim().ToLowerInvariant() ?? string.Empty;

    private static void Pulse(DeviceGate gate)
    {
        var previous = gate.StateChanged;
        gate.StateChanged = CreateSignal();
        previous.TrySetResult(true);
    }

    private static TaskCompletionSource<bool> CreateSignal()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class DeviceGate
    {
        public object SyncRoot { get; } = new();
        public SemaphoreSlim InitializeLock { get; } = new(1, 1);
        public bool Initialized;
        public bool Reserved;

        /// <summary>
        /// 离线兜底释放后允许一条 start_aging 穿透 agingStart 状态。
        /// 只作用于一次放行，占用发送门时立即清除；真实状态上报也会清除。
        /// </summary>
        public bool ForceReleased;

        public string? AgingState;
        public TaskCompletionSource<bool> StateChanged = CreateSignal();
    }
}
