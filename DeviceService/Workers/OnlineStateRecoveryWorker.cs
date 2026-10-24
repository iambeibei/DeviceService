using Bluetti.Device.Redis;
using Bluetti.Device.Redis.Models;
using DeviceService.Mqtt;
using DeviceService.Services;
using Newtonsoft.Json.Linq;

namespace DeviceService.Workers;

/// <summary>
/// 启动 / MQTT 重连后的在线状态对账。
///
/// 解决的问题：上行订阅是 QoS 0，服务停机期间设备发的 up_line / down_line
/// **一定全部丢失**，且 Broker 不会补投。于是重启后在线名单会永久失真：
/// ① 停机期间上线过的设备不在名单里（除非它再上下线一次）；
/// ② 停机期间离线了的设备仍留在名单里变成僵尸条目，群控
///    GetUniqueControllerId 可能把僵尸总控算进去，判定"多个总控"而拒绝下发 CPower。
///
/// 做法：取「全量设备目录 lgw:devices ∪ 在线名单」的并集逐个探测——
/// 只有并集才能同时覆盖上述两类。探测用 get_state 命令并等其回复，
/// 这是唯一的可靠判据（见 ProbeAsync 注释为何不能用 LastSeenUtc）。
///
/// DI 约束：禁止注入 RedisCmdWorker / GroupControlService / OtaUpgradeService
/// （三者构造函数都吃 MqttConnection，会形成 DI 环，容器直接抛异常）。
/// 离线动作一律走 DeviceLifecycleService。
/// </summary>
public sealed class OnlineStateRecoveryWorker : BackgroundService
{
    private readonly DeviceRepository _devices;
    private readonly CommandRepository _commands;
    private readonly CmdHelper _cmd;
    private readonly MqttConnection _mqtt;
    private readonly DeviceLifecycleService _lifecycle;
    private readonly ILogger<OnlineStateRecoveryWorker> _logger;

    private readonly bool _enabled;
    private readonly bool _reconnectSweepEnabled;
    private readonly TimeSpan _startupDelay;
    private readonly TimeSpan _probeTimeout;
    private readonly TimeSpan _staleThreshold;
    private readonly TimeSpan _sweepInterval;
    private readonly int _maxConcurrency;
    private readonly string _probeAction;

    public OnlineStateRecoveryWorker(
        DeviceRepository devices,
        CommandRepository commands,
        CmdHelper cmd,
        MqttConnection mqtt,
        DeviceLifecycleService lifecycle,
        IConfiguration configuration,
        ILogger<OnlineStateRecoveryWorker> logger)
    {
        _devices = devices;
        _commands = commands;
        _cmd = cmd;
        _mqtt = mqtt;
        _lifecycle = lifecycle;
        _logger = logger;

        _enabled = configuration.GetValue<bool?>("OnlineStateRecovery:Enabled") ?? true;
        _reconnectSweepEnabled =
            configuration.GetValue<bool?>("OnlineStateRecovery:ReconnectSweepEnabled") ?? true;

        var startupDelaySeconds =
            configuration.GetValue<double?>("OnlineStateRecovery:StartupDelaySeconds") ?? 30;
        _startupDelay = startupDelaySeconds > 0
            ? TimeSpan.FromSeconds(startupDelaySeconds)
            : TimeSpan.Zero;

        // 必须大于 CommandProcessing:ReplyTimeoutSeconds（默认 15），
        // 否则探测命令还没被判超时，这里就已经放弃等待了。
        var probeTimeoutSeconds =
            configuration.GetValue<double?>("OnlineStateRecovery:ProbeTimeoutSeconds") ?? 30;
        if (probeTimeoutSeconds <= 0)
            throw new InvalidOperationException(
                "OnlineStateRecovery:ProbeTimeoutSeconds 必须大于 0");
        _probeTimeout = TimeSpan.FromSeconds(probeTimeoutSeconds);

        var staleMinutes =
            configuration.GetValue<double?>("OnlineStateRecovery:StaleMinutes") ?? 5;
        _staleThreshold = staleMinutes > 0
            ? TimeSpan.FromMinutes(staleMinutes)
            : TimeSpan.Zero;

        var sweepIntervalMinutes =
            configuration.GetValue<double?>("OnlineStateRecovery:SweepIntervalMinutes") ?? 0;
        _sweepInterval = sweepIntervalMinutes > 0
            ? TimeSpan.FromMinutes(sweepIntervalMinutes)
            : Timeout.InfiniteTimeSpan;

        var concurrency =
            configuration.GetValue<int?>("OnlineStateRecovery:MaxConcurrency") ?? 8;
        _maxConcurrency = concurrency > 0 ? concurrency : 1;

        _probeAction =
            configuration.GetValue<string>("OnlineStateRecovery:ProbeAction") ?? "get_state";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_enabled)
        {
            _logger.LogInformation("在线状态对账已关闭（OnlineStateRecovery:Enabled=false）");
            return;
        }

        _logger.LogInformation(
            "在线状态对账已启用，StartupDelay={StartupDelaySeconds}s, ProbeTimeout={ProbeTimeoutSeconds}s, StaleMinutes={StaleMinutes}, MaxConcurrency={MaxConcurrency}, ProbeAction={ProbeAction}",
            _startupDelay.TotalSeconds,
            _probeTimeout.TotalSeconds,
            _staleThreshold.TotalMinutes,
            _maxConcurrency,
            _probeAction);

        // 启动早期 RedisCmdWorker 还没执行 EnsureGroupAsync，
        // 此时写命令 Stream 会 NOGROUP，因此必须延迟。
        if (_startupDelay > TimeSpan.Zero)
            await Task.Delay(_startupDelay, stoppingToken);

        var lastConnected = _mqtt.IsConnected;

        try
        {
            await SweepAsync("启动", stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "启动后的在线状态对账失败");
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

                // 边沿检测：只在"从断开变为连接"时触发一次，避免抖动时反复扫描。
                var connected = _mqtt.IsConnected;
                if (_reconnectSweepEnabled && connected && !lastConnected)
                {
                    _logger.LogInformation("检测到 MQTT 重连，开始在线状态对账");
                    await SweepAsync("重连", stoppingToken);
                }
                lastConnected = connected;

                if (_sweepInterval != Timeout.InfiniteTimeSpan)
                {
                    await Task.Delay(_sweepInterval, stoppingToken);
                    await SweepAsync("周期", stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "在线状态对账失败");
            }
        }
    }

    private async Task SweepAsync(string trigger, CancellationToken ct)
    {
        // EnsureGroupAsync 幂等（内部吞 BUSYGROUP），先调用可彻底消除 NOGROUP 风险。
        await _commands.EnsureGroupAsync();

        var onlineIds = await ReadOnlineIdsAsync();
        var knownIds = await _devices.GetKnownDeviceIdsAsync();

        var targets = new HashSet<string>(knownIds, StringComparer.Ordinal);
        foreach (var id in onlineIds)
            targets.Add(id);

        if (targets.Count == 0)
            return;

        var recovered = 0;
        var cleaned = 0;
        var probed = 0;

        _logger.LogInformation(
            "在线状态对账开始（{Trigger}），待核对设备数={TargetCount}, 当前在线={OnlineCount}",
            trigger,
            targets.Count,
            onlineIds.Count);

        await Parallel.ForEachAsync(
            targets,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = _maxConcurrency,
                CancellationToken = ct
            },
            async (deviceId, token) =>
            {
                try
                {
                    var outcome = await ReconcileOneAsync(deviceId, onlineIds.Contains(deviceId), token);

                    switch (outcome)
                    {
                        case Outcome.Probed:
                            Interlocked.Increment(ref probed);
                            break;
                        case Outcome.Recovered:
                            Interlocked.Increment(ref recovered);
                            break;
                        case Outcome.Cleaned:
                            Interlocked.Increment(ref cleaned);
                            break;
                    }
                }
                catch (Exception ex)
                {
                    // 单台异常不能中断整批。
                    _logger.LogWarning(ex, "在线状态对账处理单台设备失败，DeviceId={DeviceId}", deviceId);
                }
            });

        _logger.LogInformation(
            "在线状态对账结束（{Trigger}），已探测={Probed}, 恢复在线={Recovered}, 清理僵尸={Cleaned}",
            trigger,
            probed,
            recovered,
            cleaned);
    }

    private enum Outcome
    {
        Skipped,
        Probed,
        Recovered,
        Cleaned
    }

    private async Task<Outcome> ReconcileOneAsync(
        string deviceId,
        bool currentlyOnline,
        CancellationToken ct)
    {
        var node = await _devices.GetAsync(deviceId);

        // 近期有业务消息 → 认为仍在线，不必探测。
        // 注意方向：这只能用来"跳过探测"，绝不能反过来用它判定离线——
        // LastSeenUtc 只被业务消息刷新，MQTT PING 不刷新它，
        // 业务静默的设备（例如空闲的总控）完全可能是正常在线的。
        if (node?.LastSeenUtc is not null &&
            _staleThreshold > TimeSpan.Zero &&
            DateTime.UtcNow - node.LastSeenUtc.Value < _staleThreshold)
        {
            return Outcome.Skipped;
        }

        var alive = await ProbeAsync(deviceId, ct);

        if (alive && !currentlyOnline)
        {
            // 补登记上线：type / aging / meshLevel 全部来自 runtime，绝不臆造。
            // 群控重估也在 RecoverOnlineAsync 内部完成——Worker 不能注入
            // GroupControlService（会与 MqttConnection 形成 DI 环）。
            return await _lifecycle.RecoverOnlineAsync(deviceId, node)
                ? Outcome.Recovered
                : Outcome.Probed;
        }

        if (!alive && currentlyOnline)
        {
            await _lifecycle.MarkOfflineAsync(
                deviceId,
                node?.DeviceType ?? string.Empty,
                messageSeq: null,
                rawPayload: "{\"reason\":\"recovery_probe_timeout\"}");

            _logger.LogWarning(
                "对账清理僵尸在线条目（探测无响应），DeviceId={DeviceId}",
                deviceId);

            return Outcome.Cleaned;
        }

        return Outcome.Probed;
    }

    /// <summary>
    /// 探测设备真实在线状态：发一条命令并等它的回复。
    /// 用命令回复而不是时间戳，是因为只有"设备真的回了"才能证明它还连着。
    /// </summary>
    private async Task<bool> ProbeAsync(string deviceId, CancellationToken ct)
    {
        try
        {
            var seq = await _cmd.SendCmdAsync(deviceId, _probeAction);

            var completion = await _commands.WaitForCompletionAsync(
                deviceId,
                seq,
                _probeTimeout,
                ct);

            return completion is not null &&
                   string.Equals(completion.Status, CommandStates.Completed, StringComparison.OrdinalIgnoreCase);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "在线状态对账探测失败，DeviceId={DeviceId}", deviceId);
            return false;
        }
    }

    private async Task<HashSet<string>> ReadOnlineIdsAsync()
    {
        var result = new HashSet<string>(StringComparer.Ordinal);

        foreach (var item in await _devices.GetOnlineDeviceJsonListAsync())
        {
            if (string.IsNullOrWhiteSpace(item))
                continue;

            try
            {
                var id = JObject.Parse(item)
                    .GetValue("id", StringComparison.OrdinalIgnoreCase)
                    ?.Value<string>();

                if (!string.IsNullOrWhiteSpace(id))
                    result.Add(RedisKeys.Normalize(id));
            }
            catch (Newtonsoft.Json.JsonException)
            {
                // 非法历史元素不参与对账。
            }
        }

        return result;
    }
}
