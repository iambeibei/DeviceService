using Bluetti.Device.Redis;
using Bluetti.Device.Redis.Models;
using DeviceService.Mqtt;
using DeviceService.Services;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using StackExchange.Redis;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace DeviceService.Workers;

/// <summary>
/// 消费 Redis 命令队列并发布 MQTT 指令。
///
/// 设计目标：
/// 1. 普通命令继续按 DeviceId 串行，收到 CmdSeq 回复或超时后处理下一条；
/// 2. start_aging 使用同设备独立专用队列，只受 AgingState 门控，不长期阻塞普通命令；
/// 3. agingStart 时后续 start_aging 暂存，agingStop / agingError / agingIdle 时放行；
/// 4. 不同设备可并行；
/// 5. Redis Stream Entry 在其所有 DeviceInfos 子命令结束前保持 Pending。
/// </summary>
public sealed class RedisCmdWorker : BackgroundService
{
    private const int BatchSize = 64;
    private const int RecoveryBatchSize = 256;

    private readonly CommandRepository _commands;
    private readonly DeviceRepository _devices;
    private readonly MqttConnection _mqtt;
    private readonly AgingCommandGate _agingGate;
    private readonly GroupControlService _groupControl;
    private readonly OtaUpgradeService _otaUpgrade;
    private readonly ILogger<RedisCmdWorker> _logger;
    private readonly string _consumerName;
    private readonly TimeSpan _replyTimeout;
    private readonly TimeSpan _publishRetryDelay;
    private readonly TimeSpan _startAgingStagger;
    private readonly TimeSpan _startAgingReplyTimeout;
    private readonly int _startAgingMaxAttempts;
    private readonly TimeSpan _startAgingRetryDelay;
    private readonly int _timeoutRetryCount;

    private readonly ConcurrentDictionary<string, DeviceCommandQueue> _deviceQueues =
        new(StringComparer.Ordinal);

    // start_aging 单独排队：它可以等待数小时，但不能堵住同设备的普通命令。
    private readonly ConcurrentDictionary<string, DeviceCommandQueue> _agingQueues =
        new(StringComparer.Ordinal);

    public RedisCmdWorker(
        CommandRepository commands,
        DeviceRepository devices,
        MqttConnection mqtt,
        AgingCommandGate agingGate,
        GroupControlService groupControl,
        OtaUpgradeService otaUpgrade,
        IConfiguration configuration,
        ILogger<RedisCmdWorker> logger)
    {
        _commands = commands;
        _devices = devices;
        _mqtt = mqtt;
        _agingGate = agingGate;
        _groupControl = groupControl;
        _otaUpgrade = otaUpgrade;
        _logger = logger;

        _consumerName = configuration.GetValue<string>("RedisSettings:ConsumerName")?? Environment.MachineName;

        var replyTimeoutSeconds = configuration.GetValue<double?>("CommandProcessing:ReplyTimeoutSeconds") ?? 15;
        if (replyTimeoutSeconds <= 0)
            throw new InvalidOperationException("CommandProcessing:ReplyTimeoutSeconds 必须大于 0");
        _replyTimeout = TimeSpan.FromSeconds(replyTimeoutSeconds);

        var retryDelayMilliseconds = configuration.GetValue<int?>("CommandProcessing:PublishRetryDelayMilliseconds") ?? 1000;
        if (retryDelayMilliseconds <= 0)
            throw new InvalidOperationException("CommandProcessing:PublishRetryDelayMilliseconds 必须大于 0");
        _publishRetryDelay = TimeSpan.FromMilliseconds(retryDelayMilliseconds);

        // 批量 start_aging 的错峰间隔。默认 0 表示不间隔（各设备并行下发）。
        // 注意：该延迟施加在 Stream 消费主循环里，会阻塞新命令读取 N×间隔毫秒。
        var staggerMilliseconds = configuration.GetValue<int?>(
            "CommandProcessing:StartAgingStaggerMilliseconds") ?? 0;
        if (staggerMilliseconds < 0)
            throw new InvalidOperationException(
                "CommandProcessing:StartAgingStaggerMilliseconds 不能为负数");
        _startAgingStagger = TimeSpan.FromMilliseconds(staggerMilliseconds);

        // 等待回复超时后的重试次数（普通命令）。0 = 关闭。
        // start_aging 走独立的重发流程，见 SendStartAgingWithAttemptsAsync。
        var timeoutRetry = configuration.GetValue<int?>(
            "CommandProcessing:TimeoutRetryCount") ?? 1;
        _timeoutRetryCount = timeoutRetry > 0 ? timeoutRetry : 0;

        // start_aging 专用等待回复超时。与普通命令分离，避免拉长所有命令的等待时间。
        var startAgingTimeoutSeconds = configuration.GetValue<double?>(
            "CommandProcessing:StartAgingReplyTimeoutSeconds") ?? 20;
        if (startAgingTimeoutSeconds <= 0)
            throw new InvalidOperationException(
                "CommandProcessing:StartAgingReplyTimeoutSeconds 必须大于 0");
        _startAgingReplyTimeout = TimeSpan.FromSeconds(startAgingTimeoutSeconds);

        // start_aging 的总下发次数（含首次）。1 = 不重发，等价于改造前的行为。
        var startAgingMaxAttempts = configuration.GetValue<int?>(
            "CommandProcessing:StartAgingMaxAttempts") ?? 5;
        if (startAgingMaxAttempts < 1)
            throw new InvalidOperationException(
                "CommandProcessing:StartAgingMaxAttempts 必须大于等于 1");
        _startAgingMaxAttempts = startAgingMaxAttempts;

        // 两次下发之间的间隔，0 = 不间隔。
        var startAgingRetryDelayMs = configuration.GetValue<int?>(
            "CommandProcessing:StartAgingRetryDelayMilliseconds") ?? 1000;
        if (startAgingRetryDelayMs < 0)
            throw new InvalidOperationException(
                "CommandProcessing:StartAgingRetryDelayMilliseconds 不能为负数");
        _startAgingRetryDelay = TimeSpan.FromMilliseconds(startAgingRetryDelayMs);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _commands.EnsureGroupAsync();

        _logger.LogInformation(
            "Redis 命令消费者已启动，Consumer={Consumer}, ReplyTimeout={ReplyTimeoutSeconds}s",
            _consumerName,
            _replyTimeout.TotalSeconds);

        try
        {
            // 服务重启后，先恢复“属于当前 Consumer、但尚未 XACK”的 Stream Pending 命令。
            // 恢复完成后正常循环只读 >，避免反复读取同一批 Pending 导致重复入队。
            await RecoverPendingCommandsAsync(stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var messages = await _commands.ReadAsync(_consumerName, ">", BatchSize);
                    if (messages.Count == 0)
                    {
                        await Task.Delay(200, stoppingToken);
                        continue;
                    }

                    _logger.LogInformation("从 Redis Stream 读取到 {Count} 条新命令", messages.Count);

                    foreach (var item in messages)
                    {
                        stoppingToken.ThrowIfCancellationRequested();
                        await DispatchBatchAsync(item.Id, item.Commands, stoppingToken);
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "读取/分发 Redis 命令失败");
                    await Task.Delay(1000, stoppingToken);
                }
            }
        }
        finally
        {
            foreach (var queue in _deviceQueues.Values)
                queue.Channel.Writer.TryComplete();
            foreach (var queue in _agingQueues.Values)
                queue.Channel.Writer.TryComplete();

            var processorTasks = _deviceQueues.Values
                .Concat(_agingQueues.Values)
                .Select(x => x.ProcessorTask)
                .ToArray();

            if (processorTasks.Length > 0)
            {
                try
                {
                    await Task.WhenAll(processorTasks);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    // 正常停止。
                }
            }
        }
    }

    /// <summary>
    /// 恢复当前 Consumer 的 Pending Stream Entries。
    /// position=0-0 表示读取这个 Consumer 已经拿到但未 ACK 的历史消息。
    /// </summary>
    private async Task RecoverPendingCommandsAsync(CancellationToken ct)
    {

        var position = "0-0";
        var recovered = 0;

        while (!ct.IsCancellationRequested)
        {
            var messages = await _commands.ReadAsync(_consumerName, position, RecoveryBatchSize);
            if (messages.Count == 0)
                break;

            foreach (var item in messages)
            {
                await DispatchBatchAsync(item.Id, item.Commands, ct);
                recovered++;
            }

            if (messages.Count < RecoveryBatchSize)
                break;

            // XREADGROUP 对显式 ID 读取的是“该 Consumer Pending 中大于该 ID”的数据。
            position = messages[^1].Id.ToString();
        }

        if (recovered > 0)
            _logger.LogInformation("已恢复 {Count} 条 Pending Redis 命令", recovered);
    }

    /// <summary>
    /// 一条 Redis Stream Entry 可以包含多个 DeviceInfos。
    /// 这里仅负责把已经由 CommandRepository 展开的 CmdInfo 分发到各设备原有串行队列。
    /// 所有设备子命令完成/失败/超时后，才对原始 Stream Entry 做一次 XACK。
    /// </summary>
    private async Task DispatchBatchAsync(
        RedisValue messageId,
        IReadOnlyList<CmdInfo> commands,
        CancellationToken ct)
    {
        if (commands.Count == 0)
        {
            _logger.LogWarning(
                "Redis 批量命令没有有效 DeviceInfos，将直接 ACK，StreamId={StreamId}",
                messageId);
            await _commands.AcknowledgeAsync(messageId);
            return;
        }

        if (commands.Any(command => IsStartAging(command.Action)))
        {
            await _groupControl.RegisterAgingBatchAsync(
                commands.Where(command => IsStartAging(command.Action)).ToArray(),
                ct);
        }

        // 多设备 ota 交给 OtaUpgradeService 按 Mesh 层级调度：
        // 分层升级要等上一层重启确认后才下发下一层，耗时远超单条命令的回复窗口，
        // 不能走命令流逐台分发。单设备 ota 没有 mesh 依赖，仍按普通命令直接转发。
        var otaCommands = commands.Where(command => IsOta(command.Action)).ToArray();
        if (_otaUpgrade.Enabled && otaCommands.Length > 1)
        {
            await TakeOverOtaBatchAsync(messageId, otaCommands, commands, ct);
            return;
        }

        var batch = new BatchAckTracker(messageId, commands.Count);

        _logger.LogInformation(
            "Redis 批量命令开始分发，Seq={Seq}, DeviceCount={DeviceCount}, StreamId={StreamId}",
            commands[0].Seq,
            commands.Count,
            messageId);

        for (var index = 0; index < commands.Count; index++)
        {
            ct.ThrowIfCancellationRequested();
            await DispatchAsync(batch, commands[index], ct);

            // 各设备的命令队列是独立的、并行消费的，入队即意味着马上开始发布。
            // 批量 start_aging 这里按顺序间隔入队，避免整架设备在同一瞬间启动。
            // 最后一台后面不再等待，避免给批次尾巴白白加上一段延迟。
            if (_startAgingStagger > TimeSpan.Zero &&
                IsStartAging(commands[index].Action) &&
                index < commands.Count - 1)
            {
                await Task.Delay(_startAgingStagger, ct);
            }
        }
    }

    /// <summary>
    /// 多设备 ota 指令的接管：交给 OtaUpgradeService 分层调度，
    /// 命令流侧把整条指令的命令项一次性结束，避免 Stream Entry 长时间挂着不 ACK。
    /// 真实进度查 lgw:ota:{架}:run:{Seq}:state。
    /// </summary>
    private async Task TakeOverOtaBatchAsync(
        RedisValue messageId,
        IReadOnlyList<CmdInfo> otaCommands,
        IReadOnlyList<CmdInfo> allCommands,
        CancellationToken ct)
    {
        var batch = new BatchAckTracker(messageId, allCommands.Count);
        var decision = await _otaUpgrade.TryTakeOverAsync(otaCommands);

        foreach (var command in allCommands)
        {
            var deviceId = RedisKeys.Normalize(command.DeviceId);

            if (decision.Accepted)
            {
                await _commands.MarkCompletedAsync(
                    command,
                    $"已由 OTA 分层升级服务接管，AgingNumber={decision.AgingNumber}，进度见 lgw:ota:{decision.AgingNumber}:run:{command.Seq}:state");
            }
            else
            {
                await _commands.MarkFailedAsync(command, decision.Reason);
            }

            await CompleteBatchItemAsync(batch, deviceId, command.Seq);
        }

        if (decision.Accepted)
        {
            _logger.LogWarning(
                "多设备 ota 指令已交由分层升级服务接管，Seq={Seq}, DeviceCount={DeviceCount}, AgingNumber={AgingNumber}, StreamId={StreamId}",
                otaCommands[0].Seq,
                otaCommands.Count,
                decision.AgingNumber,
                messageId);
        }
        else
        {
            _logger.LogWarning(
                "多设备 ota 指令接管失败，Seq={Seq}, DeviceCount={DeviceCount}, Reason={Reason}, StreamId={StreamId}",
                otaCommands[0].Seq,
                otaCommands.Count,
                decision.Reason,
                messageId);
        }
    }

    /// <summary>
    /// 根据 DeviceId 将拆分后的单设备命令投递到原有设备队列。
    /// start_aging 在创建老化专用队列（及其 AgingCommandGate 发送门）之前，
    /// 先通过 lgw:devices:online 检查设备是否在线；离线则不创建门控并直接结束该命令。
    /// </summary>
    private async Task DispatchAsync(BatchAckTracker batch, CmdInfo command, CancellationToken ct)
    {
        var deviceId = RedisKeys.Normalize(command.DeviceId);
        command.DeviceId = deviceId;

        var isStartAging = IsStartAging(command.Action);

        if (isStartAging && !await _devices.IsDeviceOnlineAsync(deviceId))
        {
            // 离线设备收不到 MQTT 指令，也不会再上报 agingStart / 终态，
            // 为它创建老化门控只会让命令在队列里永久等待，因此这里不创建
            // 设备老化专用队列和 AgingCommandGate 发送门，直接结束该命令。
            _logger.LogWarning(
                "start_aging 目标设备不在线，不创建老化门控，命令标记失败，DeviceId={DeviceId}, Seq={Seq}, StreamId={StreamId}",
                deviceId,
                command.Seq,
                batch.MessageId);

            await _commands.MarkFailedAsync(
                command,
                "设备不在线（未出现在 lgw:devices:online），拒绝创建老化门控");

            await CompleteBatchItemAsync(batch, deviceId, command.Seq);
            return;
        }

        _logger.LogInformation(
            "Redis 命令已进入设备串行队列，DeviceId={DeviceId}, Action={Action}, Seq={Seq}, StreamId={StreamId}",
            deviceId,
            command.Action,
            command.Seq,
            batch.MessageId);

        var queue = isStartAging
            ? _agingQueues.GetOrAdd(deviceId, id => CreateAgingQueue(id, ct))
            : _deviceQueues.GetOrAdd(deviceId, id => CreateDeviceQueue(id, ct));

        if (isStartAging)
        {
            // TaskQueue 只表示“尚未真正发布到 ESP32”的老化指令。
            // 先入 Redis 可让平台立即看到等待任务；真正 Publish 成功后再移除。
            await _devices.AddPendingAgingTaskAsync(deviceId, command.Seq);

            _logger.LogInformation(
                "start_aging 已进入设备老化专用队列，DeviceId={DeviceId}, Seq={Seq}, StreamId={StreamId}",
                deviceId,
                command.Seq,
                batch.MessageId);
        }

        await queue.Channel.Writer.WriteAsync(
            new QueuedCommand(batch, command),
            ct);
    }

    private DeviceCommandQueue CreateDeviceQueue(string deviceId, CancellationToken ct)
    {
        var channel = Channel.CreateUnbounded<QueuedCommand>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

        var processorTask = ProcessDeviceQueueAsync(deviceId, channel.Reader, ct);

        _logger.LogInformation("创建设备命令串行队列，DeviceId={DeviceId}", deviceId);
        return new DeviceCommandQueue(channel, processorTask);
    }

    private DeviceCommandQueue CreateAgingQueue(string deviceId, CancellationToken ct)
    {
        var channel = Channel.CreateUnbounded<QueuedCommand>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

        var processorTask = ProcessAgingQueueAsync(deviceId, channel.Reader, ct);

        _logger.LogInformation("创建设备 start_aging 专用队列，DeviceId={DeviceId}", deviceId);
        return new DeviceCommandQueue(channel, processorTask);
    }

    /// <summary>
    /// start_aging 的专用消费者。
    /// 第二条老化命令在第一条老化期间只在这里等待，不占用普通命令队列。
    /// agingStart 时保持等待；agingStop / agingError / agingIdle 后才允许真正 MQTT Publish。
    /// </summary>
    private async Task ProcessAgingQueueAsync(
        string deviceId,
        ChannelReader<QueuedCommand> reader,
        CancellationToken ct)
    {
        await foreach (var item in reader.ReadAllAsync(ct))
        {
            // 基础设施异常导致当前 item 重试时，必须记住本 item 是否已经占用了老化发送门，
            // 否则重试时再次 WaitAndReserve 会被自己留下的 Reservation 永久阻塞。
            var reservationHeldByThisItem = false;

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var persisted = await _commands.GetAsync(deviceId, item.Command.Seq);

                    // 已进入终态的历史 Pending 项只补批次完成，不重新发布 MQTT。
                    // 对 start_aging 必须区分“明确失败”和“仅回复超时”：
                    // - failed：下位机已经明确回复失败，可释放老化发送门；
                    // - timed_out：只能说明上位机没有收到回复，不能证明 ESP32 没有收到/执行，
                    //              因此必须继续保持发送门和 CurrentAgingSeq，直到收到
                    //              agingIdle / agingStop / agingError。
                    if (persisted is not null && CommandStates.IsTerminal(persisted.Status))
                    {
                        // 已经实际发布过/处理过的历史项不应继续出现在未运行 TaskQueue。
                        await _devices.RemovePendingAgingTaskAsync(deviceId, item.Command.Seq);

                        var terminal = await ProcessOneCommandAsync(
                            deviceId,
                            item,
                            ct,
                            allowTimeoutRetry: false);
                        if (string.Equals(terminal.Status, CommandStates.Failed, StringComparison.OrdinalIgnoreCase))
                        {
                            await _agingGate.ReleaseAfterCommandFailureAsync(deviceId, ct);
                            reservationHeldByThisItem = false;

                            var terminalState = await _agingGate.GetAgingStateAsync(deviceId, ct);
                            if (!IsAgingStartState(terminalState))
                                await _devices.ClearCurrentAgingTaskIdAsync(deviceId);
                        }
                        else if (string.Equals(terminal.Status, CommandStates.TimedOut, StringComparison.OrdinalIgnoreCase))
                        {
                            _logger.LogWarning(
                                "start_aging 已回复超时但继续保持老化发送门，等待设备状态明确退出后再放行下一条，DeviceId={DeviceId}, Seq={Seq}",
                                deviceId,
                                item.Command.Seq);
                        }
                        break;
                    }

                    var currentState = await _agingGate.GetAgingStateAsync(deviceId, ct);

                    // 服务重启恢复时，如果这条 start_aging 已标记 published 且设备当前明确 agingStart，
                    // 说明命令很可能已真正到达 ESP32。此时绝不能在老化结束后把同一条旧命令再发一次。
                    // 这里只等待它原来的 CmdSeq 回复/超时，后续新的 start_aging 仍由状态门继续阻挡。
                    if (persisted is not null &&
                        string.Equals(persisted.Status, CommandStates.Published, StringComparison.OrdinalIgnoreCase) &&
                        IsAgingStartState(currentState))
                    {
                        // 这条命令已经实际处于运行态，不应继续出现在 TaskQueue。
                        await _devices.RemovePendingAgingTaskAsync(deviceId, item.Command.Seq);
                        await _devices.SetCurrentAgingTaskIdAsync(deviceId, item.Command.Seq, item.Command.RecordId);

                        var completion = await WaitExistingPublishedCommandAsync(
                            deviceId,
                            item,
                            ct);

                        LogTerminalResult(completion);
                        break;
                    }

                    // 在真正发布 MQTT 前原子占用门。这样即使 cmd/reply 比 agingStart 先到，
                    // 下一条 start_aging 也不会穿过这个时间窗口提前发布。
                    if (!reservationHeldByThisItem)
                    {
                        await _agingGate.WaitAndReserveAsync(deviceId, ct);
                        reservationHeldByThisItem = true;
                    }

                    var result = await SendStartAgingWithAttemptsAsync(
                        deviceId,
                        item,
                        ct,
                        onPublished: async () =>
                        {
                            // MQTT Publish 真正成功后，这条 Seq 已不再属于“未运行任务”。
                            await _devices.RemovePendingAgingTaskAsync(deviceId, item.Command.Seq);
                            await _devices.SetCurrentAgingTaskIdAsync(deviceId, item.Command.Seq, item.Command.RecordId);
                        });

                    // start_aging 的发送门只在以下三种情况下释放：
                    // 1. 下位机明确回复失败（failed，含 Code != 1 的业务失败）；
                    // 2. 连续 StartAgingMaxAttempts 次下发都收不到 cmd/reply，判定节点异常（failed）；
                    // 3. 后续 state 事件上报 agingIdle / agingStop / agingError。
                    //
                    // 特别注意：单纯的 timed_out（设备已上报 agingStart，只是回复丢了）仍保留
                    // CurrentAgingSeq 和发送门，第二/第三条 start_aging 继续在 TaskQueue 中等待。
                    if (string.Equals(result.Command.Status, CommandStates.Failed, StringComparison.OrdinalIgnoreCase))
                    {
                        await _agingGate.ReleaseAfterCommandFailureAsync(deviceId, ct);
                        reservationHeldByThisItem = false;

                        var stateAfterFailure = await _agingGate.GetAgingStateAsync(deviceId, ct);
                        if (!IsAgingStartState(stateAfterFailure))
                            await _devices.ClearCurrentAgingTaskIdAsync(deviceId);

                        // 只有“耗尽重发且始终没有任何回复”才判节点异常；
                        // 设备明确回复 Code != 1 属于业务失败，只释放门、不记节点故障。
                        if (result.NodeFault)
                        {
                            var faultMessage = result.Command.ResultMessage
                                ?? "start_aging 连续下发未收到设备回复，判定节点异常";

                            _logger.LogError(
                                "start_aging 连续 {Attempts} 次下发均未收到 cmd/reply，判定节点异常并取消下发，DeviceId={DeviceId}, LogicalSeq={LogicalSeq}, AttemptSeqs={AttemptSeqs}",
                                result.Attempts,
                                deviceId,
                                item.Command.Seq,
                                string.Join(',', result.AttemptSeqs));

                            // 幂等兜底：发布成功时已出队，异常路径可能残留。
                            await _devices.RemovePendingAgingTaskAsync(deviceId, item.Command.Seq);
                            await _groupControl.HandleStartAgingNoReplyAsync(deviceId, item.Command.Seq, faultMessage);
                        }
                    }
                    else if (string.Equals(result.Command.Status, CommandStates.TimedOut, StringComparison.OrdinalIgnoreCase))
                    {
                        _logger.LogWarning(
                            "start_aging 连续 {Attempts} 次下发均未收到 cmd/reply，但设备已上报 agingStart，命令标记 timed_out，老化发送门继续保持；后续 start_aging 将等待 agingIdle/agingStop/agingError，DeviceId={DeviceId}, Seq={Seq}, AttemptSeqs={AttemptSeqs}",
                            result.Attempts,
                            deviceId,
                            item.Command.Seq,
                            string.Join(',', result.AttemptSeqs));
                    }
                    else if (string.Equals(result.Command.Status, CommandStates.Completed, StringComparison.OrdinalIgnoreCase))
                    {
                        // Code == 1 包含固件“指令重复，老化已启动”这类幂等回复，一律视为已送达。
                        // 此时发送门必须保持关闭：设备正在老化，下一条 start_aging 继续排队。
                        var stateAfterCompleted = await _agingGate.GetAgingStateAsync(deviceId, ct);
                        _logger.LogInformation(
                            "start_aging 已被设备确认，发送门保持关闭等待老化结束，DeviceId={DeviceId}, Seq={Seq}, Attempts={Attempts}, Code={Code}, Msg={Msg}, AgingState={AgingState}",
                            deviceId,
                            item.Command.Seq,
                            result.Attempts,
                            result.Command.ResultCode,
                            result.Command.ResultMessage ?? "<null>",
                            stateAfterCompleted ?? "<empty>");
                    }

                    break;
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "处理 start_aging 命令异常，将保留并重试，DeviceId={DeviceId}, Seq={Seq}, StreamId={StreamId}",
                        deviceId,
                        item.Command.Seq,
                        item.Batch.MessageId);

                    await Task.Delay(_publishRetryDelay, ct);
                }
            }
        }
    }

    /// <summary>
    /// start_aging 的多次下发流程：每次尝试使用独立的 MQTT Seq，超时未收到 cmd/reply 就重发，
    /// 达到 StartAgingMaxAttempts 仍无回复则本次下发失败。
    ///
    /// Seq 归属（改这里必须同时理解）：
    /// - 逻辑 Seq（item.Command.Seq）：Stream Entry 的身份，绑定 CurrentAgingSeq、群控 RunSeq、
    ///   批次 ACK 以及平台读取的命令 Hash，全程不变；
    /// - 下发 Seq（attempt.Seq）：仅第 1 次复用逻辑 Seq，第 2 次起新分配，
    ///   只用于 MQTT 报文 Seq 与 Data.CmdSeq 回复匹配，每次尝试单独落一条命令 Hash 便于追溯。
    /// 因此 CurrentAgingSeq 绝不能被写成下发 Seq，否则 RecordId 关联与群控 RunSeq 都会错位。
    /// </summary>
    private async Task<StartAgingAttemptResult> SendStartAgingWithAttemptsAsync(
        string deviceId,
        QueuedCommand item,
        CancellationToken ct,
        Func<Task>? onPublished)
    {
        var logical = item.Command;
        var attemptSeqs = new List<long>(_startAgingMaxAttempts);
        var attempt = logical;
        var attempts = 0;
        var exhausted = false;

        // 逻辑记录必须在第一次下发前就存在：迟到回复、CurrentAgingSeq → RecordId 关联都依赖它。
        await _commands.EnsureStoredAsync(logical);

        for (var attemptIndex = 1; attemptIndex <= _startAgingMaxAttempts; attemptIndex++)
        {
            ct.ThrowIfCancellationRequested();

            var attemptSeq = attemptIndex == 1
                ? logical.Seq
                : await _commands.NextAttemptSeqAsync();

            attempt = new CmdInfo
            {
                Seq = attemptSeq,
                DeviceId = logical.DeviceId,
                RecordId = logical.RecordId,
                Action = logical.Action,
                Data = logical.Data?.DeepClone(),
                Status = CommandStates.Queued,
                ExpectedReplyType = logical.ExpectedReplyType,
                CreatedAtUtc = logical.CreatedAtUtc == default ? DateTime.UtcNow : logical.CreatedAtUtc,
                RetryCount = attemptIndex - 1,
                LastAttemptSeq = attemptSeq
            };

            attemptSeqs.Add(attemptSeq);
            attempts = attemptIndex;

            // 与 ProcessOneCommandAsync 一致：先成功交给 Broker，再写 published。
            // 发布失败（未连接 / Broker 拒绝）沿用无限重试，不计入下发次数。
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await _commands.EnsureStoredAsync(attempt);
                    await PublishCommandAsync(attempt, ct);

                    // 只有第一次下发才更新 TaskQueue / CurrentAgingSeq；
                    // 后续重发必须沿用逻辑 Seq，不能改写 CurrentAgingSeq。
                    if (attemptIndex == 1 && onPublished is not null)
                        await onPublished();

                    await _commands.MarkPublishedAsync(attempt);
                    break;
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    await _commands.MarkQueuedAsync(attempt, ex.Message);

                    _logger.LogWarning(
                        ex,
                        "start_aging MQTT 发布失败，将重试本次尝试，DeviceId={DeviceId}, Attempt={Attempt}/{MaxAttempts}, Seq={Seq}, RetryAfterMs={RetryAfterMs}",
                        deviceId,
                        attemptIndex,
                        _startAgingMaxAttempts,
                        attemptSeq,
                        _publishRetryDelay.TotalMilliseconds);

                    await Task.Delay(_publishRetryDelay, ct);
                }
            }

            ct.ThrowIfCancellationRequested();

            await _commands.UpdateAttemptProgressAsync(deviceId, logical.Seq, attemptIndex, attemptSeq);

            _logger.LogInformation(
                "start_aging 已下发，等待设备回复，DeviceId={DeviceId}, Attempt={Attempt}/{MaxAttempts}, LogicalSeq={LogicalSeq}, AttemptSeq={AttemptSeq}, TimeoutSeconds={TimeoutSeconds}",
                deviceId,
                attemptIndex,
                _startAgingMaxAttempts,
                logical.Seq,
                attemptSeq,
                _startAgingReplyTimeout.TotalSeconds);

            var reply = await _commands.WaitForCompletionAsync(
                deviceId,
                attemptSeq,
                _startAgingReplyTimeout,
                ct);

            if (reply is not null)
            {
                _logger.LogInformation(
                    "start_aging 收到设备回复，DeviceId={DeviceId}, Attempt={Attempt}/{MaxAttempts}, AttemptSeq={AttemptSeq}, Status={Status}, Code={Code}, Msg={Msg}",
                    deviceId,
                    attemptIndex,
                    _startAgingMaxAttempts,
                    attemptSeq,
                    reply.Status,
                    reply.ResultCode,
                    reply.ResultMessage);

                return await FinalizeStartAgingAttemptsAsync(
                    deviceId, item, reply, attempts, exhausted: false, attemptSeqs, ct);
            }

            attempt = await _commands.MarkTimedOutAsync(attempt, _startAgingReplyTimeout);

            // 超时落库之前回复可能已经到达：MarkTimedOutAsync 有终态保护，会原样返回真实结果。
            if (!string.Equals(attempt.Status, CommandStates.TimedOut, StringComparison.OrdinalIgnoreCase))
            {
                return await FinalizeStartAgingAttemptsAsync(
                    deviceId, item, attempt, attempts, exhausted: false, attemptSeqs, ct);
            }

            // 更早某次尝试的迟到回复可能已经写进逻辑记录（completed / failed）。
            // 命中就必须停止重发，否则后续下发会覆盖掉设备的真实答复。
            var logicalNow = await _commands.GetAsync(deviceId, logical.Seq);
            if (logicalNow is not null &&
                CommandStates.IsTerminal(logicalNow.Status) &&
                !string.Equals(logicalNow.Status, CommandStates.TimedOut, StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning(
                    "start_aging 重发期间收到更早尝试的迟到回复，停止重发，DeviceId={DeviceId}, LogicalSeq={LogicalSeq}, Status={Status}",
                    deviceId,
                    logical.Seq,
                    logicalNow.Status);

                return await FinalizeStartAgingAttemptsAsync(
                    deviceId, item, logicalNow, attempts, exhausted: false, attemptSeqs, ct);
            }

            if (attemptIndex >= _startAgingMaxAttempts)
            {
                exhausted = true;
                break;
            }

            _logger.LogWarning(
                "start_aging 在 {TimeoutSeconds}s 内未收到 cmd/reply，准备第 {NextAttempt} 次下发，DeviceId={DeviceId}, LogicalSeq={LogicalSeq}, AttemptSeq={AttemptSeq}",
                _startAgingReplyTimeout.TotalSeconds,
                attemptIndex + 1,
                deviceId,
                logical.Seq,
                attemptSeq);

            if (_startAgingRetryDelay > TimeSpan.Zero)
                await Task.Delay(_startAgingRetryDelay, ct);
        }

        return await FinalizeStartAgingAttemptsAsync(
            deviceId, item, attempt, attempts, exhausted, attemptSeqs, ct);
    }

    /// <summary>
    /// 把多次下发的结果落到逻辑命令记录，并按设备当前老化状态决定是否判定节点异常。
    /// 设备若已上报 agingStart，说明命令其实生效、只是回复丢了，此时绝不能判异常，
    /// 否则会重复下发并在已有老化任务上再次启动老化。
    /// </summary>
    private async Task<StartAgingAttemptResult> FinalizeStartAgingAttemptsAsync(
        string deviceId,
        QueuedCommand item,
        CmdInfo finalResult,
        int attempts,
        bool exhausted,
        IReadOnlyList<long> attemptSeqs,
        CancellationToken ct)
    {
        string? exhaustedReason = null;

        if (exhausted)
        {
            var agingState = await _agingGate.GetAgingStateAsync(deviceId, ct);
            if (IsAgingStartState(agingState))
            {
                _logger.LogWarning(
                    "start_aging 连续 {Attempts} 次下发均未收到 cmd/reply，但设备已上报 agingStart，视为命令已生效，不判节点异常；保持发送门等待 agingIdle/agingStop/agingError，DeviceId={DeviceId}, LogicalSeq={LogicalSeq}, AttemptSeqs={AttemptSeqs}",
                    attempts,
                    deviceId,
                    item.Command.Seq,
                    string.Join(',', attemptSeqs));
            }
            else
            {
                exhaustedReason =
                    $"start_aging 连续 {attempts} 次下发（Seq={string.Join(',', attemptSeqs)}）均未收到 cmd/reply（每次 {_startAgingReplyTimeout.TotalSeconds:0.###}s），判定节点异常";
            }
        }

        var finalized = await _commands.FinalizeAttemptsAsync(
            deviceId,
            item.Command.Seq,
            finalResult,
            attempts,
            exhaustedReason);

        await CompleteBatchItemAsync(item.Batch, deviceId, item.Command.Seq);
        LogTerminalResult(finalized);

        return new StartAgingAttemptResult(
            finalized,
            attempts,
            NodeFault: exhaustedReason is not null,
            attemptSeqs);
    }

    /// <summary>
    /// 单个 DeviceId 的唯一消费者。
    /// 命令 N 未收到回复/未超时时，不会进入命令 N+1。
    /// </summary>
    private async Task ProcessDeviceQueueAsync(
        string deviceId,
        ChannelReader<QueuedCommand> reader,
        CancellationToken ct)
    {
        await foreach (var item in reader.ReadAllAsync(ct))
        {
            // 当前命令没有进入“已回复/已超时并 ACK”的终点前，绝不处理本设备下一条命令。
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    _ = await ProcessOneCommandAsync(deviceId, item, ct);
                    break;
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    // Redis/MQTT 等基础设施短暂异常时重试同一条命令，而不是越过它去执行下一条。
                    // 这样可以严格保持单设备命令顺序。
                    _logger.LogError(
                        ex,
                        "处理设备命令异常，将重试同一条命令，DeviceId={DeviceId}, Seq={Seq}, StreamId={StreamId}",
                        deviceId,
                        item.Command.Seq,
                        item.Batch.MessageId);

                    await Task.Delay(_publishRetryDelay, ct);
                }
            }
        }
    }

    private async Task<CmdInfo> ProcessOneCommandAsync(
        string deviceId,
        QueuedCommand item,
        CancellationToken ct,
        Func<Task>? onPublished = null,
        bool allowTimeoutRetry = true)
    {
        var command = item.Command;

        // Stream Pending 恢复时，先看 Redis 中该命令是否其实已经进入终态。
        // 若回复已落库但进程在 XACK 前退出，这里只补 ACK，不重复 MQTT 下发。
        var persisted = await _commands.GetAsync(deviceId, command.Seq);

        // 重启恢复时把已持久化的重试次数带回来，避免重启后重新从 0 计、无限重试。
        if (persisted is not null && command.RetryCount == 0)
            command.RetryCount = persisted.RetryCount;

        if (persisted is not null && CommandStates.IsTerminal(persisted.Status))
        {
            await CompleteBatchItemAsync(item.Batch, deviceId, command.Seq);
            _logger.LogInformation(
                "命令已处于终态，完成本设备批次项，DeviceId={DeviceId}, Seq={Seq}, Status={Status}",
                deviceId,
                command.Seq,
                persisted.Status);
            return persisted;
        }

        // MQTT 发布前先持久化 DeviceId/Seq/RecordId 等元数据。
        // 下位机可能在发布后非常快地上报 agingStart，因此 RecordId 必须提前可查。
        await _commands.EnsureStoredAsync(command);

        // 对所有“Pending 且未进入终态”的命令都重新执行 MQTT Publish。
        // 这里不能仅凭 Redis Hash 中的 published 状态就跳过发布：
        // 上一版 RedisCmdWorker 是先 MarkPublishedAsync 再 PublishAsync，
        // 因此历史数据中可能存在“状态已经是 published，但 MQTT 实际并未发送”的命令。
        // Stream 本身提供至少一次投递语义；重启恢复时使用同一个 Seq 重新发布，
        // 设备端应基于 Seq/CmdSeq 做幂等去重。
        if (persisted is not null &&
            string.Equals(persisted.Status, CommandStates.Published, StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogWarning(
                "恢复到未 ACK 的 published 命令，将使用原 Seq 重新发布以避免假 published，DeviceId={DeviceId}, Seq={Seq}",
                deviceId,
                command.Seq);
        }

        CmdInfo completion;

        // 「发布 + 等回复」整体包在重试循环里：等待回复超时也算一次失败机会。
        // 重试点在 CompleteBatchItemAsync **之前**，因此此时尚未 XACK；
        // 但重试仅限进程内循环，重试期间进程崩溃该命令仍会丢失（已知取舍）。
        while (true)
        {
            ct.ThrowIfCancellationRequested();

            // 新命令（或上次 MQTT Publish 失败后仍为 queued 的命令）。
            // Publish 失败时只重试当前设备当前命令；其他设备队列不受影响。
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    // 先成功交给 MQTT Broker，再写 published。
                    // 如果进程恰好在两者之间退出，Stream Entry 仍是 Pending 且状态仍为 queued，
                    // 重启后会用同一个 Seq 再发一次；设备端应按 Seq 去重，这比“命令实际没发出却被当成 published”更安全。
                    await PublishCommandAsync(command, ct);

                    // 对 start_aging：Publish 成功即代表这条命令已经离开 TaskQueue，
                    // 并成为当前等待 agingIdle/agingStop/agingError 的任务。
                    if (onPublished is not null)
                        await onPublished();

                    await _commands.MarkPublishedAsync(command);
                    break;
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    await _commands.MarkQueuedAsync(command, ex.Message);

                    _logger.LogWarning(
                        ex,
                        "MQTT 命令发布失败，将重试当前设备当前命令，DeviceId={DeviceId}, Seq={Seq}, RetryAfterMs={RetryAfterMs}",
                        deviceId,
                        command.Seq,
                        _publishRetryDelay.TotalMilliseconds);

                    await Task.Delay(_publishRetryDelay, ct);
                }
            }

            ct.ThrowIfCancellationRequested();

            // MQTT 已发布，但此时绝不 XACK。
            // 只有匹配 DeviceId + CmdSeq 的四类回复处理器完成落库，或等待回复超时，才结束这条 Stream 命令项。
            // 对普通命令，timed_out 会自然放行下一条；对 start_aging，外层 ProcessAgingQueueAsync
            // 会继续保持长期老化发送门，直到状态事件明确上报 agingIdle / agingStop / agingError。
            var waited = await _commands.WaitForCompletionAsync(
                deviceId,
                command.Seq,
                _replyTimeout,
                ct);

            if (waited is not null)
            {
                completion = waited;
                break;
            }

            completion = await _commands.MarkTimedOutAsync(command, _replyTimeout);

            // start_aging 绝不能因超时重发：超时只说明上位机没收到回复，
            // 不能证明 ESP32 没执行，重发会重复消耗 PN（AgingCommandGate 就是为这个场景设计的）。
            if (!allowTimeoutRetry || command.RetryCount >= _timeoutRetryCount)
                break;

            command.RetryCount++;

            // 必须先复位再重试：WaitForCompletionAsync 遇终态会立即返回
            // （timed_out 是终态），不复位的话下一次等待形同虚设。
            await _commands.ResetForRetryAsync(deviceId, command.Seq);

            _logger.LogWarning(
                "命令等待回复超时，使用原 Seq 重试第 {RetryCount} 次，DeviceId={DeviceId}, Seq={Seq}, Action={Action}",
                command.RetryCount,
                deviceId,
                command.Seq,
                command.Action);
        }

        ct.ThrowIfCancellationRequested();

        await CompleteBatchItemAsync(item.Batch, deviceId, command.Seq);
        LogTerminalResult(completion);
        return completion;
    }

    /// <summary>
    /// 恢复场景：start_aging 已经 published 且设备正处于 agingStart 时，不重复发布旧命令。
    /// 仅等待原 CmdSeq 的回复；超时后结束 Redis 命令项，但老化发送门仍由设备状态控制。
    /// </summary>
    private async Task<CmdInfo> WaitExistingPublishedCommandAsync(
        string deviceId,
        QueuedCommand item,
        CancellationToken ct)
    {
        var command = item.Command;

        _logger.LogWarning(
            "恢复到已发布且设备正在 agingStart 的 start_aging，不重复 MQTT 发布，DeviceId={DeviceId}, Seq={Seq}",
            deviceId,
            command.Seq);

        var completion = await _commands.WaitForCompletionAsync(
            deviceId,
            command.Seq,
            _replyTimeout,
            ct);

        if (completion is null)
            completion = await _commands.MarkTimedOutAsync(command, _replyTimeout);

        await CompleteBatchItemAsync(item.Batch, deviceId, command.Seq);
        return completion;
    }

    private static bool IsStartAging(string action)
        => string.Equals(action?.Trim(), "start_aging", StringComparison.OrdinalIgnoreCase);

    private static bool IsOta(string action)
        => string.Equals(action?.Trim(), "ota", StringComparison.OrdinalIgnoreCase);

    private static bool IsAgingStartState(string? state)
        => string.Equals(state?.Trim(), "agingStart", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// 标记批量命令中的一个设备子命令已经结束。
    /// 只有最后一个设备结束时才 ACK 原始 Redis Stream Entry。
    /// </summary>
    private async Task CompleteBatchItemAsync(
        BatchAckTracker batch,
        string deviceId,
        long seq)
    {
        var remaining = batch.CompleteOne(deviceId, seq);

        _logger.LogInformation(
            "批量命令设备项完成，DeviceId={DeviceId}, Seq={Seq}, Remaining={Remaining}, StreamId={StreamId}",
            deviceId,
            seq,
            remaining,
            batch.MessageId);

        if (remaining != 0 || !batch.TryBeginAcknowledge())
            return;

        try
        {
            await _commands.AcknowledgeAsync(batch.MessageId);
            batch.MarkAcknowledged();

            _logger.LogInformation(
                "批量命令所有设备均已结束，已 ACK Redis Stream，Seq={Seq}, StreamId={StreamId}",
                seq,
                batch.MessageId);
        }
        catch
        {
            // ACK 失败时允许当前设备队列重试 ACK，但不会再次减少 Remaining。
            batch.AllowAcknowledgeRetry();
            throw;
        }
    }

    private async Task PublishCommandAsync(CmdInfo command, CancellationToken ct)
    {
        // 最新协议要求所有平台指令都包含 Data 字段；
        // 无参数指令也必须显式发送 "Data": null。
        var envelope = new JObject
        {
            ["Seq"] = command.Seq,
            ["Data"] = command.Data is null
                ? JValue.CreateNull()
                : command.Data.DeepClone(),
            ["RecordId"]= command.RecordId?? ""
        };

        var topic = $"server/{command.DeviceId}/cmd/{command.Action}";
        var payload = envelope.ToString(Formatting.None);

        await _mqtt.PublishAsync(topic, payload, ct);
        _logger.LogInformation(
            "命令已发布并开始等待回复，DeviceId={DeviceId}, Action={Action}, Seq={Seq}",
            command.DeviceId,
            command.Action,
            command.Seq);
    }

    private void LogTerminalResult(CmdInfo command)
    {
        if (string.Equals(command.Status, CommandStates.TimedOut, StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogWarning(
                "设备命令等待回复超时，DeviceId={DeviceId}, Seq={Seq}, Action={Action}",
                command.DeviceId,
                command.Seq,
                command.Action);
            return;
        }

        _logger.LogInformation(
            "设备命令已收到回复并放行下一条，DeviceId={DeviceId}, Seq={Seq}, Action={Action}, Status={Status}, Code={Code}",
            command.DeviceId,
            command.Seq,
            command.Action,
            command.Status,
            command.ResultCode);
    }

    private sealed record QueuedCommand(BatchAckTracker Batch, CmdInfo Command);

    /// <summary>
    /// start_aging 多次下发的最终结果。
    /// <paramref name="NodeFault"/> 为 true 表示“耗尽下发次数且设备始终没有任何回复”，需要判定节点异常。
    /// </summary>
    private sealed record StartAgingAttemptResult(
        CmdInfo Command,
        int Attempts,
        bool NodeFault,
        IReadOnlyList<long> AttemptSeqs);

    private sealed record DeviceCommandQueue(
        Channel<QueuedCommand> Channel,
        Task ProcessorTask);

    /// <summary>
    /// 一个实例对应一条原始 Redis Stream Entry。
    /// 多个设备队列共享同一个计数器，确保整批只 ACK 一次。
    /// </summary>
    private sealed class BatchAckTracker
    {
        private readonly ConcurrentDictionary<string, byte> _completedItems =
            new(StringComparer.Ordinal);

        private int _remaining;
        private int _ackState; // 0=可ACK，1=ACK进行中，2=ACK成功

        public BatchAckTracker(RedisValue messageId, int count)
        {
            if (count <= 0)
                throw new ArgumentOutOfRangeException(nameof(count));

            MessageId = messageId;
            _remaining = count;
        }

        public RedisValue MessageId { get; }

        public int CompleteOne(string deviceId, long seq)
        {
            var itemKey = $"{deviceId}:{seq}";
            if (!_completedItems.TryAdd(itemKey, 0))
                return Volatile.Read(ref _remaining);

            var remaining = Interlocked.Decrement(ref _remaining);
            if (remaining < 0)
                throw new InvalidOperationException($"批量命令完成计数异常，StreamId={MessageId}");

            return remaining;
        }

        public bool TryBeginAcknowledge()
            => Volatile.Read(ref _remaining) == 0 &&
               Interlocked.CompareExchange(ref _ackState, 1, 0) == 0;

        public void MarkAcknowledged()
            => Interlocked.Exchange(ref _ackState, 2);

        public void AllowAcknowledgeRetry()
            => Interlocked.CompareExchange(ref _ackState, 0, 1);
    }
}
