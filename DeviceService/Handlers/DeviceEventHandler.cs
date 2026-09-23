using Bluetti.Device.Redis;
using DeviceService.Mqtt;
using DeviceService.Services;
using MQTTnet;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using StackExchange.Redis;
using System.Reflection.Metadata;
using System.Text;

namespace DeviceService.Handlers;

/// <summary>
/// 处理 device/{id}/event/state、fault、warn、StepId。
/// state 事件同时负责：
/// 1. 保存下位机主动上报的老化状态；
/// 2. 通过 CurrentAgingSeq 找到 start_aging 对应 RecordId 并回调业务 API；
/// 3. agingIdle / agingStop / agingError 时清当前任务并释放下一条 start_aging。
/// </summary>
public sealed class DeviceEventHandler : IMqttMessageHandler
{
    private readonly DeviceRepository _devices;
    private readonly CommandRepository _commands;
    private readonly AgingCommandGate _agingGate;
    private readonly AgingRecordCallbackClient _callbackClient;
    private readonly GroupControlService _groupControl;
    private readonly ILogger<DeviceEventHandler> _logger;
    private readonly RedisHelper _redisHelper;

    public DeviceEventHandler(DeviceRepository devices,CommandRepository commands,
        AgingCommandGate agingGate,
        AgingRecordCallbackClient callbackClient,
        GroupControlService groupControl,
        ILogger<DeviceEventHandler> logger,
        RedisHelper redisHelper)
    {
        _devices = devices;
        _commands = commands;
        _agingGate = agingGate;
        _callbackClient = callbackClient;
        _groupControl = groupControl;
        _logger = logger;
        _redisHelper = redisHelper;
    }

    public bool CanHandle(string topic)
        => MqttTopic.TryGetEventType(topic, out _, out _);

    public async Task HandleAsync(MqttApplicationMessage message)
    {
        if (!MqttTopic.TryGetEventType(message.Topic, out var deviceId, out var eventType))
            return;

        var payload = Encoding.UTF8.GetString(message.Payload);
        JObject json;
        try
        {
            json = JObject.Parse(payload);
        }
        catch (JsonException ex)
        {
            _logger.LogError(
                ex,
                "设备事件 JSON 解析失败，Topic={Topic}, Payload={Payload}",
                message.Topic,
                payload);
            return;
        }

        var agingState = json.SelectToken("Data.AgingState")?.Value<string>();
        var isStateEvent = eventType.Equals("state", StringComparison.OrdinalIgnoreCase) &&
                           !string.IsNullOrWhiteSpace(agingState);

        if(eventType.Equals("warn", StringComparison.OrdinalIgnoreCase))
        {
            var jsonWarn = JsonConvert.SerializeObject(new
            {
                DeviceID = deviceId,
                type = eventType,
                Message = json.SelectToken("Data.Msg")?.Value<string>() ?? "无",
                ReceivedAtUtc = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")
            });

            await _redisHelper.PublishAsync(RedisChannel.Literal(RedisKeys.channelWarn), jsonWarn);
        }
        else if(message.Topic.Contains("fault")) 
        {
            var jsonFault = JsonConvert.SerializeObject(new
            {
                DeviceID = deviceId,
                type = eventType,
                Message = json.SelectToken("Data.Msg")?.Value<string>() ?? "无",
                ReceivedAtUtc = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")
            });

            await _redisHelper.PublishAsync(RedisChannel.Literal(RedisKeys.channelFault), jsonFault);

        }
        else if(eventType.Equals("stepid", StringComparison.OrdinalIgnoreCase))
        {
            var StepId = json.SelectToken("Data.StepId")?.Value<string>();
            await _devices.StepIDSaveToRuntime(deviceId, StepId);
        }

        try
        {
            await _devices.SaveEventAsync(
                deviceId,
                eventType,
                json["Seq"]?.Value<long?>(),
                agingState,
                json.SelectToken("Data.Msg")?.Value<string>(),
                payload);
        }
        catch (Exception ex)
        {
            // Redis 短暂异常不能让已经收到的 ESP32 状态完全失效。
            // 对 state 事件仍继续更新内存 AgingCommandGate，避免后续老化任务永久卡住。
            _logger.LogError(
                ex,
                "设备事件保存 Redis 失败，Topic={Topic}, Payload={Payload}",
                message.Topic,
                payload);
        }

        if (!isStateEvent)
            return;

        var isExitState = IsExitState(agingState);
        long? currentAgingSeq = null;

        // 状态回调只做快速后台队列入队，不在 MQTT 消息线程等待 HTTPS。
        // 入队失败/关联异常也不能改变下位机真实状态，更不能永久阻塞下一条 start_aging。
        try
        {
            var callbackStatus = AgingRecordCallbackClient.MapStatus(agingState);
            if (callbackStatus is not null)
            {
                // SaveEventAsync 不会提前清 CurrentAgingSeq，所以这里可以稳定关联：
                // DeviceId + CurrentAgingSeq -> cmd Hash -> RecordId。
                currentAgingSeq = await _devices.GetCurrentAgingTaskIdAsync(deviceId);

                // 极窄竞态保护：MQTT QoS1 发布后，ESP32 可能非常快地上报 agingStart，
                // 此时 Worker 还未来得及把已发布 Seq 从 TaskQueue 移到 CurrentAgingSeq。
                // 仅对 agingStart 使用 TaskQueue 队头恢复关联；退出状态绝不猜测下一条任务。
                if (!currentAgingSeq.HasValue && IsAgingStartState(agingState))
                {
                    var pendingSeq = await _devices.GetFirstPendingAgingTaskAsync(deviceId);
                    if (pendingSeq.HasValue)
                    {
                        currentAgingSeq = pendingSeq;
                        await _devices.SetCurrentAgingTaskIdAsync(deviceId, pendingSeq.Value);
                        await _devices.RemovePendingAgingTaskAsync(deviceId, pendingSeq.Value);

                        _logger.LogInformation(
                            "agingStart 到达早于 CurrentAgingSeq 写入，已从 TaskQueue 恢复关联，DeviceId={DeviceId}, Seq={Seq}",
                            deviceId,
                            pendingSeq.Value);
                    }
                }

                if (currentAgingSeq.HasValue)
                {
                    var command = await _commands.GetAsync(deviceId, currentAgingSeq.Value);
                    var recordId = command?.RecordId?.Trim();

                    // 命令 Hash 默认有 TTL；超长老化任务结束时若命令 Hash 已过期，
                    // 回退使用 runtime.CurrentAgingRecordId，保证仍能完成最终状态回调。
                    if (string.IsNullOrWhiteSpace(recordId))
                        recordId = (await _devices.GetCurrentAgingRecordIdAsync(deviceId))?.Trim();
                    else
                        await _devices.SetCurrentAgingTaskIdAsync(deviceId, currentAgingSeq.Value, recordId);

                    if (!string.IsNullOrWhiteSpace(recordId))
                    {
                        await _callbackClient.QueueAsync(
                            recordId,
                            agingState!,
                            CancellationToken.None);
                    }
                    else
                    {
                        _logger.LogWarning(
                            "老化状态已收到但找不到对应 RecordId，DeviceId={DeviceId}, CurrentAgingSeq={CurrentAgingSeq}, AgingState={AgingState}",
                            deviceId,
                            currentAgingSeq.Value,
                            agingState);
                    }
                }
                else
                {
                    // 设备启动/空闲时主动上报 agingIdle 等场景可能没有当前任务，此时只保存状态，不回调。
                    _logger.LogInformation(
                        "老化状态没有对应 CurrentAgingSeq，不执行业务 API 回调，DeviceId={DeviceId}, AgingState={AgingState}",
                        deviceId,
                        agingState);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "老化状态业务 API 回调处理异常，DeviceId={DeviceId}, AgingState={AgingState}",
                deviceId,
                agingState);
        }

        try
        {
            await _groupControl.HandleAgingStateAsync(
                deviceId,
                currentAgingSeq,
                agingState!);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "处理群控批次状态失败，DeviceId={DeviceId}, CurrentAgingSeq={CurrentAgingSeq}, AgingState={AgingState}",
                deviceId,
                currentAgingSeq?.ToString() ?? "<empty>",
                agingState);
        }

        if (isExitState)
        {
            try
            {
                // 必须在 RecordId 查询并完成 API 回调入队后再清除，否则会丢失当前任务关联。
                await _devices.ClearCurrentAgingTaskIdAsync(deviceId);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "清理 CurrentAgingSeq 失败，DeviceId={DeviceId}, AgingState={AgingState}",
                    deviceId,
                    agingState);
            }
        }

        // agingStart 保持下一条老化指令等待；agingIdle/agingStop/agingError 放行。
        // 即使 Redis 或业务 API 暂时异常，也优先遵循 ESP32 主动上报的真实状态。
        _agingGate.NotifyAgingState(deviceId, agingState);
    }

    private static bool IsAgingStartState(string? state)
        => string.Equals(state?.Trim(), "agingStart", StringComparison.OrdinalIgnoreCase);

    private static bool IsExitState(string? state)
    {
        var normalized = state?.Trim().ToLowerInvariant();
        return normalized is "agingidle" or "agingstop" or "agingerror";
    }
}
