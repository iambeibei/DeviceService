using Bluetti.Device.Redis;
using DeviceService.Mqtt;
using DeviceService.Services;
using MQTTnet;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Text;

namespace DeviceService.Handlers;

public sealed class GroupControlModeHandler : IMqttMessageHandler
{
    private readonly GroupControlService _groupControl;
    private readonly ILogger<GroupControlModeHandler> _logger;

    public GroupControlModeHandler(
        GroupControlService groupControl,
        ILogger<GroupControlModeHandler> logger)
    {
        _groupControl = groupControl;
        _logger = logger;
    }

    public bool CanHandle(string topic) => MqttTopic.IsGroupControlMode(topic);

    public async Task HandleAsync(MqttApplicationMessage message)
    {
        var payload = Encoding.UTF8.GetString(message.Payload);
        try
        {
            var json = JObject.Parse(payload);
            var seqToken = json["Seq"];
            var modeToken = json.SelectToken("Data.Mode");
            var agingNumberToken = json.SelectToken("Data.AgingNumber");
            var seq = seqToken?.Type == JTokenType.Integer ? seqToken.Value<long?>() : null;
            var mode = modeToken?.Type == JTokenType.Integer ? modeToken.Value<int?>() : null;
            var agingNumber = agingNumberToken?.Type == JTokenType.String
                ? agingNumberToken.Value<string>()
                : null;

            if (!seq.HasValue || seq.Value <= 0 || mode is not (1 or 2) ||
                string.IsNullOrWhiteSpace(agingNumber))
            {
                _logger.LogWarning(
                    "群控模式上报缺少有效 Seq、Data.Mode 或 Data.AgingNumber，Payload={Payload}",
                    payload);
                return;
            }

            await _groupControl.UpdateModeAsync(
                RedisKeys.NormalizeAgingNumber(agingNumber),
                mode.Value,
                seq.Value);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "群控模式上报 JSON 非法，Payload={Payload}", payload);
        }
        catch (ArgumentException ex)
        {
            _logger.LogWarning(ex, "群控模式上报取值非法，Payload={Payload}", payload);
        }
    }
}

public sealed class GroupControlTaskCompletionHandler : IMqttMessageHandler
{
    private readonly GroupControlService _groupControl;
    private readonly ILogger<GroupControlTaskCompletionHandler> _logger;

    public GroupControlTaskCompletionHandler(
        GroupControlService groupControl,
        ILogger<GroupControlTaskCompletionHandler> logger)
    {
        _groupControl = groupControl;
        _logger = logger;
    }

    public bool CanHandle(string topic)
        => MqttTopic.TryGetTaskCompletionDeviceId(topic, out _);

    public async Task HandleAsync(MqttApplicationMessage message)
    {
        if (!MqttTopic.TryGetTaskCompletionDeviceId(message.Topic, out var deviceId))
            return;

        var payload = Encoding.UTF8.GetString(message.Payload);
        try
        {
            var json = JObject.Parse(payload);
            var seqToken = json["Seq"];
            var completeToken = json.SelectToken("Data.IsComplete");
            var seq = seqToken?.Type == JTokenType.Integer ? seqToken.Value<long?>() : null;
            var isComplete = completeToken?.Type == JTokenType.Integer
                ? completeToken.Value<int?>()
                : null;

            if (!seq.HasValue || seq.Value <= 0 || isComplete != 1)
            {
                _logger.LogWarning(
                    "TaskCP 缺少有效 Seq 或 Data.IsComplete != 1，DeviceId={DeviceId}, Payload={Payload}",
                    deviceId,
                    payload);
                return;
            }

            await _groupControl.RecordTaskCompletionAsync(deviceId, seq.Value);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(
                ex,
                "TaskCP JSON 非法，DeviceId={DeviceId}, Payload={Payload}",
                deviceId,
                payload);
        }
        catch (ArgumentException ex)
        {
            _logger.LogWarning(
                ex,
                "TaskCP 设备或字段非法，DeviceId={DeviceId}, Payload={Payload}",
                deviceId,
                payload);
        }
    }
}

public sealed class GroupControlAgingStageHandler : IMqttMessageHandler
{
    private readonly GroupControlService _groupControl;
    private readonly ILogger<GroupControlAgingStageHandler> _logger;

    public GroupControlAgingStageHandler(
        GroupControlService groupControl,
        ILogger<GroupControlAgingStageHandler> logger)
    {
        _groupControl = groupControl;
        _logger = logger;
    }

    public bool CanHandle(string topic) => MqttTopic.IsGroupControlAgingStage(topic);

    public async Task HandleAsync(MqttApplicationMessage message)
    {
        var payload = Encoding.UTF8.GetString(message.Payload);
        try
        {
            var json = JObject.Parse(payload);
            var seqToken = json["Seq"];
            var stageToken = json.SelectToken("Data.AgingStage");
            var agingNumberToken = json.SelectToken("Data.AgingNumber");
            var seq = seqToken?.Type == JTokenType.Integer ? seqToken.Value<long?>() : null;
            var stage = stageToken?.Type == JTokenType.String ? stageToken.Value<string>() : null;
            var agingNumber = agingNumberToken?.Type == JTokenType.String
                ? agingNumberToken.Value<string>()
                : null;

            if (!seq.HasValue || seq.Value <= 0 || string.IsNullOrWhiteSpace(stage) ||
                string.IsNullOrWhiteSpace(agingNumber))
            {
                _logger.LogWarning(
                    "agingStage 缺少有效 Seq、Data.AgingStage 或 Data.AgingNumber，Payload={Payload}",
                    payload);
                return;
            }

            await _groupControl.HandleAgingStageAsync(
                RedisKeys.NormalizeAgingNumber(agingNumber),
                stage,
                seq.Value);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "agingStage JSON 非法，Payload={Payload}", payload);
        }
        catch (ArgumentException ex)
        {
            _logger.LogWarning(ex, "agingStage 取值非法，Payload={Payload}", payload);
        }
    }
}
