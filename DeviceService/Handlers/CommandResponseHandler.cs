using Bluetti.Device.Redis;
using DeviceService.Mqtt;
using MQTTnet;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Text;

namespace DeviceService.Handlers;

/// <summary>
/// 四类命令回复处理器的公共实现。
/// 最新协议只接受：
/// device/{id}/cmd/reply/point
/// device/{id}/cmd/reply/state
/// device/{id}/cmd/reply/pn
/// device/{id}/cmd/reply/config
/// </summary>
public abstract class CommandReplyHandlerBase : IMqttMessageHandler
{
    private readonly string _replyType;
    private readonly CommandRepository _commands;
    private readonly DeviceRepository _devices;
    private readonly ILogger _logger;

    protected CommandReplyHandlerBase(
        string replyType,
        CommandRepository commands,
        DeviceRepository devices,
        ILogger logger)
    {
        _replyType = RedisKeys.NormalizeReplyType(replyType);
        _commands = commands;
        _devices = devices;
        _logger = logger;
    }

    public bool CanHandle(string topic)
        => MqttTopic.TryGetCommandReply(topic, out _, out var type) &&
           type.Equals(_replyType, StringComparison.Ordinal);

    public async Task HandleAsync(MqttApplicationMessage message)
    {
        if (!MqttTopic.TryGetCommandReply(message.Topic, out var deviceId, out var replyType))
        {
            _logger.LogWarning("非法命令回复主题：{Topic}", message.Topic);
            return;
        }

        var payload = Encoding.UTF8.GetString(message.Payload);
        try
        {
            var response = JsonConvert.DeserializeObject<CommandReplyPayload>(payload);
            if (response?.Data is null || response.Data.CmdSeq <= 0)
            {
                _logger.LogWarning(
                    "{ReplyType} 回复缺少有效 Data.CmdSeq，DeviceId={DeviceId}, Payload={Payload}",
                    replyType,
                    deviceId,
                    payload);
                return;
            }

            await _devices.SaveCommandReplyAsync(
                deviceId,
                replyType,
                response.Seq,
                response.Data.CmdSeq,
                response.Data.Code,
                response.Data.Msg,
                response.Data.CmdData);

            await _commands.CompleteAsync(
                deviceId,
                response.Data.CmdSeq,
                replyType,
                response.Data.Code,
                response.Data.Msg,
                payload);

            _logger.LogInformation(
                "设备回复已落库，DeviceId={DeviceId}, ReplyType={ReplyType}, CmdSeq={CmdSeq}, Code={Code}",
                deviceId,
                replyType,
                response.Data.CmdSeq,
                response.Data.Code);
        }
        catch (JsonException ex)
        {
            _logger.LogError(
                ex,
                "{ReplyType} 回复 JSON 解析失败，Topic={Topic}, Payload={Payload}",
                _replyType,
                message.Topic,
                payload);
        }
    }
}

public sealed class PointCommandReplyHandler : CommandReplyHandlerBase
{
    public PointCommandReplyHandler(
        CommandRepository commands,
        DeviceRepository devices,
        ILogger<PointCommandReplyHandler> logger)
        : base("point", commands, devices, logger)
    {
    }
}

public sealed class StateCommandReplyHandler : CommandReplyHandlerBase
{
    public StateCommandReplyHandler(
        CommandRepository commands,
        DeviceRepository devices,
        ILogger<StateCommandReplyHandler> logger)
        : base("state", commands, devices, logger)
    {
    }
}

public sealed class PnCommandReplyHandler : CommandReplyHandlerBase
{
    public PnCommandReplyHandler(
        CommandRepository commands,
        DeviceRepository devices,
        ILogger<PnCommandReplyHandler> logger)
        : base("pn", commands, devices, logger)
    {
    }
}

public sealed class ConfigCommandReplyHandler : CommandReplyHandlerBase
{
    public ConfigCommandReplyHandler(
        CommandRepository commands,
        DeviceRepository devices,
        ILogger<ConfigCommandReplyHandler> logger)
        : base("config", commands, devices, logger)
    {
    }
}

public sealed class CommandReplyData
{
    [JsonProperty("CmdSeq")]
    public long CmdSeq { get; set; }

    [JsonProperty("Code")]
    public int Code { get; set; }

    [JsonProperty("Msg")]
    public string? Msg { get; set; }

    [JsonProperty("CmdData")]
    public JToken? CmdData { get; set; }
}

public sealed class CommandReplyPayload : PayloadBase<CommandReplyData>
{
}
