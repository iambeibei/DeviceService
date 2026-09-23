using Bluetti.Device.Redis;
using DeviceService.Mqtt;
using DeviceService.Services;
using MQTTnet;
using Newtonsoft.Json.Linq;
using System.Text;

namespace DeviceService.Handlers;

public sealed class DeviceOnlineHandler : IMqttMessageHandler
{
    private readonly DeviceRepository _devices;
    private readonly GroupControlService _groupControl;
    private readonly ILogger<DeviceOnlineHandler> _logger;

    public DeviceOnlineHandler(
        DeviceRepository devices,
        GroupControlService groupControl,
        ILogger<DeviceOnlineHandler> logger)
    {
        _devices = devices;
        _groupControl = groupControl;
        _logger = logger;
    }

    public bool CanHandle(string topic)
        => topic.Equals("device/public/up_line", StringComparison.OrdinalIgnoreCase);

    public async Task HandleAsync(MqttApplicationMessage message)
    {
        var payload = Encoding.UTF8.GetString(message.Payload);
        var json = JObject.Parse(payload);
        var deviceId = json.SelectToken("Data.id")?.Value<string>();
        var deviceType = json.SelectToken("Data.type")?.Value<string>();
        var version = json.SelectToken("Data.Version")?.Value<string>();
        var seq = json["Seq"]?.Value<long?>();
        var agingNumberToken = json.SelectToken("Data.AgingNumber");
        string? agingNumber = null;
        if (agingNumberToken?.Type == JTokenType.String)
        {
            try
            {
                agingNumber = RedisKeys.NormalizeAgingNumber(
                    agingNumberToken.Value<string>()!);
            }
            catch (ArgumentException)
            {
                // 非法架号仍允许设备进入全量在线列表。
            }
        }

        if (string.IsNullOrWhiteSpace(deviceId))
        {
            _logger.LogWarning("上线消息缺少 Data.id：{Payload}", payload);
            return;
        }

        if (string.IsNullOrWhiteSpace(deviceType))
        {
            _logger.LogWarning("上线消息缺少 Data.type：{Payload}", payload);
            return;
        }

        if (string.IsNullOrWhiteSpace(version))
        {
            _logger.LogWarning("上线消息缺少 Data.Version：{Payload}", payload);
            return;
        }

        if (agingNumber is null)
        {
            _logger.LogWarning(
                "上线消息缺少有效 Data.AgingNumber，设备只进入全量在线列表且不参与群控：{Payload}",
                payload);
        }

        var previous = await _devices.GetAsync(deviceId);
        await _devices.SaveOnlineAsync(deviceId, deviceType, agingNumber, seq);
        await _devices.VersionSaveToRuntime(deviceId, version);
        await _groupControl.ReevaluateMembershipAsync(
            previous?.AgingNumber,
            agingNumber);
        
        _logger.LogInformation(
            "节点上线，DeviceId={DeviceId}, Type={DeviceType}, AgingNumber={AgingNumber}",
            deviceId,
            deviceType,
            agingNumber ?? "<empty>");
    }
}
