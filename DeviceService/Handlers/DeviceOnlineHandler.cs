using Bluetti.Device.Redis;
using DeviceService.Mqtt;
using MQTTnet;
using Newtonsoft.Json.Linq;
using System.Text;

namespace DeviceService.Handlers;

public sealed class DeviceOnlineHandler : IMqttMessageHandler
{
    private readonly DeviceRepository _devices;
    private readonly ILogger<DeviceOnlineHandler> _logger;

    public DeviceOnlineHandler(
        DeviceRepository devices,
        ILogger<DeviceOnlineHandler> logger)
    {
        _devices = devices;
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

        await _devices.SaveOnlineAsync(deviceId, deviceType, seq);
        await _devices.VersionSaveToRuntime(deviceId, version);
        
        _logger.LogInformation(
            "子节点上线，DeviceId={DeviceId}, Type={DeviceType}",
            deviceId,
            deviceType);
    }
}
