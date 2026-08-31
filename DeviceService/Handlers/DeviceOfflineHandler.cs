using Bluetti.Device.Redis;
using DeviceService.Mqtt;
using MQTTnet;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Text;

namespace DeviceService.Handlers;

/// <summary>
/// 处理子节点 MQTT 遗嘱消息：device/public/down_line。
/// </summary>
public sealed class DeviceOfflineHandler : IMqttMessageHandler
{
    private readonly DeviceRepository _devices;
    private readonly ILogger<DeviceOfflineHandler> _logger;

    public DeviceOfflineHandler(
        DeviceRepository devices,
        ILogger<DeviceOfflineHandler> logger)
    {
        _devices = devices;
        _logger = logger;
    }

    public bool CanHandle(string topic)
        => topic.Equals(
            "device/public/down_line",
            StringComparison.OrdinalIgnoreCase);

    public async Task HandleAsync(MqttApplicationMessage message)
    {
        var payload = Encoding.UTF8.GetString(message.Payload);

        try
        {
            var json = JObject.Parse(payload);
            var deviceId = json.SelectToken("Data.id")?.Value<string>();
            var deviceType = json.SelectToken("Data.type")?.Value<string>();
            var seq = json["Seq"]?.Value<long?>();

            if (string.IsNullOrWhiteSpace(deviceId))
            {
                _logger.LogWarning(
                    "离线遗嘱消息缺少 Data.id：{Payload}",
                    payload);
                return;
            }

            if (string.IsNullOrWhiteSpace(deviceType))
            {
                _logger.LogWarning(
                    "离线遗嘱消息缺少 Data.type：{Payload}",
                    payload);
                return;
            }

            var removed = await _devices.SaveOfflineAsync(
                deviceId,
                deviceType,
                seq,
                payload);

            _logger.LogInformation(
                "子节点离线，DeviceId={DeviceId}, Type={DeviceType}, RemovedFromOnlineList={Removed}",
                deviceId,
                deviceType,
                removed);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(
                ex,
                "离线遗嘱消息不是合法 JSON：{Payload}",
                payload);
        }
    }
}
