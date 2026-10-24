using Bluetti.Device.Redis;
using DeviceService.Mqtt;
using DeviceService.Services;
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
    private readonly DeviceLifecycleService _lifecycle;
    private readonly ILogger<DeviceOfflineHandler> _logger;

    public DeviceOfflineHandler(
        DeviceRepository devices,
        DeviceLifecycleService lifecycle,
        ILogger<DeviceOfflineHandler> logger)
    {
        _devices = devices;
        _lifecycle = lifecycle;
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

            var previous = await _devices.GetAsync(deviceId);

            // 离线处理已下沉到 DeviceLifecycleService：在线状态对账复用同一条路径，
            // 保证"清理在线列表 / OTA 分层清单 / 群控成员"三步不会被漏做。
            var removed = await _lifecycle.MarkOfflineAsync(deviceId, deviceType, seq, payload);

            _logger.LogInformation(
                "收到下线遗嘱消息，DeviceId={DeviceId}, AgingNumber={AgingNumber}, RemovedFromOnlineList={Removed}",
                deviceId,
                previous?.AgingNumber ?? "<empty>",
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
