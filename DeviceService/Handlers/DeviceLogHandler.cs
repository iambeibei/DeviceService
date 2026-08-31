using Bluetti.Device.Redis;
using DeviceService.Mqtt;
using MQTTnet;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Text;

namespace DeviceService.Handlers;

public sealed class DeviceLogHandler : IMqttMessageHandler
{
    private readonly DeviceRepository _devices;
    private readonly ILogger<DeviceLogHandler> _logger;

    public DeviceLogHandler(
        DeviceRepository devices,
        ILogger<DeviceLogHandler> logger)
    {
        _devices = devices;
        _logger = logger;
    }

    public bool CanHandle(string topic)
        => MqttTopic.TryGetLogType(topic, out _, out _);

    public async Task HandleAsync(MqttApplicationMessage message)
    {
        if (!MqttTopic.TryGetLogType(message.Topic, out var deviceId, out var logType))
            return;

        var payload = Encoding.UTF8.GetString(message.Payload);
        try
        {
            var json = JObject.Parse(payload);
            await _devices.SaveLogAsync(
                deviceId,
                logType,
                json["Seq"]?.Value<long?>(),
                payload);
        }
        catch (JsonException ex)
        {
            _logger.LogError(
                ex,
                "设备日志 JSON 解析失败，Topic={Topic}, Payload={Payload}",
                message.Topic,
                payload);
        }
    }
}
