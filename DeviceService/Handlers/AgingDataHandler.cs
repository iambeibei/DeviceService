using Bluetti.Device.Redis;
using DeviceService.Mqtt;
using MQTTnet;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Text;

namespace DeviceService.Handlers;

public sealed class AgingDataHandler : IMqttMessageHandler
{
    private readonly DeviceRepository _devices;
    private readonly ILogger<AgingDataHandler> _logger;

    public AgingDataHandler(DeviceRepository devices, ILogger<AgingDataHandler> logger)
    {
        _devices = devices;
        _logger = logger;
    }

    public bool CanHandle(string topic)
        => MqttTopic.TryGetAgingDataDeviceId(topic, out _);

    public async Task HandleAsync(MqttApplicationMessage message)
    {
        if (!MqttTopic.TryGetAgingDataDeviceId(message.Topic, out var deviceId))
            return;

        var payload = Encoding.UTF8.GetString(message.Payload);
        try
        {
            var json = JObject.Parse(payload);
            await _devices.SaveAgingDataAsync(
                deviceId,
                json.SelectToken("Data.SN")?.Value<string>(),
                json.SelectToken("Data.CurrentStep")?.Value<int?>(),
                json.SelectToken("Data.IDNUM")?.Value<long?>(),
                json.SelectToken("Data.TIMESTAMP")?.Value<long?>(),
                json["Seq"]?.Value<long?>(),
                payload);
        }
        catch (JsonException ex)
        {
            _logger.LogError(
                ex,
                "老化数据 JSON 解析失败，DeviceId={DeviceId}, Payload={Payload}",
                deviceId,
                payload);
        }
        catch (ArgumentException ex)
        {
            _logger.LogError(
                ex,
                "老化数据缺少有效 PN（Data.SN），不写入 Redis，DeviceId={DeviceId}, Payload={Payload}",
                deviceId,
                payload);
        }
    }
}
