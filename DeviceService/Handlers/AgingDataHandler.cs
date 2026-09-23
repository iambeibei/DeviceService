using Bluetti.Device.Redis;
using DeviceService.Mqtt;
using DeviceService.Services;
using MQTTnet;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Text;

namespace DeviceService.Handlers;

public class AgingDataApi
{
    public string? SaveDataUrl {  get; set; }
}
public sealed class AgingDataHandler : IMqttMessageHandler
{
    private readonly DeviceRepository _devices;
    private readonly ILogger<AgingDataHandler> _logger;

    private readonly AgingDataApi agingDataURL;

    public AgingDataHandler(DeviceRepository devices, ILogger<AgingDataHandler> logger, IConfiguration config)
    {
        _devices = devices;
        _logger = logger;
        agingDataURL= config.GetSection("AgingDataApi").Get<AgingDataApi>()?? throw new InvalidOperationException("AgingDataApi not configured");
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
                json.SelectToken("Data.PN")?.Value<string>(),
                json.SelectToken("Data.CurrentStep")?.Value<int?>(),
                json.SelectToken("Data.IDNUM")?.Value<long?>(),
                json.SelectToken("Data.TIMESTAMP")?.Value<long?>(),
                json["Seq"]?.Value<long?>(),
                payload);

            var jsonsend = new { payload = JToken.Parse(payload) };
            var rtxt= await DataSQLHelper.JsonHttpPostAsync(agingDataURL.SaveDataUrl, jsonsend);
            //_logger.LogInformation(rtxt);


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
                "老化数据缺少有效 PN（Data.PN），不写入 Redis，DeviceId={DeviceId}, Payload={Payload}",
                deviceId,
                payload);
        }
    }
}


