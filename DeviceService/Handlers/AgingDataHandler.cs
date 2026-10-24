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
            var notDuplicate = await _devices.SaveAgingDataAsync(
                deviceId,
                json.SelectToken("Data.PN")?.Value<string>(),
                json.SelectToken("Data.CurrentStep")?.Value<int?>(),
                json.SelectToken("Data.IDNUM")?.Value<long?>(),
                json.SelectToken("Data.TIMESTAMP")?.Value<long?>(),
                json["Seq"]?.Value<long?>(),
                payload);

            // 纯观测：重复到达只记数，数据照常入库、照常上报。
            // 用来判断 IDNUM 是否会重复、新一轮是否从 0 开始——
            // 这决定了将来（改 QoS 1 之后）的去重键该怎么选。
            if (!notDuplicate)
            {
                _logger.LogInformation(
                    "老化采样疑似重复到达（观测，数据仍照常入库），DeviceId={DeviceId}, IDNUM={IdNum}, Seq={Seq}",
                    deviceId,
                    json.SelectToken("Data.IDNUM")?.Value<long?>(),
                    json["Seq"]?.Value<long?>());
            }

            var jsonsend = new { payload = JToken.Parse(payload) };
            var rtxt = await DataSQLHelper.JsonHttpPostAsync(agingDataURL.SaveDataUrl, jsonsend);

            // N2 最小改造：原先完全不检查返回值，HTTP 失败（超时/非 2xx）被静默吞掉。
            // DataSQLHelper 不抛异常，失败时返回以 "false: " 开头的字符串。
            if (rtxt is null || rtxt.StartsWith("false:", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogError(
                    "老化数据上报业务 API 失败，DeviceId={DeviceId}, Seq={Seq}, Result={Result}",
                    deviceId,
                    json["Seq"]?.Value<long?>(),
                    rtxt ?? "<null>");
            }


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


