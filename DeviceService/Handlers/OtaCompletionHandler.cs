using Bluetti.Device.Redis;
using DeviceService.Mqtt;
using DeviceService.Services;
using MQTTnet;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Text;

namespace DeviceService.Handlers;

/// <summary>
/// 处理子节点 OTA 下载完成上报：device/{deviceId}/event/OTACP。
///
/// 报文示例：{"Seq":1782373476,"Data":{"IsComplete":1,"AgingNumber":"2"}}
/// 注意这里的 Seq 是设备自身消息序号，不是 ota 指令 Seq，
/// 因此批次归属由 OtaUpgradeService 按 deviceId 反查，不使用本报文的 Seq。
/// </summary>
public sealed class OtaCompletionHandler : IMqttMessageHandler
{
    private readonly OtaUpgradeService _otaUpgrade;
    private readonly ILogger<OtaCompletionHandler> _logger;

    public OtaCompletionHandler(
        OtaUpgradeService otaUpgrade,
        ILogger<OtaCompletionHandler> logger)
    {
        _otaUpgrade = otaUpgrade;
        _logger = logger;
    }

    public bool CanHandle(string topic)
        => MqttTopic.TryGetOtaCompletionDeviceId(topic, out _);

    public async Task HandleAsync(MqttApplicationMessage message)
    {
        if (!MqttTopic.TryGetOtaCompletionDeviceId(message.Topic, out var deviceId))
            return;

        var payload = Encoding.UTF8.GetString(message.Payload);

        try
        {
            var json = JObject.Parse(payload);
            var completeToken = json.SelectToken("Data.IsComplete");
            var agingNumberToken = json.SelectToken("Data.AgingNumber");

            // 只认 IsComplete == 1：其它取值一律视为未完成，继续等批次超时兜底。
            var isComplete = completeToken?.Type == JTokenType.Integer &&
                             completeToken.Value<int?>() == 1;
            var reportedAgingNumber = agingNumberToken?.Type == JTokenType.String
                ? agingNumberToken.Value<string>()
                : null;

            await _otaUpgrade.RecordCompletionAsync(
                RedisKeys.Normalize(deviceId),
                isComplete,
                reportedAgingNumber);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(
                ex,
                "OTACP 上报不是合法 JSON，DeviceId={DeviceId}, Payload={Payload}",
                deviceId,
                payload);
        }
        catch (ArgumentException ex)
        {
            _logger.LogWarning(
                ex,
                "OTACP 上报设备或字段非法，DeviceId={DeviceId}, Payload={Payload}",
                deviceId,
                payload);
        }
    }
}
