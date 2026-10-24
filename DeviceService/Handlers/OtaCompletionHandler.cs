using Bluetti.Device.Redis;
using DeviceService.Mqtt;
using DeviceService.Services;
using MQTTnet;
using Newtonsoft.Json.Linq;
using System.Text;

namespace DeviceService.Handlers;

/// <summary>
/// 处理子节点固件下载完成上报：device/{deviceId}/event/OTACP。
///
/// payload 形如：
/// <c>{"Seq":1782373476,"Data":{"IsComplete":1,"AgingNumber":"2"}}</c>
///
/// 这是多点位 OTA 升级的**完成判据**（2026-10-22 起）：
/// 新固件升级不再重启，版本号可能不变，因此不再用"版本号变化"推断成功，
/// 改由设备下载完成后主动上报。
///
/// 注意 payload 里的 Seq 是**设备自己的消息序号**，不是 ota 指令 Seq，
/// 所以批次归属由 OtaUpgradeService 按 deviceId 反查该架活跃批次。
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
        if (!MqttTopic.TryGetOtaCompletionDeviceId(message.Topic, out var rawDeviceId))
            return;

        var payload = Encoding.UTF8.GetString(message.Payload);

        JObject json;
        try
        {
            json = JObject.Parse(payload);
        }
        catch (Newtonsoft.Json.JsonException ex)
        {
            _logger.LogWarning(
                ex,
                "OTACP 上报不是合法 JSON，Topic={Topic}, Payload={Payload}",
                message.Topic,
                payload);
            return;
        }

        var isComplete = json.SelectToken("Data.IsComplete")?.Value<int?>();
        if (isComplete != 1)
        {
            // 非 1 视为未完成（下载失败/进行中），不标记。
            _logger.LogInformation(
                "OTACP 上报未完成，DeviceId={DeviceId}, IsComplete={IsComplete}",
                rawDeviceId,
                isComplete);
            return;
        }

        string? agingNumber = null;
        var token = json.SelectToken("Data.AgingNumber");
        if (token?.Type == JTokenType.String)
        {
            // 架号非法时按 null 处理，由服务按设备 runtime 反查。
            try
            {
                agingNumber = RedisKeys.NormalizeAgingNumber(token.Value<string>()!);
            }
            catch (ArgumentException ex)
            {
                _logger.LogWarning(
                    ex,
                    "OTACP 上报的架号非法，改为按设备 runtime 反查，DeviceId={DeviceId}, AgingNumber={AgingNumber}",
                    rawDeviceId,
                    token.Value<string>());
            }
        }

        await _otaUpgrade.MarkDeviceUpgradedAsync(rawDeviceId, agingNumber);
    }
}
