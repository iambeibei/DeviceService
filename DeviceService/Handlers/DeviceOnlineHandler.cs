using Bluetti.Device.Redis;
using Bluetti.Device.Redis.Models;
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

        // Mesh 层级来自设备上报，老固件没有这个字段，缺失时只跳过分层清单写入。
        var meshLevelToken = json.SelectToken("Data.MeshLevel");
        var meshLevel = meshLevelToken?.Type == JTokenType.Integer
            ? meshLevelToken.Value<long?>()
            : null;

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
        await _devices.SaveOnlineAsync(deviceId, deviceType, agingNumber, seq, meshLevel);
        await _devices.VersionSaveToRuntime(deviceId, version);

        // 换架或换 Mesh 层级时先清掉旧分组的条目，
        // 否则同一台设备会同时留在两个 lgw:ota:{架}:{层} 清单里。
        await RemovePreviousMeshLevelAsync(deviceId, previous, agingNumber, meshLevel);

        // 按架 + Mesh 层级登记，供 OTA 分层升级取用。
        // 架号或层级缺失/非法时不写入，避免把设备记到错误的分组里。
        if (agingNumber is not null && meshLevel is not null && meshLevel.Value >= 0)
        {
            await _devices.SaveOtaMeshLevelAsync(
                deviceId,
                deviceType,
                agingNumber,
                meshLevel.Value);
        }
        else if (meshLevel is null)
        {
            _logger.LogWarning(
                "上线消息缺少有效的 Data.MeshLevel，不写入 OTA 分层清单，DeviceId={DeviceId}, Payload={Payload}",
                deviceId,
                payload);
        }

        await _groupControl.ReevaluateMembershipAsync(
            previous?.AgingNumber,
            agingNumber);
        
        _logger.LogInformation(
            "节点上线，DeviceId={DeviceId}, Type={DeviceType}, AgingNumber={AgingNumber}, MeshLevel={MeshLevel}",
            deviceId,
            deviceType,
            agingNumber ?? "<empty>",
            meshLevel?.ToString() ?? "<empty>");
    }

    /// <summary>
    /// 上架归属或 Mesh 层级发生变化时，把设备从旧分组清单移除。
    /// </summary>
    private async Task RemovePreviousMeshLevelAsync(
        string deviceId,
        DeviceNode? previous,
        string? agingNumber,
        long? meshLevel)
    {
        if (previous?.MeshLevel is null || string.IsNullOrWhiteSpace(previous.AgingNumber))
            return;

        try
        {
            var previousAgingNumber = RedisKeys.NormalizeAgingNumber(previous.AgingNumber);
            var changed =
                !string.Equals(previousAgingNumber, agingNumber, StringComparison.Ordinal) ||
                previous.MeshLevel.Value != meshLevel;

            if (changed)
                await _devices.RemoveOtaMeshLevelAsync(
                    deviceId,
                    previousAgingNumber,
                    previous.MeshLevel.Value);
        }
        catch (ArgumentException ex)
        {
            _logger.LogWarning(
                ex,
                "旧架号非法，跳过 OTA 分层清单清理，DeviceId={DeviceId}, AgingNumber={AgingNumber}",
                deviceId,
                previous.AgingNumber);
        }
    }
}
