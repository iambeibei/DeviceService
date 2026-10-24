using Bluetti.Device.Redis;
using Bluetti.Device.Redis.Models;

namespace DeviceService.Services;

/// <summary>
/// 设备下线的统一入口。
///
/// 把 DeviceOfflineHandler 里的离线处理逻辑下沉到这里，是为了让"在线状态对账"
/// 也能复用同一条路径。离线不是只删一个列表，它必须同时完成：
/// ① SaveOfflineAsync（全量在线列表 + 按架列表 + 按架 OTA 列表 + runtime.Online=false）
/// ② 从「架 + Mesh 层级」清单移除（否则 OTA 分层清单残留已离线节点）
/// ③ 群控：移出阶段屏障成员并重估该架
/// 少做任何一步都会留下不一致状态，所以对账不能绕过这里。
///
/// 依赖说明：本服务与 Handler 同级，可以注入 GroupControlService；
/// 而在线状态对账 Worker 只能注入本服务，不能注入 GroupControlService
/// （那会与 MqttConnection 形成 DI 环）。
/// </summary>
public sealed class DeviceLifecycleService
{
    private readonly DeviceRepository _devices;
    private readonly GroupControlService _groupControl;
    private readonly ILogger<DeviceLifecycleService> _logger;

    public DeviceLifecycleService(
        DeviceRepository devices,
        GroupControlService groupControl,
        ILogger<DeviceLifecycleService> logger)
    {
        _devices = devices;
        _groupControl = groupControl;
        _logger = logger;
    }

    /// <summary>
    /// 执行一次完整的离线处理。
    /// </summary>
    /// <param name="rawPayload">原始报文，仅用于落库追溯；对账场景可传标记文本。</param>
    /// <returns>是否确实从全量在线列表中移除了条目。</returns>
    public async Task<bool> MarkOfflineAsync(
        string deviceId,
        string deviceType,
        long? messageSeq,
        string rawPayload)
    {
        var previous = await _devices.GetAsync(deviceId);

        var removed = await _devices.SaveOfflineAsync(
            deviceId,
            deviceType,
            messageSeq,
            rawPayload);

        // 从「架 + Mesh 层级」清单移除，避免 OTA 分层清单里残留已离线节点。
        if (previous?.MeshLevel is not null &&
            !string.IsNullOrWhiteSpace(previous.AgingNumber))
        {
            try
            {
                await _devices.RemoveOtaMeshLevelAsync(
                    deviceId,
                    RedisKeys.NormalizeAgingNumber(previous.AgingNumber),
                    previous.MeshLevel.Value);
            }
            catch (ArgumentException ex)
            {
                _logger.LogWarning(
                    ex,
                    "架号非法，跳过 OTA 分层清单清理，DeviceId={DeviceId}, AgingNumber={AgingNumber}",
                    deviceId,
                    previous.AgingNumber);
            }
        }

        // 先剔除屏障成员，再重估该架：掉线设备不再上报阶段完成，
        // 若不移出 participants 会永久阻塞整架放行。
        await _groupControl.HandleDeviceOfflineAsync(deviceId);
        await _groupControl.ReevaluateMembershipAsync(
            previous?.AgingNumber,
            currentAgingNumber: null);

        _logger.LogInformation(
            "节点离线，DeviceId={DeviceId}, Type={DeviceType}, AgingNumber={AgingNumber}, RemovedFromOnlineList={Removed}",
            deviceId,
            deviceType,
            previous?.AgingNumber ?? "<empty>",
            removed);

        return removed;
    }

    /// <summary>
    /// 对账场景的"补登记上线"：设备确实在线，但服务停机期间的 up_line 丢了，
    /// 所以它不在任何在线名单里。这里补一次完整的上线写入并重估群控成员。
    /// </summary>
    /// <returns>是否完成补登记；runtime 缺 DeviceType 时返回 false（类型不可臆造）。</returns>
    public async Task<bool> RecoverOnlineAsync(string deviceId, DeviceNode? node)
    {
        if (node is null || string.IsNullOrWhiteSpace(node.DeviceType))
        {
            _logger.LogWarning(
                "对账发现设备在线但 runtime 缺少 DeviceType，无法恢复在线名单，DeviceId={DeviceId}",
                deviceId);
            return false;
        }

        await _devices.SaveOnlineAsync(
            deviceId,
            node.DeviceType,
            node.AgingNumber,
            messageSeq: null,
            meshLevel: node.MeshLevel);

        // 与正常 up_line 一致：补登记后重估该架成员，让群控把这台设备算回去。
        await _groupControl.ReevaluateMembershipAsync(
            previousAgingNumber: null,
            currentAgingNumber: node.AgingNumber);

        _logger.LogWarning(
            "对账恢复设备在线（服务停机期间的 up_line 已丢失），DeviceId={DeviceId}, Type={DeviceType}, AgingNumber={AgingNumber}, MeshLevel={MeshLevel}",
            deviceId,
            node.DeviceType,
            node.AgingNumber ?? "<empty>",
            node.MeshLevel?.ToString() ?? "<empty>");

        return true;
    }
}
