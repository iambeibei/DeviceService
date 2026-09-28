namespace Bluetti.Device.Redis;

/// <summary>
/// Redis 键过期时间统一定义。
///
/// 改过期时间只需改这里：DefaultDays 是唯一的改动点。
///
/// 当前只有两类键设置过期（整键过期，过期后 Redis 惰性+主动抽样删除）：
/// 1. lgw:device:{deviceId}:cmd:{seq}            命令状态 Hash；
/// 2. lgw:device:{deviceId}:aging:{pn}:history   老化采样历史 Stream。
///
/// 以下键刻意不设过期，新增键时不要顺手套用 TTL：
/// - lgw:cmd:seq：自增序列，过期会导致 Seq 回退，命令号重复；
/// - lgw:cmd:queue：命令 Stream，同时承载消费者组 device-service。整键过期会连消费组一起消失，
///   而消费组只在服务启动时创建一次，之后会持续 NOGROUP 报错直到重启；该键靠 MAXLEN 裁剪；
/// - lgw:device:{deviceId}:runtime 与 event:state：CurrenAgingSeq / CurrentAgingRecordId /
///   AgingNumber 的恢复源，过期会让运行中的批次永远无法结束；
/// - 在线列表与设备目录（lgw:devices、devices:last_seen、devices:online*）：由上下线生命周期维护；
/// - lgw:group-control:racks 与 lgw:group-control:{n}:state：架注册表与架当前状态，过期等于整架重置。
/// </summary>
public static class RedisTtl
{
    /// <summary>
    /// 统一过期天数。修改此值即可调整所有纳入统一过期策略的键。
    /// </summary>
    public const int DefaultDays = 7;

    public static readonly TimeSpan Default = TimeSpan.FromDays(DefaultDays);

    /// <summary>
    /// 单条命令状态：lgw:device:{deviceId}:cmd:{seq}。
    /// 注意：该值不应短于一次老化任务的最长时长，否则超长任务结束时命令 Hash 已过期，
    /// 只能依赖 runtime.CurrentAgingRecordId 回退完成状态回调。
    /// </summary>
    public static readonly TimeSpan Command = Default;

    /// <summary>
    /// 老化采样历史：lgw:device:{deviceId}:aging:{pn}:history。
    /// 每次采样写入都会刷新过期时间；某 PN 停止采样超过该天数后整条历史会被删除。
    /// </summary>
    public static readonly TimeSpan DeviceAgingHistory = Default;
}
