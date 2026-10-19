namespace Bluetti.Device.Redis.Models;

/// <summary>
/// 一条 ota 指令在某个老化架上的分层升级批次，对应 lgw:ota:{AgingNumber}:run:{Seq}:state。
///
/// 与旧方案的区别：不再等整架全部下载完成后广播重启，而是按 Mesh 层级从 1 开始逐层下发，
/// 每层靠"设备固件版本号是否变化"判断该层是否重启成功，成功后再进入下一层。
/// 这样根节点（第 1 层）先升级重建 mesh，下面的节点才不会在升级过程中失联。
/// </summary>
public sealed class OtaBatchState
{
    public string AgingNumber { get; set; } = string.Empty;

    /// <summary>ota 指令 Seq，同时是该批次的批次号。</summary>
    public long Seq { get; set; }

    public string Status { get; set; } = OtaBatchStatuses.Active;

    /// <summary>当前正在处理的 Mesh 层级，从 1 开始。</summary>
    public int CurrentLevel { get; set; } = 1;

    public string LevelStatus { get; set; } = OtaLevelStatuses.Pending;

    public DateTime? LevelStartedUtc { get; set; }

    /// <summary>本层确认超时时刻。持久化后服务重启继续有效。</summary>
    public DateTime? LevelDeadlineUtc { get; set; }

    /// <summary>
    /// 层间 mesh 稳定等待的截止时刻。上一层升级重启会让下层设备短暂掉线，
    /// 等设备重新上线回到分层清单后再取下一层交集，否则会被误判成"不在名单里"。
    /// </summary>
    public DateTime? StabilizeUntilUtc { get; set; }

    /// <summary>平台下发的 DeviceInfos 总数，即成功率统计的分母。</summary>
    public int TotalCount { get; set; }

    /// <summary>ota 指令携带的 Data（序列化后的 JSON），下发时原样透传；为空则发 "Data": null。</summary>
    public string DataJson { get; set; } = string.Empty;

    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;

    public DateTime? FinishedAtUtc { get; set; }

    public DateTime UpdatedAtUtc { get; set; } = DateTime.UtcNow;
}

public static class OtaBatchStatuses
{
    public const string Active = "active";
    public const string Finished = "finished";
}

public static class OtaLevelStatuses
{
    /// <summary>本层尚未开始下发。</summary>
    public const string Pending = "pending";

    /// <summary>本层已下发，等待各设备版本号变化。</summary>
    public const string Upgrading = "upgrading";

    /// <summary>本层已结束，正在等待 mesh 稳定后进入下一层。</summary>
    public const string Stabilizing = "stabilizing";
}

/// <summary>
/// 升级名单中每台设备的状态。名单是平台 DeviceInfos 全集，
/// 与分层清单取交集后才真正下发，因此会出现从未下发过的 skipped。
/// </summary>
public static class OtaTargetStatuses
{
    public const string Pending = "pending";
    public const string Upgraded = "upgraded";
    public const string Failed = "failed";
    public const string Skipped = "skipped";
}
