namespace Bluetti.Device.Redis.Models;

/// <summary>
/// 一条 ota 指令在某个老化架上的分层升级批次，对应 lgw:ota:{AgingNumber}:run:{Seq}:state。
///
/// 按 Mesh 层级从 1 开始逐层下发，每层等设备上报 OTACP（固件下载完成）
/// 后再等一个固定延时进入下一层。
/// 注意：2026-10-22 起新固件升级**不再重启**，因此不再用"版本号变化"判断成功，
/// 改为设备主动上报 OTACP；版本基线与 mesh 稳定计数判定已删除。
/// </summary>
public sealed class OtaBatchState
{
    public string AgingNumber { get; set; } = string.Empty;

    /// <summary>ota 指令 Seq，同时是该批次的批次号。</summary>
    public long Seq { get; set; }

    public string Status { get; set; } = OtaBatchStatuses.Active;

    /// <summary>当前正在处理的 Mesh 层级，从 1 开始。</summary>
    public int CurrentLevel { get; set; } = 1;

    /// <summary>
    /// 当前层内正在处理的小组序号（1 起）。每层按 `LevelGroupSize` 把该层设备切成若干组，
    /// 一组全部处理完才下发下一组；本层所有组都处理完才进入下一层。
    /// 进入新层时重置为 1（见 <c>AdvanceLevelAsync</c>）。
    /// </summary>
    public int CurrentGroupIndex { get; set; } = 1;

    public string LevelStatus { get; set; } = OtaLevelStatuses.Pending;

    public DateTime? LevelStartedUtc { get; set; }

    /// <summary>本层确认超时时刻。持久化后服务重启继续有效。</summary>
    public DateTime? LevelDeadlineUtc { get; set; }

    /// <summary>
    /// 层间 mesh 稳定等待的截止时刻。上一层升级重启会让下层设备短暂掉线，
    /// 等设备重新上线回到分层清单后再取下一层交集，否则会被误判成"不在名单里"。
    /// 到达该时刻仍未稳定就直接进入下一层。
    /// </summary>
    public DateTime? StabilizeUntilUtc { get; set; }

    /// <summary>
    /// 当前小组已补发（重发 ota 指令）的次数。每 `ResendIntervalMinutes` 分钟补发一次，
    /// 进入新小组时清零。用于在日志里标记"这是第几次补发"。
    /// </summary>
    public int ResendCount { get; set; }

    /// <summary>
    /// 下一次补发的时刻。当前小组内有设备迟迟不上报 OTACP 时，
    /// 到点就把升级指令重发一次（payload 里的 Seq 不变，设备按 Seq 幂等）。
    /// </summary>
    public DateTime? NextResendUtc { get; set; }

    /// <summary>
    /// 组间延时的截止时刻。一组处理完后要等 `GroupDelaySeconds`（默认 5 秒）才下发下一组，
    /// 给设备端留一点收尾时间。本层最后一组不受影响（直接走层间延时）。
    /// </summary>
    public DateTime? GroupDelayUntilUtc { get; set; }

    /// <summary>
    /// `Restarting` 阶段的等待截止时刻。窗口内剩余设备全部升级完成就立即广播重启；
    /// 到达该时刻仍有未完成的设备，也照常广播重启（不再继续等）。
    /// </summary>
    public DateTime? RestartDeadlineUtc { get; set; }

    // 已删除 ExpectedOnlineCount（mesh 稳定基准在线数）：
    // 新固件升级后设备不再重启，不会掉线、mesh 不会重组，在线数始终恒定，
    // "回到基准即稳定"的判定恒为真、等于没有。层间改为固定 LevelStabilizeSeconds 延时。

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

    /// <summary>
    /// 所有 Mesh 层级都已走完，正在对"从未被下发过"的设备做兜底补发。
    /// 节点重启后 MeshLevel 可能上移，分层下发覆盖不到它们，补发完即收尾。
    /// </summary>
    public const string Finalizing = "finalizing";

    /// <summary>
    /// 补发结束、但仍有没有升级完成的设备：等它们补齐后再广播重启。
    /// 窗口内全部完成就立即重启；窗口到期仍有未完成也照常重启（不再等）。
    /// 配置项 `RestartWaitTimeoutMinutes`（默认 20 分钟）。
    /// </summary>
    public const string Restarting = "restarting";
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
