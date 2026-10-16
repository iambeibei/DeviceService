namespace Bluetti.Device.Redis.Models;

/// <summary>
/// 一条 ota 指令在某个老化架上的升级批次状态，对应 lgw:ota:{AgingNumber}:run:{Seq}:state。
/// PendingRestart 保存已分配 Seq、但尚未确认发布成功的重启指令 JSON，重启后必须复用同一 Seq。
/// </summary>
public sealed class OtaBatchState
{
    public string AgingNumber { get; set; } = string.Empty;

    /// <summary>OTA 指令 Seq，同时是该批次的批次号。</summary>
    public long Seq { get; set; }

    public string Status { get; set; } = OtaBatchStatuses.Pending;

    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;

    /// <summary>首台设备下发成功的时刻，批次倒计时从这里起算。</summary>
    public DateTime? FirstDispatchUtc { get; set; }

    /// <summary>批次超时时刻。持久化后服务重启倒计时继续有效。</summary>
    public DateTime? DeadlineUtc { get; set; }

    public DateTime? RestartedAtUtc { get; set; }

    public string PendingRestart { get; set; } = string.Empty;

    public DateTime UpdatedAtUtc { get; set; } = DateTime.UtcNow;
}

public static class OtaBatchStatuses
{
    /// <summary>批次已建立，尚无设备下发成功。</summary>
    public const string Pending = "pending";

    /// <summary>已有设备下发成功，等待各设备上报 OTACP。</summary>
    public const string Active = "active";

    /// <summary>已广播 CanRestart，批次结束。</summary>
    public const string Finished = "finished";
}
