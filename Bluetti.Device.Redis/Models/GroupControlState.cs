namespace Bluetti.Device.Redis.Models;

/// <summary>
/// 单个老化架的群控可恢复状态，对应 lgw:group-control:{AgingNumber}:state。
/// Pending 字段保存已经分配固定 Seq、但尚未确认发布成功的 MQTT 指令 JSON。
/// </summary>
public sealed class GroupControlState
{
    public string AgingNumber { get; set; } = string.Empty;
    public int Mode { get; set; } = 1;
    public long Generation { get; set; }
    public long? LastModeSeq { get; set; }
    public long? CurrentRunSeq { get; set; }
    public string CurrentStage { get; set; } = string.Empty;
    public string LastAppliedStage { get; set; } = string.Empty;
    public string LastAppliedControllerId { get; set; } = string.Empty;
    public string PendingNextStep { get; set; } = string.Empty;
    public string PendingPower { get; set; } = string.Empty;
    public DateTime UpdatedAtUtc { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// 一批 start_aging 在某个老化架上的群控运行状态。
/// RunSeq 直接复用 Redis Stream 批量命令 Seq，不改变下位机协议。
/// </summary>
public sealed class GroupControlRunState
{
    public string AgingNumber { get; set; } = string.Empty;
    public long RunSeq { get; set; }
    public string Status { get; set; } = GroupControlRunStatuses.Candidate;
    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
    public DateTime? ActivatedAtUtc { get; set; }
    public DateTime? FinishedAtUtc { get; set; }
    public DateTime UpdatedAtUtc { get; set; } = DateTime.UtcNow;
}

public static class GroupControlRunStatuses
{
    public const string Candidate = "candidate";
    public const string Active = "active";
    public const string Finished = "finished";
    public const string Conflict = "conflict";
    public const string Cancelled = "cancelled";
}

/// <summary>
/// 群控服务直接发布的 MQTT 指令。持久化后重试时必须复用相同 Seq。
/// </summary>
public sealed class PendingGroupControlCommand
{
    public long Seq { get; set; }
    public string Payload { get; set; } = string.Empty;
    public string Stage { get; set; } = string.Empty;
}
