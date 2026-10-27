using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Bluetti.Device.Redis.Models;

/// <summary>
/// 管理端写入 Redis，DeviceService 再转发给指定设备的命令。
/// </summary>
public sealed class CmdInfo
{
    public long Seq { get; set; }
    public string DeviceId { get; set; } = string.Empty;
    public string RecordId { get; set; } = string.Empty;
    public string Action { get; set; } = string.Empty;
    public JToken? Data { get; set; }
    public string Status { get; set; } = CommandStates.Queued;
    public string ExpectedReplyType { get; set; } = "point";
    public DateTime CreatedAtUtc { get; set; }
    public DateTime? PublishedAtUtc { get; set; }
    public DateTime? CompletedAtUtc { get; set; }
    public DateTime? TimedOutAtUtc { get; set; }
    public int? ResultCode { get; set; }
    public string? ResultMessage { get; set; }
    public string? ReplyType { get; set; }
    public string? ReplyJson { get; set; }

    /// <summary>
    /// 已用掉的重发次数（总下发次数 = RetryCount + 1）。
    /// 普通命令按 <c>CommandProcessing:TimeoutRetryCount</c> 限制；start_aging 按
    /// <c>CommandProcessing:StartAgingMaxAttempts</c> 限制。
    /// 存在 Redis 命令 Hash 中，服务重启后不会重复计数。
    /// </summary>
    public int RetryCount { get; set; }

    /// <summary>
    /// 最近一次实际下发使用的 MQTT Seq。start_aging 每次重发都会分配新的下发 Seq，
    /// 逻辑命令 Seq 保持不变，因此需要单独记录最后一次真正发出去的 Seq 便于排查。
    /// 未重发过时等于 <see cref="Seq"/>。
    /// </summary>
    public long LastAttemptSeq { get; set; }

    [JsonIgnore]
    public string StatusKey => RedisKeys.DeviceCommandStatus(DeviceId, Seq);
}

public static class CommandStates
{
    public const string Queued = "queued";
    public const string Published = "published";
    public const string Completed = "completed";
    public const string Failed = "failed";
    public const string TimedOut = "timed_out";

    public static bool IsTerminal(string? status)
        => string.Equals(status, Completed, StringComparison.OrdinalIgnoreCase) ||
           string.Equals(status, Failed, StringComparison.OrdinalIgnoreCase) ||
           string.Equals(status, TimedOut, StringComparison.OrdinalIgnoreCase);
}

/// <summary>
/// 平台指令与四类设备回复主题的固定映射。
/// </summary>
public static class CommandReplyTypeResolver
{
    public static string ForAction(string action)
    {
        var value = action.Trim().ToLowerInvariant();
        return value switch
        {
            "get_state" => "state",
            "set_pn" => "pn",
            "get_config" => "config",
            _ => "point"
        };
    }
}
