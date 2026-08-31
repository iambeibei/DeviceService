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
