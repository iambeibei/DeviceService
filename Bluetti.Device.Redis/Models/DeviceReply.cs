using Newtonsoft.Json.Linq;

namespace Bluetti.Device.Redis.Models;

/// <summary>
/// 设备命令回复最新值模型。
/// 对应：lgw:device:{deviceId}:reply:{point|state|pn|config}。
/// </summary>
public sealed class DeviceReply
{
    public string DeviceId { get; set; } = string.Empty;
    public string ReplyType { get; set; } = string.Empty;
    public long? Seq { get; set; }
    public long CmdSeq { get; set; }
    public int Code { get; set; }
    public string? Message { get; set; }
    public JToken? CmdData { get; set; }
    public string Payload { get; set; } = string.Empty;
    public DateTime ReceivedAtUtc { get; set; }
}
