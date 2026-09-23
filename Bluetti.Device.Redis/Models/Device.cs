namespace Bluetti.Device.Redis.Models;

/// <summary>
/// 一个子节点就是一个独立老化控制点。对应 Redis Hash：
/// lgw:device:{deviceId}:runtime。
/// </summary>
public sealed class DeviceNode
{
    public string DeviceId { get; set; } = string.Empty;
    public bool Online { get; set; }
    public string? DeviceType { get; set; }
    public string? AgingNumber { get; set; }
    public DateTime? FirstSeenUtc { get; set; }
    public DateTime? LastSeenUtc { get; set; }
    public string? LastSource { get; set; }
    public string? AgingState { get; set; }
    public string? CurrentPn { get; set; }
    public int? CurrentStep { get; set; }
    public long? LastMessageSeq { get; set; }
}
