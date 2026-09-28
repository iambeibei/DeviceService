namespace Bluetti.Device.Redis;

/// <summary>
/// Redis 键统一定义。设备数据均以 DeviceId 作为隔离维度。
/// 在线结构：
/// 1. lgw:devices:online 为全量 List，元素为 {"id":"...","type":"...","AgingNumber":"0"|null}
/// 2. lgw:devices:online:aging:{AgingNumber} 为各架 type=1 点位 List；online:control 为 type=2 总控 List。
/// 3. lgw:device:{deviceId}:runtime 为设备运行状态 Hash。
/// </summary>
public static class RedisKeys
{
    public const string Prefix = "lgw";

    //消息频道
    public const string channelFault = "event:fault"; // 故障频道
    public const string channelWarn = "event:warn";   // 告警频道


    // 设备目录及在线列表。
    public const string DeviceSet = $"{Prefix}:devices";
    public const string DeviceLastSeen = $"{Prefix}:devices:last_seen";
    public const string OnlineDeviceList = $"{Prefix}:devices:online";
    public const string ControlOnlineDeviceList = $"{Prefix}:devices:online:control";

    public static string AgingRackOnlineDeviceList(string agingNumber)
        => $"{Prefix}:devices:online:aging:{NormalizeAgingNumber(agingNumber)}";

    // 命令基础设施。
    public const string CommandSequence = $"{Prefix}:cmd:seq";
    public const string CommandQueue = $"{Prefix}:cmd:queue";

    // 多老化架群控状态。旧版无架号键保留在 Redis 中，但新代码不读取也不删除。
    public const string GroupControlRacks = $"{Prefix}:group-control:racks";

    public static string GroupControlState(string agingNumber)
        => $"{Prefix}:group-control:{NormalizeAgingNumber(agingNumber)}:state";

    public static string GroupControlCompletedDevices(string agingNumber)
        => $"{Prefix}:group-control:{NormalizeAgingNumber(agingNumber)}:completed";

    public static string GroupControlLastTaskSeq(string agingNumber)
        => $"{Prefix}:group-control:{NormalizeAgingNumber(agingNumber)}:taskcp:last-seq";

    public static string GroupControlRunState(string agingNumber, long runSeq)
    {
        if (runSeq <= 0)
            throw new ArgumentOutOfRangeException(nameof(runSeq), "群控批次序号必须大于 0");

        return $"{Prefix}:group-control:{NormalizeAgingNumber(agingNumber)}:run:{runSeq}:state";
    }

    public static string GroupControlRunParticipants(string agingNumber, long runSeq)
        => $"{GroupControlRunRoot(agingNumber, runSeq)}:participants";

    public static string GroupControlRunStartedDevices(string agingNumber, long runSeq)
        => $"{GroupControlRunRoot(agingNumber, runSeq)}:started";

    public static string GroupControlRunCompletedDevices(string agingNumber, long runSeq)
        => $"{GroupControlRunRoot(agingNumber, runSeq)}:completed";

    public static string GroupControlRunTerminalDevices(string agingNumber, long runSeq)
        => $"{GroupControlRunRoot(agingNumber, runSeq)}:terminal";

    /// <summary>
    /// 因阶段超时被剔除出当前批次的设备。仅用于审计与展示，不参与任何放行判定。
    /// </summary>
    public static string GroupControlRunExcludedDevices(string agingNumber, long runSeq)
        => $"{GroupControlRunRoot(agingNumber, runSeq)}:excluded";

    // 原有全局设备事件流，仅用于保持上线/下线处理格式。
    public const string DeviceEventStream = $"{Prefix}:device:events";

    /// <summary>
    /// 原有设备运行状态路径。
    /// </summary>
    public static string DeviceRuntime(string deviceId)
        => $"{Prefix}:device:{Normalize(deviceId)}:runtime";

    /// <summary>
    /// 设备命名空间前缀。该值主要用于拼接子键，本身不要求单独建立 Redis Key。
    /// </summary>
    public static string DeviceRoot(string deviceId)
        => $"{Prefix}:device:{Normalize(deviceId)}";

    // 老化数据。按设备 + PN 隔离，PN 来自 device/{id}/data/aging 的 Data.SN。
    public static string DeviceAgingLatest(string deviceId, string pn)
        => $"{DeviceRoot(deviceId)}:aging:{NormalizePn(pn)}:latest";

    public static string DeviceAgingHistory(string deviceId, string pn)
        => $"{DeviceRoot(deviceId)}:aging:{NormalizePn(pn)}:history";

    // 设备事件：state / fault / warn。
    public static string DeviceEventLatest(string deviceId, string eventType)
        => $"{DeviceRoot(deviceId)}:event:{NormalizeSegment(eventType, nameof(eventType))}";

    public static string DeviceEventHistory(string deviceId, string eventType)
        => $"{DeviceEventLatest(deviceId, eventType)}:history";

    // 设备日志：runtime / error / local。
    public static string DeviceLogLatest(string deviceId, string logType)
        => $"{DeviceRoot(deviceId)}:log:{NormalizeSegment(logType, nameof(logType))}";

    public static string DeviceLogHistory(string deviceId, string logType)
        => $"{DeviceLogLatest(deviceId, logType)}:history";

    // 四类命令回复：point / state / pn / config。
    public static string DeviceReplyLatest(string deviceId, string replyType)
        => $"{DeviceRoot(deviceId)}:reply:{NormalizeReplyType(replyType)}";

    public static string DeviceReplyHistory(string deviceId, string replyType)
        => $"{DeviceReplyLatest(deviceId, replyType)}:history";

    // 单条下发命令状态归属对应设备。
    public static string DeviceCommandStatus(string deviceId, long cmdSeq)
    {
        if (cmdSeq <= 0)
            throw new ArgumentOutOfRangeException(nameof(cmdSeq), "命令序号必须大于 0");

        return $"{DeviceRoot(deviceId)}:cmd:{cmdSeq}";
    }

    public static string NormalizeReplyType(string replyType)
    {
        var value = NormalizeSegment(replyType, nameof(replyType));
        return value switch
        {
            "point" or "state" or "pn" or "config" => value,
            _ => throw new ArgumentException(
                "ReplyType 只允许 point、state、pn、config",
                nameof(replyType))
        };
    }


    public static string NormalizePn(string pn)
    {
        if (string.IsNullOrWhiteSpace(pn))
            throw new ArgumentException("PN 不能为空", nameof(pn));

        var value = pn.Trim();
        if (value.Contains(':') || value.Contains('/') || value.Any(char.IsWhiteSpace))
            throw new ArgumentException("PN 不能包含冒号、斜杠或空白字符", nameof(pn));

        return value;
    }

    public static string Normalize(string deviceId)
    {
        if (string.IsNullOrWhiteSpace(deviceId))
            throw new ArgumentException("DeviceId 不能为空", nameof(deviceId));

        var value = deviceId.Trim();
        if (value.Contains(':') || value.Contains('/') || value.Any(char.IsWhiteSpace))
            throw new ArgumentException("DeviceId 不能包含冒号、斜杠或空白字符", nameof(deviceId));

        return value;
    }

    public static string NormalizeAgingNumber(string agingNumber)
    {
        if (string.IsNullOrWhiteSpace(agingNumber))
            throw new ArgumentException("AgingNumber 不能为空", nameof(agingNumber));

        var value = agingNumber.Trim();
        if (!string.Equals(value, agingNumber, StringComparison.Ordinal) ||
            value.Any(ch => ch is < '0' or > '9') ||
            !int.TryParse(value, out var number) || number < 0)
            throw new ArgumentException(
                "AgingNumber 必须是非负整数格式的字符串",
                nameof(agingNumber));

        return number.ToString();
    }

    private static string GroupControlRunRoot(string agingNumber, long runSeq)
    {
        if (runSeq <= 0)
            throw new ArgumentOutOfRangeException(nameof(runSeq), "群控批次序号必须大于 0");

        return $"{Prefix}:group-control:{NormalizeAgingNumber(agingNumber)}:run:{runSeq}";
    }

    private static string NormalizeSegment(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException("Redis 键片段不能为空", parameterName);

        var normalized = value.Trim().ToLowerInvariant();
        if (normalized.Any(ch => !(char.IsLetterOrDigit(ch) || ch == '_')))
            throw new ArgumentException("Redis 键片段只能包含字母、数字和下划线", parameterName);

        return normalized;
    }
}
