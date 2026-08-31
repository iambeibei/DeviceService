namespace DeviceService.Mqtt;

/// <summary>
/// MQTT 主题解析工具。所有方法都按最新协议严格匹配，不兼容旧主题。
/// </summary>
public static class MqttTopic
{
    public static bool TryGetDeviceId(string topic, out string deviceId)
    {
        deviceId = string.Empty;
        var parts = Split(topic);
        if (parts.Length < 3 ||
            !parts[0].Equals("device", StringComparison.OrdinalIgnoreCase) ||
            parts[1].Equals("public", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        deviceId = parts[1];
        return !string.IsNullOrWhiteSpace(deviceId);
    }

    public static bool TryGetCommandReply(
        string topic,
        out string deviceId,
        out string replyType)
    {
        deviceId = string.Empty;
        replyType = string.Empty;
        var parts = Split(topic);

        if (parts.Length != 5 ||
            !parts[0].Equals("device", StringComparison.OrdinalIgnoreCase) ||
            parts[1].Equals("public", StringComparison.OrdinalIgnoreCase) ||
            !parts[2].Equals("cmd", StringComparison.OrdinalIgnoreCase) ||
            !parts[3].Equals("reply", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var type = parts[4].ToLowerInvariant();
        if (type is not ("point" or "state" or "pn" or "config"))
            return false;

        deviceId = parts[1];
        replyType = type;
        return true;
    }

    public static bool TryGetEventType(
        string topic,
        out string deviceId,
        out string eventType)
    {
        deviceId = string.Empty;
        eventType = string.Empty;
        var parts = Split(topic);

        if (parts.Length != 4 ||
            !parts[0].Equals("device", StringComparison.OrdinalIgnoreCase) ||
            parts[1].Equals("public", StringComparison.OrdinalIgnoreCase) ||
            !parts[2].Equals("event", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var type = parts[3].ToLowerInvariant();
        if (type is not ("state" or "fault" or "warn"))
            return false;

        deviceId = parts[1];
        eventType = type;
        return true;
    }

    public static bool TryGetLogType(
        string topic,
        out string deviceId,
        out string logType)
    {
        deviceId = string.Empty;
        logType = string.Empty;
        var parts = Split(topic);

        if (parts.Length != 4 ||
            !parts[0].Equals("device", StringComparison.OrdinalIgnoreCase) ||
            parts[1].Equals("public", StringComparison.OrdinalIgnoreCase) ||
            !parts[2].Equals("log", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var type = parts[3].ToLowerInvariant();
        if (type is not ("runtime" or "error" or "local"))
            return false;

        deviceId = parts[1];
        logType = type;
        return true;
    }

    public static bool TryGetAgingDataDeviceId(string topic, out string deviceId)
    {
        deviceId = string.Empty;
        var parts = Split(topic);
        if (parts.Length != 4 ||
            !parts[0].Equals("device", StringComparison.OrdinalIgnoreCase) ||
            parts[1].Equals("public", StringComparison.OrdinalIgnoreCase) ||
            !parts[2].Equals("data", StringComparison.OrdinalIgnoreCase) ||
            !parts[3].Equals("aging", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        deviceId = parts[1];
        return true;
    }

    private static string[] Split(string topic)
    {
        if (string.IsNullOrWhiteSpace(topic))
            return [];

        return topic.Split(
            '/',
            StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }
}
