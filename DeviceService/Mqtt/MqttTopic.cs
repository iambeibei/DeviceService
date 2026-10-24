namespace DeviceService.Mqtt;

/// <summary>
/// MQTT 主题解析工具。所有方法都按最新协议严格匹配，不兼容旧主题。
/// </summary>
public static class MqttTopic
{
    public static bool IsGroupControlMode(string topic)
        => topic.Equals("device/public/deviceMode", StringComparison.OrdinalIgnoreCase);

    public static bool IsGroupControlAgingStage(string topic)
        => topic.Equals("device/public/agingStage", StringComparison.OrdinalIgnoreCase);

    public static bool TryGetTaskCompletionDeviceId(string topic, out string deviceId)
    {
        deviceId = string.Empty;
        var parts = Split(topic);
        if (parts.Length != 4 ||
            !parts[0].Equals("device", StringComparison.OrdinalIgnoreCase) ||
            parts[1].Equals("public", StringComparison.OrdinalIgnoreCase) ||
            !parts[2].Equals("event", StringComparison.OrdinalIgnoreCase) ||
            !parts[3].Equals("TaskCP", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        deviceId = parts[1];
        return !string.IsNullOrWhiteSpace(deviceId);
    }

    /// <summary>
    /// device/{deviceId}/event/OTACP：子节点固件下载完成上报，payload 形如
    /// {"Seq":1782373476,"Data":{"IsComplete":1,"AgingNumber":"2"}}。
    ///
    /// 这是 OTA 升级完成的**主要判据**（2026-10-22 改回）：
    /// 新固件升级不再重启，因此"版本号变化"已不可靠，改由设备主动上报下载完成。
    /// 注意它与 TaskCP 的区别：OTACP 不携带 ota 指令 Seq（payload 里的 Seq 是设备
    /// 自己的消息序号），批次归属只能按 deviceId 反查该架的活跃 OTA 批次。
    /// </summary>
    public static bool TryGetOtaCompletionDeviceId(string topic, out string deviceId)
    {
        deviceId = string.Empty;
        var parts = Split(topic);
        if (parts.Length != 4 ||
            !parts[0].Equals("device", StringComparison.OrdinalIgnoreCase) ||
            parts[1].Equals("public", StringComparison.OrdinalIgnoreCase) ||
            !parts[2].Equals("event", StringComparison.OrdinalIgnoreCase) ||
            !parts[3].Equals("OTACP", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        deviceId = parts[1];
        return !string.IsNullOrWhiteSpace(deviceId);
    }

    // 已删除 TryGetDeviceId：全项目零调用者，且它的判定逻辑（parts[1] != "public"
    // 却又把 deviceId 取成 parts[1]）自相矛盾，留着只会误导。

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
        if (type is not ("state" or "fault" or "warn" or "stepid"))
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
