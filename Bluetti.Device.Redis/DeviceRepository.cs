using Bluetti.Device.Redis.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using StackExchange.Redis;
using System.Collections.Concurrent;

namespace Bluetti.Device.Redis;

/// <summary>
/// 设备数据仓储。
/// 上线/下线维护全量、分架点位和总控在线列表；配置、PN、状态、点位、事件、日志和老化数据
/// 继续保存到 lgw:device:{deviceId}:... 命名空间。
/// </summary>
public sealed class DeviceRepository
{
    private readonly RedisHelper _redis;

    // TaskQueue 保存于 state Hash 的字符串 JSON 数组字段中。
    // 同一进程内用每设备锁保护 read-modify-write，避免并发追加/删除覆盖。
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _agingTaskQueueLocks =
        new(StringComparer.Ordinal);

    public DeviceRepository(RedisHelper redis) => _redis = redis;

    /// <summary>
    /// 保存子节点上线信息。
    /// 全量在线 List 保持原键，并按 type/AgingNumber 同步维护分架点位列表和总控列表。
    /// 每个列表元素都是 JSON 字符串：
    /// {"id":"100000010","type":"1","AgingNumber":"0"}
    /// </summary>
    public async Task SaveOnlineAsync(
        string deviceId,
        string deviceType,
        string? agingNumber,
        long? messageSeq = null,
        long? meshLevel = null)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        deviceType = deviceType.Trim();

        if (string.IsNullOrWhiteSpace(deviceType))
            throw new ArgumentException("设备类型不能为空", nameof(deviceType));
        if (agingNumber is not null)
            agingNumber = RedisKeys.NormalizeAgingNumber(agingNumber);

        var previous = await GetAsync(deviceId);

        var onlineJson = JsonConvert.SerializeObject(new
        {
            id = deviceId,
            type = deviceType,
            AgingNumber = agingNumber
        });

        // 先移除同一设备的旧分类，支持设备重新上线时改变架号或类型。
        await RemoveDeviceFromOnlineListAsync(RedisKeys.OnlineDeviceList, deviceId);
        if (!string.IsNullOrWhiteSpace(previous?.AgingNumber))
        {
            if (string.Equals(previous.DeviceType, "1", StringComparison.Ordinal))
                await RemoveDeviceFromOnlineListAsync(
                    RedisKeys.AgingRackOnlineDeviceList(previous.AgingNumber),
                    deviceId);
            else if (string.Equals(previous.DeviceType, "2", StringComparison.Ordinal))
                await RemoveDeviceFromOnlineListAsync(
                    RedisKeys.ControlOnlineDeviceList,
                    deviceId);
        }

        await _redis.ListRightPushAsync(RedisKeys.OnlineDeviceList, onlineJson);

        if (agingNumber is not null && string.Equals(deviceType, "1", StringComparison.Ordinal))
        {
            var rackList = RedisKeys.AgingRackOnlineDeviceList(agingNumber);
            await RemoveDeviceFromOnlineListAsync(rackList, deviceId);
            await _redis.ListRightPushAsync(rackList, onlineJson);
        }
        else if (agingNumber is not null && string.Equals(deviceType, "2", StringComparison.Ordinal))
        {
            await RemoveDeviceFromOnlineListAsync(RedisKeys.ControlOnlineDeviceList, deviceId);
            await _redis.ListRightPushAsync(RedisKeys.ControlOnlineDeviceList, onlineJson);
        }

        // 继续维护设备目录、最后活动时间和原有 runtime Hash。
        await MarkSeenAsync(deviceId, "up_line", messageSeq);
        await _redis.HashSetAsync(
            RedisKeys.DeviceRuntime(deviceId),
            [
                new HashEntry("DeviceType", deviceType),
                new HashEntry("AgingNumber", agingNumber ?? string.Empty),
                new HashEntry("MeshLevel", meshLevel?.ToString() ?? string.Empty)
            ]);
    }

    /// <summary>
    /// 设备上线时把它写入「架 + Mesh 层级」清单 lgw:ota:{AgingNumber}:{MeshLevel}，供 OTA 分层升级取用。
    /// 与在线列表同构、同样先移除该设备的旧元素再追加：设备重复上线不会留下重复条目，
    /// 换架或换层级时旧条目也会被顶掉。
    /// 注意：该清单目前只在上/下线时维护，不设过期；
    /// 若设备离线后不移除，清单里会保留已离线节点，取用方需自行判断在线状态。
    /// </summary>
    public async Task SaveOtaMeshLevelAsync(
        string deviceId,
        string deviceType,
        string agingNumber,
        long meshLevel)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        agingNumber = RedisKeys.NormalizeAgingNumber(agingNumber);

        var key = RedisKeys.OtaMeshLevelDevices(agingNumber, meshLevel);
        var meshJson = JsonConvert.SerializeObject(new
        {
            id = deviceId,
            type = deviceType.Trim(),
            AgingNumber = agingNumber,
            MeshLevel = meshLevel
        });

        await RemoveDeviceFromOnlineListAsync(key, deviceId);
        await _redis.ListRightPushAsync(key, meshJson);
    }

    /// <summary>
    /// 设备离线时从「架 + Mesh 层级」清单移除，避免清单里残留已离线节点。
    /// </summary>
    public async Task<bool> RemoveOtaMeshLevelAsync(
        string deviceId,
        string agingNumber,
        long meshLevel)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        agingNumber = RedisKeys.NormalizeAgingNumber(agingNumber);

        return await RemoveDeviceFromOnlineListAsync(
            RedisKeys.OtaMeshLevelDevices(agingNumber, meshLevel),
            deviceId);
    }

    public async Task VersionSaveToRuntime(string deviceId,string version)
    {
        await _redis.HashSetAsync(RedisKeys.DeviceRuntime(deviceId),[new HashEntry("FirewareVersion", version)]);
    }

    public async Task StepIDSaveToRuntime(string deviceId, string StepId)
    {
        await _redis.HashSetAsync(RedisKeys.DeviceRuntime(deviceId), [new HashEntry("StepId", StepId)]);
    }


    /// <summary>
    /// 根据设备 ID 从全量及其所属分类在线列表中删除设备。
    /// </summary>
    public async Task<bool> RemoveOnlineDeviceAsync(string deviceId)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        var existing = await GetAsync(deviceId);
        var removed = await RemoveDeviceFromOnlineListAsync(RedisKeys.OnlineDeviceList, deviceId);
        await RemoveDeviceFromClassifiedOnlineListAsync(existing, deviceId);
        return removed;
    }

    /// <summary>
    /// 处理 device/public/down_line 遗嘱消息。
    /// 从原有在线 List 删除设备，并将 runtime.Online 更新为 false。
    /// 不删除设备目录、老化数据、四类回复和历史状态。
    /// </summary>
    public async Task<bool> SaveOfflineAsync(
        string deviceId,
        string deviceType,
        long? messageSeq,
        string rawPayload)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        deviceType = deviceType.Trim();

        if (string.IsNullOrWhiteSpace(deviceType))
            throw new ArgumentException("设备类型不能为空", nameof(deviceType));

        var now = DateTime.UtcNow;
        var runtimeKey = RedisKeys.DeviceRuntime(deviceId);
        var existing = await _redis.HashToObjectAsync<DeviceNode>(runtimeKey);
        var removed = await RemoveDeviceFromOnlineListAsync(RedisKeys.OnlineDeviceList, deviceId);
        await RemoveDeviceFromClassifiedOnlineListAsync(existing, deviceId);

        var node = existing ?? new DeviceNode
        {
            DeviceId = deviceId,
            FirstSeenUtc = now
        };

        node.Online = false;
        node.DeviceType = deviceType;
        node.LastSeenUtc = now;
        node.LastSource = "down_line";

        // 遗嘱消息通常 Seq=0，不用 0 覆盖之前有效的业务消息序号。
        if (messageSeq.HasValue && messageSeq.Value > 0)
            node.LastMessageSeq = messageSeq.Value;

        // lgw:devices 是已发现设备目录，不是在线列表，因此离线时继续保留。
        await _redis.SetAddAsync(RedisKeys.DeviceSet, deviceId);
        await _redis.SortedSetAddAsync(
            RedisKeys.DeviceLastSeen,
            deviceId,
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        await _redis.ObjectToHashAsync(runtimeKey, node);
        await _redis.HashSetAsync(
            runtimeKey,
            [new HashEntry("LastLifecyclePayload", rawPayload)]);

        // 保留原有全局离线事件 Stream 格式。
        //await _redis.StreamAddAsync(
        //    RedisKeys.DeviceEventStream,
        //    [
        //        new NameValueEntry("deviceId", deviceId),
        //        new NameValueEntry("event", "down_line"),
        //        new NameValueEntry("type", deviceType),
        //        new NameValueEntry("payload", rawPayload),
        //        new NameValueEntry("removedFromOnlineList", removed ? "true" : "false"),
        //        new NameValueEntry("receivedAtUtc", now.ToString("O"))
        //    ],
        //    maxLength: 100_000);

        return removed;
    }

    private async Task<bool> RemoveDeviceFromOnlineListAsync(string listKey, string deviceId)
    {
        var existingItems = await _redis.ListRangeAsync(listKey);
        var removed = false;

        foreach (var item in existingItems)
        {
            if (string.IsNullOrWhiteSpace(item))
                continue;

            try
            {
                var json = JObject.Parse(item);
                var oldDeviceId = json.GetValue("id", StringComparison.OrdinalIgnoreCase)?.Value<string>();
                if (!string.Equals(oldDeviceId, deviceId, StringComparison.Ordinal))
                    continue;

                if (await _redis.ListRemoveAsync(listKey, item, count: 0) > 0)
                    removed = true;
            }
            catch (JsonException)
            {
                // 非法历史元素不参与按 ID 删除，避免误删其他业务数据。
            }
        }

        return removed;
    }

    private async Task RemoveDeviceFromClassifiedOnlineListAsync(
        DeviceNode? device,
        string deviceId)
    {
        if (string.IsNullOrWhiteSpace(device?.AgingNumber))
            return;

        var agingNumber = RedisKeys.NormalizeAgingNumber(device.AgingNumber);

        if (string.Equals(device.DeviceType, "1", StringComparison.Ordinal))
        {
            await RemoveDeviceFromOnlineListAsync(
                RedisKeys.AgingRackOnlineDeviceList(agingNumber),
                deviceId);
        }
        else if (string.Equals(device.DeviceType, "2", StringComparison.Ordinal))
        {
            await RemoveDeviceFromOnlineListAsync(
                RedisKeys.ControlOnlineDeviceList,
                deviceId);
        }
    }

    /// <summary>
    /// 任意有效设备业务消息更新原有 runtime Hash。
    /// </summary>
    public async Task MarkSeenAsync(string deviceId, string source, long? messageSeq = null)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        var now = DateTime.UtcNow;
        var key = RedisKeys.DeviceRuntime(deviceId);
        var existing = await _redis.HashToObjectAsync<DeviceNode>(key);

        var node = existing ?? new DeviceNode
        {
            DeviceId = deviceId,
            FirstSeenUtc = now
        };

        node.Online = true;
        node.LastSeenUtc = now;
        node.LastSource = source;
        if (messageSeq.HasValue)
            node.LastMessageSeq = messageSeq;

        await _redis.SetAddAsync(RedisKeys.DeviceSet, deviceId);
        await _redis.SortedSetAddAsync(
            RedisKeys.DeviceLastSeen,
            deviceId,
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        await _redis.ObjectToHashAsync(key, node);
    }

    /// <summary>
    /// 保存 state/fault/warn 事件的最新值和设备独立历史 Stream。
    /// </summary>
    public async Task SaveEventAsync(
        string deviceId,
        string eventType,
        long? messageSeq,
        string? state,
        string? message,
        string rawPayload)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        eventType = NormalizeEventType(eventType);
        await MarkSeenAsync(deviceId, $"event/{eventType}", messageSeq);

        var now = DateTime.UtcNow;
        var eventKey = RedisKeys.DeviceEventLatest(deviceId, eventType);

        await _redis.HashSetAsync(
            eventKey,
            [
                new HashEntry("DeviceId", deviceId),
                new HashEntry("EventType", eventType),
                new HashEntry("Seq", messageSeq?.ToString() ?? string.Empty),
                new HashEntry("State", state ?? string.Empty),
                new HashEntry("Message", message ?? string.Empty),
                new HashEntry("Payload", rawPayload),
                new HashEntry("ReceivedAtUtc", now.ToString("O"))
            ]);

        await _redis.StreamAddAsync(
            RedisKeys.DeviceEventHistory(deviceId, eventType),
            [
                new NameValueEntry("seq", messageSeq?.ToString() ?? string.Empty),
                new NameValueEntry("state", state ?? string.Empty),
                new NameValueEntry("message", message ?? string.Empty),
                new NameValueEntry("payload", rawPayload),
                new NameValueEntry("receivedAtUtc", now.ToString("O"))
            ],
            maxLength: 50_000);

        if (eventType == "state")
        {
            // state Hash 只保留任务调度字段：
            // TaskQueue：尚未真正发布到 ESP32 的 start_aging Seq，使用字符串 JSON 数组；
            // CurrentAgingSeq：当前已经发布并等待下位机状态结束的 start_aging Seq。
            // 已完成状态不再存 CompleteTasks，改由 DeviceEventHandler 回调业务 API。
            await _redis.HashSetIfNotExistsAsync(eventKey, "TaskQueue", "[]");
            await _redis.HashSetIfNotExistsAsync(eventKey, "CurrentAgingSeq", string.Empty);

            // 清理旧版本遗留字段。
            await _redis.HashDeleteAsync(eventKey, "CompleteTasks");
            await _redis.HashDeleteAsync(eventKey, "TaskId");
            await _redis.HashDeleteAsync(eventKey, "Statu");

            await _redis.HashSetAsync(
                RedisKeys.DeviceRuntime(deviceId),
                [
                    new HashEntry("AgingState", state ?? string.Empty),
                    new HashEntry("LastStatePayload", rawPayload)
                ]);
            var json = JObject.Parse(rawPayload);
            if(json.SelectToken("Data.ProgramId")!= null)
            {
                var programId = json.SelectToken("Data.ProgramId").Value<string>();
                await _redis.HashSetAsync(RedisKeys.DeviceEventLatest(deviceId, eventType),[new HashEntry("ProgramId", programId)]);
            }


            // 注意：这里不再清 CurrentAgingSeq。
            // 状态处理器需要先通过 CurrentAgingSeq 找到命令 RecordId 并完成 API 回调，
            // 对 agingIdle / agingStop / agingError 回调后再统一清理当前任务。
        }
    }

    /// <summary>
    /// 记录服务端内部产生的故障（如群控阶段超时剔除、放行指令未收到设备确认）。
    ///
    /// 与 SaveEventAsync 的区别：本方法不会调用 MarkSeenAsync。
    /// MarkSeenAsync 会把 runtime.Online 置为 true 并刷新 LastSeenUtc，
    /// 给离线设备写故障会把它"复活"成在线，进而影响总控唯一性判断和在线诊断。
    /// 服务端主动产生的故障不是设备活动证据，因此不能刷新在线状态。
    /// </summary>
    public async Task SaveGroupControlFaultAsync(
        string deviceId,
        long? messageSeq,
        string message,
        string rawPayload)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        const string eventType = "fault";
        var now = DateTime.UtcNow;
        var eventKey = RedisKeys.DeviceEventLatest(deviceId, eventType);

        await _redis.HashSetAsync(
            eventKey,
            [
                new HashEntry("DeviceId", deviceId),
                new HashEntry("EventType", eventType),
                new HashEntry("Seq", messageSeq?.ToString() ?? string.Empty),
                new HashEntry("State", string.Empty),
                new HashEntry("Message", message ?? string.Empty),
                new HashEntry("Payload", rawPayload),
                new HashEntry("ReceivedAtUtc", now.ToString("O"))
            ]);

        await _redis.StreamAddAsync(
            RedisKeys.DeviceEventHistory(deviceId, eventType),
            [
                new NameValueEntry("seq", messageSeq?.ToString() ?? string.Empty),
                new NameValueEntry("state", string.Empty),
                new NameValueEntry("message", message ?? string.Empty),
                new NameValueEntry("payload", rawPayload),
                new NameValueEntry("receivedAtUtc", now.ToString("O"))
            ],
            maxLength: 50_000);
    }

    /// <summary>
    /// 兼容读取旧版数字数组 [111,222] 与新版字符串数组 ["111","222"]，
    /// 返回值始终规范化成 List&lt;string&gt;。
    /// </summary>
    private async Task<List<string>> ReadAgingTaskQueueUnsafeAsync(string stateKey)
    {
        var raw = await _redis.HashGetAsync(stateKey, "TaskQueue");
        if (raw.IsNullOrEmpty)
            return [];

        try
        {
            var array = JArray.Parse(raw.ToString());
            var result = new List<string>(array.Count);
            foreach (var token in array)
            {
                var text = token.Type switch
                {
                    JTokenType.String => token.Value<string>()?.Trim(),
                    JTokenType.Integer => token.ToString(Formatting.None),
                    _ => null
                };

                if (!string.IsNullOrWhiteSpace(text) &&
                    long.TryParse(text, out var seq) &&
                    seq > 0)
                {
                    var normalized = seq.ToString();
                    if (!result.Any(x => string.Equals(x, normalized, StringComparison.Ordinal)))
                        result.Add(normalized);
                }
            }

            return result;
        }
        catch (JsonException)
        {
            // 历史脏数据不应阻断命令流；重新从空队列开始。
            return [];
        }
    }


    /// <summary>
    /// 保存 device/{id}/data/aging 的最新值和设备独立历史 Stream。
    /// </summary>
    public async Task SaveAgingDataAsync(
        string deviceId,
        string? sn,
        int? currentStep,
        long? idNum,
        long? sampleTimestamp,
        long? messageSeq,
        string rawPayload)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        if (string.IsNullOrWhiteSpace(sn))
            throw new ArgumentException("老化数据 Data.SN（PN）不能为空，无法按 PN 分流保存。", nameof(sn));

        var pn = RedisKeys.NormalizePn(sn);
        await MarkSeenAsync(deviceId, "data/aging", messageSeq);
        var now = DateTime.UtcNow;

        await _redis.HashSetAsync(
            RedisKeys.DeviceAgingLatest(deviceId, pn),
            [
                new HashEntry("DeviceId", deviceId),
                new HashEntry("Seq", messageSeq?.ToString() ?? string.Empty),
                new HashEntry("SN", sn ?? string.Empty),
                new HashEntry("CurrentStep", currentStep?.ToString() ?? string.Empty),
                new HashEntry("IDNUM", idNum?.ToString() ?? string.Empty),
                new HashEntry("TIMESTAMP", sampleTimestamp?.ToString() ?? string.Empty),
                new HashEntry("Payload", rawPayload),
                new HashEntry("ReceivedAtUtc", now.ToString("O"))
            ]); 

        await _redis.HashSetAsync(
            RedisKeys.DeviceRuntime(deviceId),
            [
                //new HashEntry("CurrentPn", sn ?? string.Empty),
                new HashEntry("CurrentStep", currentStep?.ToString() ?? string.Empty)
            ]);

        // 老化采样历史设置统一过期时间，每次采样都会刷新整条流的过期时间。
        // 某 PN 停止采样超过 RedisTtl.DeviceAgingHistory 后，该历史流会被整键删除。
        await _redis.StreamAddAsync(
            RedisKeys.DeviceAgingHistory(deviceId, pn),
            [
                new NameValueEntry("seq", messageSeq?.ToString() ?? string.Empty),
                new NameValueEntry("sn", sn ?? string.Empty),
                new NameValueEntry("currentStep", currentStep?.ToString() ?? string.Empty),
                new NameValueEntry("idNum", idNum?.ToString() ?? string.Empty),
                new NameValueEntry("sampleTimestamp", sampleTimestamp?.ToString() ?? string.Empty),
                new NameValueEntry("payload", rawPayload),
                new NameValueEntry("receivedAtUtc", now.ToString("O"))
            ],
            maxLength: 100_000,
            ttl: RedisTtl.DeviceAgingHistory);
    }

    /// <summary>
    /// 保存 runtime/error/local 日志的最新值和设备独立历史 Stream。
    /// </summary>
    public async Task SaveLogAsync(
        string deviceId,
        string logType,
        long? messageSeq,
        string rawPayload)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        logType = NormalizeLogType(logType);
        await MarkSeenAsync(deviceId, $"log/{logType}", messageSeq);
        var now = DateTime.UtcNow;

        await _redis.HashSetAsync(
            RedisKeys.DeviceLogLatest(deviceId, logType),
            [
                new HashEntry("DeviceId", deviceId),
                new HashEntry("LogType", logType),
                new HashEntry("Seq", messageSeq?.ToString() ?? string.Empty),
                new HashEntry("Payload", rawPayload),
                new HashEntry("ReceivedAtUtc", now.ToString("O"))
            ]);

        await _redis.StreamAddAsync(
            RedisKeys.DeviceLogHistory(deviceId, logType),
            [
                new NameValueEntry("seq", messageSeq?.ToString() ?? string.Empty),
                new NameValueEntry("payload", rawPayload),
                new NameValueEntry("receivedAtUtc", now.ToString("O"))
            ],
            maxLength: 50_000);
    }

    /// <summary>
    /// 保存四类设备回复：point/state/pn/config。
    /// 每类回复使用一个最新值 Hash 和一个历史 Stream。
    /// </summary>
    public async Task SaveCommandReplyAsync(
        string deviceId,
        string replyType,
        long? messageSeq,
        long cmdSeq,
        int code,
        string? message,
        JToken? cmdData)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        replyType = RedisKeys.NormalizeReplyType(replyType);
        await MarkSeenAsync(deviceId, $"cmd/reply/{replyType}", messageSeq);
        var now = DateTime.UtcNow;

        var reply = new DeviceReply
        {
            DeviceId = deviceId,
            ReplyType = replyType,
            Seq = messageSeq,
            CmdSeq = cmdSeq,
            Code = code,
            Message = message,
            CmdData = cmdData,
            ReceivedAtUtc = now
        };

        await _redis.ObjectToHashAsync(
            RedisKeys.DeviceReplyLatest(deviceId, replyType),
            reply);

        await _redis.StreamAddAsync(
            RedisKeys.DeviceReplyHistory(deviceId, replyType),
            [
                new NameValueEntry("seq", messageSeq?.ToString() ?? string.Empty),
                new NameValueEntry("cmdSeq", cmdSeq.ToString()),
                new NameValueEntry("code", code.ToString()),
                new NameValueEntry("message", message ?? string.Empty),
                new NameValueEntry("cmdData", cmdData?.ToString(Formatting.None) ?? string.Empty),
                new NameValueEntry("receivedAtUtc", now.ToString("O"))
            ],
            maxLength: 50_000);

        // 高频查询字段继续同步到原有 runtime Hash，不再写设备根 Hash。
        if (replyType == "pn")
        {
            var pn = cmdData?.SelectToken("GetPN")?.Value<string>();
            if (!string.IsNullOrWhiteSpace(pn))
            {
                await _redis.HashSetAsync(
                    RedisKeys.DeviceRuntime(deviceId),
                    [new HashEntry("CurrentPn", pn)]);
            }
        }
        else if (replyType == "state")
        {
            var state = cmdData?.SelectToken("State")?.Value<string>();
            if (!string.IsNullOrWhiteSpace(state))
            {
                await _redis.HashSetAsync(
                    RedisKeys.DeviceRuntime(deviceId),
                    [new HashEntry("AgingState", state)]);
            }
        }
    }

    /// <summary>
    /// 按原有格式返回在线设备 JSON String List。
    /// </summary>
    public Task<string[]> GetOnlineDeviceJsonListAsync()
        => _redis.ListRangeAsync(RedisKeys.OnlineDeviceList, 0, -1);

    /// <summary>
    /// 通过全量在线 List（lgw:devices:online）判断设备是否在线。
    /// 该列表由上线/下线处理器专门维护，是在线状态的权威来源；
    /// runtime.Online 会被任意业务消息刷新，因此创建老化门控前统一以本方法为准。
    /// </summary>
    public async Task<bool> IsDeviceOnlineAsync(string deviceId)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        var items = await _redis.ListRangeAsync(RedisKeys.OnlineDeviceList, 0, -1);

        foreach (var item in items)
        {
            if (string.IsNullOrWhiteSpace(item))
                continue;

            try
            {
                var json = JObject.Parse(item);
                var id = json.GetValue("id", StringComparison.OrdinalIgnoreCase)?.Value<string>();
                if (string.Equals(id, deviceId, StringComparison.Ordinal))
                    return true;
            }
            catch (JsonException)
            {
                // 非法历史元素不参与在线判断。
            }
        }

        return false;
    }

    public Task<string[]> GetAgingRackOnlineDeviceJsonListAsync(string agingNumber)
        => _redis.ListRangeAsync(
            RedisKeys.AgingRackOnlineDeviceList(agingNumber),
            0,
            -1);

    public Task<string[]> GetControlOnlineDeviceJsonListAsync()
        => _redis.ListRangeAsync(RedisKeys.ControlOnlineDeviceList, 0, -1);

    public Task<DeviceNode?> GetAsync(string deviceId)
        => _redis.HashToObjectAsync<DeviceNode>(RedisKeys.DeviceRuntime(deviceId));

    /// <summary>
    /// 读取设备当前老化状态。start_aging 的长任务门控只依赖该字段：
    /// agingStart 表示正在老化；agingStop / agingError / agingIdle 表示允许下一条 start_aging。
    /// </summary>
    public async Task<string?> GetAgingStateAsync(string deviceId)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        var value = await _redis.HashGetAsync(RedisKeys.DeviceRuntime(deviceId), "AgingState");
        return value.IsNullOrEmpty ? null : value.ToString();
    }

    /// <summary>
    /// 将尚未运行的 start_aging Seq 追加到 state.TaskQueue。
    /// TaskQueue 在 Redis Hash 中以字符串 JSON 数组保存，例如 ["111","222","333"]。
    /// 方法参数仍使用 long，写入前统一转换为 string；重复 Seq 不会重复追加。
    /// </summary>
    public async Task AddPendingAgingTaskAsync(string deviceId, long seq)
    {
        if (seq <= 0)
            throw new ArgumentOutOfRangeException(nameof(seq), "老化指令 Seq 必须大于 0");

        deviceId = RedisKeys.Normalize(deviceId);
        var seqText = seq.ToString();
        var sync = _agingTaskQueueLocks.GetOrAdd(deviceId, _ => new SemaphoreSlim(1, 1));
        await sync.WaitAsync();
        try
        {
            var key = RedisKeys.DeviceEventLatest(deviceId, "state");
            var queue = await ReadAgingTaskQueueUnsafeAsync(key);
            if (!queue.Any(x => string.Equals(x, seqText, StringComparison.Ordinal)))
            {
                queue.Add(seqText);
                await _redis.HashSetAsync(
                    key,
                    [new HashEntry("TaskQueue", JsonConvert.SerializeObject(queue))]);
            }
            else
            {
                // 即使 Redis 中还是旧版数字数组，也通过本次写回统一成字符串数组。
                await _redis.HashSetAsync(
                    key,
                    [new HashEntry("TaskQueue", JsonConvert.SerializeObject(queue))]);
            }

            // 即使 state 事件尚未到达，也保证平台第一次看到该 Hash 时已有稳定字段。
            await _redis.HashSetIfNotExistsAsync(key, "CurrentAgingSeq", string.Empty);
            await _redis.HashDeleteAsync(key, "CompleteTasks");
            await _redis.HashDeleteAsync(key, "TaskId");
            await _redis.HashDeleteAsync(key, "Statu");
        }
        finally
        {
            sync.Release();
        }
    }

    /// <summary>
    /// 读取 TaskQueue 队头的待运行 start_aging Seq。
    /// 主要用于处理极窄竞态：ESP32 的 agingStart 可能在 MQTT PublishAsync 返回后、
    /// Worker 写入 CurrentAgingSeq 之前到达。此时可用队头 Seq 恢复关联。
    /// </summary>
    public async Task<long?> GetFirstPendingAgingTaskAsync(string deviceId)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        var sync = _agingTaskQueueLocks.GetOrAdd(deviceId, _ => new SemaphoreSlim(1, 1));
        await sync.WaitAsync();
        try
        {
            var key = RedisKeys.DeviceEventLatest(deviceId, "state");
            var queue = await ReadAgingTaskQueueUnsafeAsync(key);
            if (queue.Count == 0)
                return null;

            return long.TryParse(queue[0], out var seq) && seq > 0 ? seq : null;
        }
        finally
        {
            sync.Release();
        }
    }

    /// <summary>
    /// start_aging 已真正发布到 ESP32 后，从 state.TaskQueue 删除对应 Seq。
    /// </summary>
    public async Task RemovePendingAgingTaskAsync(string deviceId, long seq)
    {
        if (seq <= 0)
            return;

        deviceId = RedisKeys.Normalize(deviceId);
        var seqText = seq.ToString();
        var sync = _agingTaskQueueLocks.GetOrAdd(deviceId, _ => new SemaphoreSlim(1, 1));
        await sync.WaitAsync();
        try
        {
            var key = RedisKeys.DeviceEventLatest(deviceId, "state");
            var queue = await ReadAgingTaskQueueUnsafeAsync(key);
            queue.RemoveAll(x => string.Equals(x, seqText, StringComparison.Ordinal));

            // 无论是否实际删除，都写回一次，顺便把历史数字数组迁移成字符串数组。
            await _redis.HashSetAsync(
                key,
                [new HashEntry("TaskQueue", JsonConvert.SerializeObject(queue))]);

            await _redis.HashDeleteAsync(key, "CompleteTasks");
            await _redis.HashDeleteAsync(key, "TaskId");
            await _redis.HashDeleteAsync(key, "Statu");
        }
        finally
        {
            sync.Release();
        }
    }

    /// <summary>
    /// 记录当前已经发布、等待下位机状态结束的 start_aging Seq。
    /// 同时写入 runtime 与 event:state.CurrentAgingSeq：
    /// runtime 供服务内部恢复使用，event:state 供上位平台直接查看当前正在运行的老化指令。
    /// </summary>
    public async Task SetCurrentAgingTaskIdAsync(
        string deviceId,
        long seq,
        string? recordId = null)
    {
        if (seq <= 0)
            throw new ArgumentOutOfRangeException(nameof(seq), "老化指令 Seq 必须大于 0");

        deviceId = RedisKeys.Normalize(deviceId);
        var seqText = seq.ToString();

        var runtimeEntries = new List<HashEntry>
        {
            new("CurrentAgingSeq", seqText)
        };
        if (!string.IsNullOrWhiteSpace(recordId))
            runtimeEntries.Add(new HashEntry("CurrentAgingRecordId", recordId.Trim()));

        await _redis.HashSetAsync(
            RedisKeys.DeviceRuntime(deviceId),
            [.. runtimeEntries]);

        var eventKey = RedisKeys.DeviceEventLatest(deviceId, "state");
        await _redis.HashSetAsync(
            eventKey,
            [new HashEntry("CurrentAgingSeq", seqText)]);
        await _redis.HashSetIfNotExistsAsync(eventKey, "TaskQueue", "[]");
        await _redis.HashDeleteAsync(eventKey, "CompleteTasks");
        await _redis.HashDeleteAsync(eventKey, "TaskId");
        await _redis.HashDeleteAsync(eventKey, "Statu");
    }

    public async Task<long?> GetCurrentAgingTaskIdAsync(string deviceId)
    {
        deviceId = RedisKeys.Normalize(deviceId);

        // 优先读取 runtime；如果服务升级/恢复时 runtime 中没有该字段，
        // 再从 event:state 回退读取，避免已运行任务的 Seq 丢失。
        var raw = await _redis.HashGetAsync(RedisKeys.DeviceRuntime(deviceId), "CurrentAgingSeq");
        if (!raw.IsNullOrEmpty && long.TryParse(raw.ToString(), out var runtimeSeq) && runtimeSeq > 0)
            return runtimeSeq;

        raw = await _redis.HashGetAsync(
            RedisKeys.DeviceEventLatest(deviceId, "state"),
            "CurrentAgingSeq");

        return !raw.IsNullOrEmpty && long.TryParse(raw.ToString(), out var stateSeq) && stateSeq > 0
            ? stateSeq
            : null;
    }

    /// <summary>
    /// 判断指定 start_aging Seq 是否仍属于设备当前或尚待发布的老化任务。
    /// runtime 用于识别已经发布并运行的任务，TaskQueue 用于补齐服务重启或消息乱序时的候选批次。
    /// </summary>
    public async Task<bool> HasAgingTaskAsync(string deviceId, long seq)
    {
        if (seq <= 0)
            throw new ArgumentOutOfRangeException(nameof(seq), "老化指令 Seq 必须大于 0");

        deviceId = RedisKeys.Normalize(deviceId);
        var seqText = seq.ToString();
        var runtimeSeq = await _redis.HashGetAsync(
            RedisKeys.DeviceRuntime(deviceId),
            "CurrentAgingSeq");
        if (!runtimeSeq.IsNullOrEmpty &&
            string.Equals(runtimeSeq.ToString(), seqText, StringComparison.Ordinal))
        {
            return true;
        }

        var sync = _agingTaskQueueLocks.GetOrAdd(deviceId, _ => new SemaphoreSlim(1, 1));
        await sync.WaitAsync();
        try
        {
            var queue = await ReadAgingTaskQueueUnsafeAsync(
                RedisKeys.DeviceEventLatest(deviceId, "state"));
            return queue.Any(value =>
                string.Equals(value, seqText, StringComparison.Ordinal));
        }
        finally
        {
            sync.Release();
        }
    }

    public async Task<string?> GetCurrentAgingRecordIdAsync(string deviceId)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        var raw = await _redis.HashGetAsync(
            RedisKeys.DeviceRuntime(deviceId),
            "CurrentAgingRecordId");
        return raw.IsNullOrEmpty ? null : raw.ToString();
    }

    public async Task ClearCurrentAgingTaskIdAsync(string deviceId)
    {
        deviceId = RedisKeys.Normalize(deviceId);

        // runtime 是内部恢复字段，结束后删除；event:state 是平台展示 Hash，
        // 保留字段本身并置空，保证 Hash 结构稳定。
        await _redis.HashDeleteAsync(RedisKeys.DeviceRuntime(deviceId), "CurrentAgingSeq");
        await _redis.HashDeleteAsync(RedisKeys.DeviceRuntime(deviceId), "CurrentAgingRecordId");
        await _redis.HashSetAsync(
            RedisKeys.DeviceEventLatest(deviceId, "state"),
            [new HashEntry("CurrentAgingSeq", string.Empty)]);
    }

    public Task<DeviceReply?> GetLatestReplyAsync(string deviceId, string replyType)
        => _redis.HashToObjectAsync<DeviceReply>(
            RedisKeys.DeviceReplyLatest(deviceId, replyType));

    public async Task<IReadOnlyList<DeviceNode>> GetAllAsync()
    {
        var ids = await _redis.SetMembersAsync(RedisKeys.DeviceSet);
        var devices = new List<DeviceNode>(ids.Length);
        foreach (var id in ids)
        {
            var node = await GetAsync(id);
            if (node is not null)
                devices.Add(node);
        }

        return devices.OrderBy(x => x.DeviceId, StringComparer.Ordinal).ToArray();
    }

    private static string NormalizeRequired(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException("参数不能为空", parameterName);
        return value.Trim();
    }

    private static string NormalizeEventType(string eventType)
    {
        var value = NormalizeRequired(eventType, nameof(eventType)).ToLowerInvariant();
        return value switch
        {
            "state" or "fault" or "warn" or "stepid" => value,
            _ => throw new ArgumentException(
                "EventType 只允许 state、fault、warn",
                nameof(eventType))
        };
    }

    private static string NormalizeLogType(string logType)
    {
        var value = NormalizeRequired(logType, nameof(logType)).ToLowerInvariant();
        return value switch
        {
            "runtime" or "error" or "local" => value,
            _ => throw new ArgumentException(
                "LogType 只允许 runtime、error、local",
                nameof(logType))
        };
    }
}
