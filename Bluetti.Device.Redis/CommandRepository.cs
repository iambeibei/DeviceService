using Bluetti.Device.Redis.Models;
using Newtonsoft.Json;
using StackExchange.Redis;
using System.Collections.Concurrent;

namespace Bluetti.Device.Redis;

public sealed class CommandRepository
{
    public const string ConsumerGroup = "device-service";

    // 命令 Hash 的过期时间统一由 RedisTtl.Command 控制（默认 7 天，修改见 RedisTtl.DefaultDays）。
    private static readonly TimeSpan CommandTtl = RedisTtl.Command;
    private readonly RedisHelper _redis;

    // 仅用于当前进程内“命令回复唤醒”。命令最终状态仍以 Redis Hash 为准。
    // Key = "{deviceId}:{cmdSeq}"。
    private readonly ConcurrentDictionary<string, TaskCompletionSource<CmdInfo>> _completionWaiters = new();

    public CommandRepository(RedisHelper redis) => _redis = redis;

    public async Task<CmdInfo> EnqueueAsync(string deviceId, string action, object? data = null, string? recordId = null)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        action = NormalizeAction(action);

        var command = new CmdInfo
        {
            Seq = await _redis.NextSequenceAsync(RedisKeys.CommandSequence),
            DeviceId = deviceId,
            RecordId = recordId?.Trim() ?? string.Empty,
            Action = action,
            ExpectedReplyType = CommandReplyTypeResolver.ForAction(action),
            Data = data is null ? null : Newtonsoft.Json.Linq.JToken.FromObject(data),
            Status = CommandStates.Queued,
            CreatedAtUtc = DateTime.UtcNow
        };

        // 单条命令状态存放在对应设备路径下：
        // lgw:device:{deviceId}:cmd:{seq}
        await _redis.ObjectToHashAsync(command.StatusKey, command, CommandTtl);

        // Redis 命令队列采用 DeviceInfos 数组格式。
        // 每个设备项同时携带 DeviceId 与 RecordId；RecordId 不下发给 ESP32，
        // 仅用于后续根据 CurrentAgingSeq 回调业务 API。
        var queueEnvelope = new BatchCommandEnvelope
        {
            Seq = command.Seq,
            DeviceInfos =
            [
                new BatchDeviceInfo
                {
                    DeviceId = deviceId,
                    RecordId = command.RecordId
                }
            ],
            Action = action,
            Data = command.Data?.DeepClone(),
            Status = command.Status,
            CreatedAtUtc = command.CreatedAtUtc.ToString("O")
        };

        await _redis.ObjectToStreamAsync(RedisKeys.CommandQueue, queueEnvelope, maxLength: 100_000);
        return command;
    }

    
    public Task EnsureGroupAsync()
        => _redis.EnsureConsumerGroupAsync(RedisKeys.CommandQueue, ConsumerGroup);

    /// <summary>
    /// 从 Redis Stream 读取批量命令，并在“进入原有命令处理流程之前”按 DeviceInfos 展开。
    /// Redis 中一条记录可以包含多个 {DeviceId,RecordId}；返回给 Worker 时仍是一条 Stream Entry，
    /// 但 Commands 中包含多个原有 CmdInfo，后续 MQTT/回复等待逻辑无需理解批量结构。
    /// </summary>
    public async Task<IReadOnlyList<(RedisValue Id, IReadOnlyList<CmdInfo> Commands)>> ReadAsync(
        string consumerName,
        string position,
        int count)
    {
        var entries = await _redis.StreamReadGroupAsync(
            RedisKeys.CommandQueue,
            ConsumerGroup,
            consumerName,
            position,
            count);

        var result = new List<(RedisValue, IReadOnlyList<CmdInfo>)>(entries.Length);
        foreach (var entry in entries)
        {
            var json = entry.Values.FirstOrDefault(x => x.Name == "data").Value;
            if (json.IsNullOrEmpty)
                continue;

            var batch = JsonConvert.DeserializeObject<BatchCommandEnvelope>(json.ToString());
            if (batch is null)
                continue;

            if (batch.Seq <= 0)
                throw new InvalidOperationException($"Redis 命令 Seq 必须大于 0，StreamId={entry.Id}");

            var action = NormalizeAction(batch.Action);
            var deviceInfos = NormalizeDeviceInfos(batch.DeviceInfos);

            var commands = new List<CmdInfo>(deviceInfos.Length);
            foreach (var deviceInfo in deviceInfos)
            {
                commands.Add(new CmdInfo
                {
                    Seq = batch.Seq,
                    DeviceId = deviceInfo.DeviceId,
                    RecordId = deviceInfo.RecordId,
                    Action = action,
                    ExpectedReplyType = CommandReplyTypeResolver.ForAction(action),
                    Data = batch.Data?.DeepClone(),
                    Status = string.IsNullOrWhiteSpace(batch.Status)
                        ? CommandStates.Queued
                        : batch.Status.Trim().ToLowerInvariant(),
                    CreatedAtUtc = ParseCreatedAtUtc(batch.CreatedAtUtc)
                });
            }

            result.Add((entry.Id, commands));
        }

        return result;
    }

    /// <summary>
    /// 在 MQTT 发布前先确保单设备命令元数据已经持久化。
    /// 这样下位机即使非常快地上报 agingStart，也能通过 CurrentAgingSeq 找到 RecordId。
    /// </summary>
    public async Task EnsureStoredAsync(CmdInfo command)
    {
        var current = await GetAsync(command.DeviceId, command.Seq);
        if (current is null)
        {
            await _redis.ObjectToHashAsync(command.StatusKey, command, CommandTtl);
            return;
        }

        // 恢复旧 Pending 或升级场景时，只补齐新的 RecordId，不改变现有命令状态。
        if (!string.IsNullOrWhiteSpace(command.RecordId) &&
            !string.Equals(current.RecordId, command.RecordId, StringComparison.Ordinal))
        {
            current.RecordId = command.RecordId;
            await _redis.ObjectToHashAsync(current.StatusKey, current, CommandTtl);
        }
    }

    public async Task MarkPublishedAsync(CmdInfo command)
    {
        // 回复可能非常快，因此写 published 前先检查命令是否已经进入终态，避免降级覆盖。
        var current = await GetAsync(command.DeviceId, command.Seq);
        if (current is not null && CommandStates.IsTerminal(current.Status))
            return;

        command.Status = CommandStates.Published;
        command.PublishedAtUtc = DateTime.UtcNow;
        command.CompletedAtUtc = null;
        command.TimedOutAtUtc = null;
        command.ResultCode = null;
        command.ResultMessage = null;
        command.ReplyType = null;
        command.ReplyJson = null;
        await _redis.ObjectToHashAsync(command.StatusKey, command, CommandTtl);
    }

    public async Task MarkQueuedAsync(CmdInfo command, string? reason = null)
    {
        var current = await GetAsync(command.DeviceId, command.Seq);
        if (current is not null && CommandStates.IsTerminal(current.Status))
            return;

        command.Status = CommandStates.Queued;
        command.PublishedAtUtc = null;
        command.CompletedAtUtc = null;
        command.TimedOutAtUtc = null;
        command.ResultCode = null;
        command.ResultMessage = reason;
        command.ReplyType = null;
        command.ReplyJson = null;
        await _redis.ObjectToHashAsync(command.StatusKey, command, CommandTtl);
    }

    /// <summary>
    /// 服务端主动把命令标记为 failed，用于命令尚未下发就已确定失败的场景，
    /// 例如 start_aging 目标设备不在线、拒绝创建老化门控。
    /// 与 CompleteAsync 的区别：本方法不依赖设备回复。
    /// 已处于终态（completed / failed / timed_out）的命令不会被覆盖。
    /// </summary>
    public async Task<CmdInfo> MarkFailedAsync(CmdInfo command, string reason)
    {
        ArgumentNullException.ThrowIfNull(command);

        var current = await GetAsync(command.DeviceId, command.Seq) ?? command;
        if (CommandStates.IsTerminal(current.Status))
            return current;

        current.Status = CommandStates.Failed;
        current.CompletedAtUtc = DateTime.UtcNow;
        current.TimedOutAtUtc = null;
        current.ResultCode = null;
        current.ResultMessage = reason;
        current.ReplyType = null;
        current.ReplyJson = null;
        await _redis.ObjectToHashAsync(current.StatusKey, current, CommandTtl);

        SignalCompletion(current);
        return current;
    }

    /// <summary>
    /// 服务端主动把命令标记为 completed，用于命令已被其它服务接管、不再由命令流逐台下发的场景，
    /// 例如多设备 ota 指令交给 OtaUpgradeService 按 Mesh 层级调度。
    /// 与设备回复触发的 CompleteAsync 的区别：本方法不依赖设备回复。
    /// 已处于终态（completed / failed / timed_out）的命令不会被覆盖。
    /// </summary>
    public async Task<CmdInfo> MarkCompletedAsync(CmdInfo command, string? message = null)
    {
        ArgumentNullException.ThrowIfNull(command);

        var current = await GetAsync(command.DeviceId, command.Seq) ?? command;
        if (CommandStates.IsTerminal(current.Status))
            return current;

        current.Status = CommandStates.Completed;
        current.CompletedAtUtc = DateTime.UtcNow;
        current.TimedOutAtUtc = null;
        current.ResultCode = null;
        current.ResultMessage = message;
        current.ReplyType = null;
        current.ReplyJson = null;
        await _redis.ObjectToHashAsync(current.StatusKey, current, CommandTtl);

        SignalCompletion(current);
        return current;
    }

    /// <summary>
    /// 设备在规定时间内没有返回对应 CmdSeq 时，将该命令标记为 timed_out。
    /// timed_out 是命令回复层面的终态，不做自动重发。
    /// 是否放行业务上的下一条任务由上层 Worker 决定：普通命令可继续，start_aging 仍需等待设备状态退出。
    /// </summary>
    /// <summary>
    /// 强制把命令状态改回 published，供"重发后重新等待回复"使用。
    ///
    /// 必须走本方法而不是 MarkPublishedAsync / MarkQueuedAsync：那两个方法都有终态保护，
    /// 命令一旦进入 completed / failed / timed_out 就会直接 return，不会重置；
    /// 而 WaitForCompletionAsync 发现命令已处于终态时会立即返回、根本不等待。
    /// 因此重试前若不先重置，第二次等待会瞬间结束，重试形同虚设。
    /// </summary>
    public async Task ResetForRetryAsync(string deviceId, long cmdSeq)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        if (cmdSeq <= 0)
            throw new ArgumentOutOfRangeException(nameof(cmdSeq), "命令序号必须大于 0");

        var key = RedisKeys.DeviceCommandStatus(deviceId, cmdSeq);
        var existing = await _redis.HashToObjectAsync<CmdInfo>(key);
        if (existing is null)
            return;

        existing.Status = CommandStates.Published;
        existing.CompletedAtUtc = null;
        existing.TimedOutAtUtc = null;
        existing.ResultCode = null;
        existing.ResultMessage = null;
        existing.ReplyType = null;
        existing.ReplyJson = null;
        await _redis.ObjectToHashAsync(key, existing, CommandTtl);
    }

    public async Task<CmdInfo> MarkTimedOutAsync(CmdInfo command, TimeSpan timeout)
    {
        var current = await GetAsync(command.DeviceId, command.Seq) ?? command;
        if (CommandStates.IsTerminal(current.Status))
            return current;

        current.Status = CommandStates.TimedOut;
        current.TimedOutAtUtc = DateTime.UtcNow;
        current.CompletedAtUtc = null;
        current.ResultCode = null;
        current.ResultMessage = $"设备在 {timeout.TotalSeconds:0.###} 秒内未回复 CmdSeq={command.Seq}";
        await _redis.ObjectToHashAsync(current.StatusKey, current, CommandTtl);

        SignalCompletion(current);
        return current;
    }

    /// <summary>
    /// 为 start_aging 的每次重发分配一个新的下发 Seq（只取号，不写 Stream）。
    ///
    /// 必须复用 lgw:cmd:seq 同一计数器：另开计数器可能与该计数器未来产生的命令 Seq 撞号，
    /// 导致 lgw:device:{id}:cmd:{seq} 命令 Hash 互相覆盖。
    /// 副作用：计数器里并非每个 Seq 都有对应的 Stream Entry，排查时不要假设一一对应。
    /// </summary>
    public Task<long> NextAttemptSeqAsync()
        => _redis.NextSequenceAsync(RedisKeys.CommandSequence);

    /// <summary>
    /// 记录 start_aging 的重发进度：已用重发次数与最后一次实际下发 Seq，不改动 Status。
    /// 只写这两个字段而不是整对象回写，避免覆盖并发的 CompleteAsync 状态。
    /// </summary>
    public async Task UpdateAttemptProgressAsync(
        string deviceId,
        long logicalSeq,
        int attemptIndex,
        long attemptSeq)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        if (logicalSeq <= 0)
            throw new ArgumentOutOfRangeException(nameof(logicalSeq), "命令序号必须大于 0");
        if (attemptIndex <= 0)
            throw new ArgumentOutOfRangeException(nameof(attemptIndex), "尝试序号必须大于 0");

        var key = RedisKeys.DeviceCommandStatus(deviceId, logicalSeq);
        var exists = await _redis.HashGetAsync(key, nameof(CmdInfo.Seq));
        if (exists.IsNullOrEmpty)
            return;

        await _redis.HashSetAsync(
            key,
            [
                new HashEntry(nameof(CmdInfo.RetryCount), Math.Max(0, attemptIndex - 1)),
                new HashEntry(nameof(CmdInfo.LastAttemptSeq), attemptSeq)
            ]);
    }

    /// <summary>
    /// 把 start_aging 多次下发中最后一次的结果回写到“逻辑命令记录”（Stream 里那条 Seq 对应的 Hash）。
    ///
    /// 为什么需要回写：重发使用新的下发 Seq（CmdSeq 每次不同，回复才能精确关联），
    /// 但 CurrentAgingSeq、群控 RunSeq、批次 ACK 都绑定最初那条逻辑 Seq，平台也只读逻辑记录，
    /// 因此最终结论必须落到逻辑 Seq 上。
    ///
    /// 注意两点：
    /// 1. 刻意不做终态保护——耗尽重发时逻辑记录已是 timed_out，必须能改成 failed；
    /// 2. 但 completed 不能被降级：更早某次尝试的迟到回复可能已把逻辑记录写成 completed，
    ///    此时即使最后一次尝试超时，也必须保留 completed。
    /// </summary>
    public async Task<CmdInfo> FinalizeAttemptsAsync(
        string deviceId,
        long logicalSeq,
        CmdInfo finalAttempt,
        int attempts,
        string? exhaustedReason)
    {
        ArgumentNullException.ThrowIfNull(finalAttempt);

        deviceId = RedisKeys.Normalize(deviceId);
        if (logicalSeq <= 0)
            throw new ArgumentOutOfRangeException(nameof(logicalSeq), "命令序号必须大于 0");
        if (attempts <= 0)
            throw new ArgumentOutOfRangeException(nameof(attempts), "下发次数必须大于 0");

        var key = RedisKeys.DeviceCommandStatus(deviceId, logicalSeq);
        var existing = await _redis.HashToObjectAsync<CmdInfo>(key) ?? new CmdInfo
        {
            Seq = logicalSeq,
            DeviceId = deviceId,
            CreatedAtUtc = DateTime.UtcNow
        };

        var keepCompleted =
            string.Equals(existing.Status, CommandStates.Completed, StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(finalAttempt.Status, CommandStates.Completed, StringComparison.OrdinalIgnoreCase);

        if (!keepCompleted)
        {
            existing.Status = exhaustedReason is not null ? CommandStates.Failed : finalAttempt.Status;
            existing.ResultCode = finalAttempt.ResultCode;
            existing.ResultMessage = exhaustedReason ?? finalAttempt.ResultMessage;
            existing.ReplyType = finalAttempt.ReplyType;
            existing.ReplyJson = finalAttempt.ReplyJson;
            existing.TimedOutAtUtc = finalAttempt.TimedOutAtUtc;
            existing.CompletedAtUtc = CommandStates.IsTerminal(existing.Status)
                ? finalAttempt.CompletedAtUtc ?? DateTime.UtcNow
                : null;
        }

        existing.RetryCount = Math.Max(0, attempts - 1);
        existing.LastAttemptSeq = finalAttempt.Seq;
        await _redis.ObjectToHashAsync(key, existing, CommandTtl);

        SignalCompletion(existing);
        return existing;
    }

    /// <summary>
    /// 等待对应 DeviceId + CmdSeq 的设备回复。
    /// 处理器完成 Redis 落库后会直接唤醒等待任务；同时在注册等待器后再次读取 Redis，
    /// 避免“回复刚好发生在注册瞬间”导致漏通知。
    /// 返回 null 表示等待超时；取消则抛 OperationCanceledException。
    /// </summary>
    public async Task<CmdInfo?> WaitForCompletionAsync(
        string deviceId,
        long cmdSeq,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        if (cmdSeq <= 0)
            throw new ArgumentOutOfRangeException(nameof(cmdSeq));
        if (timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout));

        var waiterKey = BuildWaiterKey(deviceId, cmdSeq);
        var waiter = new TaskCompletionSource<CmdInfo>(TaskCreationOptions.RunContinuationsAsynchronously);

        if (!_completionWaiters.TryAdd(waiterKey, waiter))
            throw new InvalidOperationException($"命令已存在等待器：DeviceId={deviceId}, CmdSeq={cmdSeq}");

        try
        {
            // 先注册，再检查 Redis，可消除极速回复的竞态窗口。
            var current = await GetAsync(deviceId, cmdSeq);
            if (current is not null && CommandStates.IsTerminal(current.Status))
                return current;

            var timeoutTask = Task.Delay(timeout, cancellationToken);
            var completedTask = await Task.WhenAny(waiter.Task, timeoutTask);

            if (completedTask == waiter.Task)
                return await waiter.Task;

            cancellationToken.ThrowIfCancellationRequested();

            // 超时边界再次检查一次 Redis，防止回复已经落库但进程内通知尚未来得及执行。
            current = await GetAsync(deviceId, cmdSeq);
            if (current is not null && CommandStates.IsTerminal(current.Status))
                return current;

            return null;
        }
        finally
        {
            if (_completionWaiters.TryGetValue(waiterKey, out var registeredWaiter) &&
                ReferenceEquals(registeredWaiter, waiter))
            {
                _completionWaiters.TryRemove(waiterKey, out _);
            }
        }
    }

    /// <summary>
    /// 根据设备回复中的 CmdSeq 完成对应命令。
    /// 回复原文的分类存储由 DeviceRepository.SaveCommandReplyAsync 负责。
    /// </summary>
    public async Task CompleteAsync(
        string deviceId,
        long cmdSeq,
        string replyType,
        int code,
        string? message,
        string rawReply)
    {
        deviceId = RedisKeys.Normalize(deviceId);
        replyType = RedisKeys.NormalizeReplyType(replyType);

        var key = RedisKeys.DeviceCommandStatus(deviceId, cmdSeq);
        var existing = await _redis.HashToObjectAsync<CmdInfo>(key) ?? new CmdInfo
        {
            Seq = cmdSeq,
            DeviceId = deviceId,
            CreatedAtUtc = DateTime.UtcNow
        };

        // 已成功完成的命令不能被后续重复/迟到失败回复降级。
        // timed_out 后如果迟到回复到达，则允许用真实设备结果覆盖 timed_out，便于追溯。
        if (!string.Equals(existing.Status, CommandStates.Completed, StringComparison.OrdinalIgnoreCase) || code == 1)
        {
            existing.Status = code == 1 ? CommandStates.Completed : CommandStates.Failed;
            existing.CompletedAtUtc = DateTime.UtcNow;
            existing.TimedOutAtUtc = null;
            existing.ResultCode = code;
            existing.ResultMessage = message;
            existing.ReplyType = replyType;
            existing.ReplyJson = rawReply;
            await _redis.ObjectToHashAsync(key, existing, CommandTtl);
        }

        SignalCompletion(existing);
    }

    public Task AcknowledgeAsync(RedisValue messageId)
        => _redis.StreamAcknowledgeAsync(RedisKeys.CommandQueue, ConsumerGroup, messageId);

    public Task<CmdInfo?> GetAsync(string deviceId, long cmdSeq)
        => _redis.HashToObjectAsync<CmdInfo>(RedisKeys.DeviceCommandStatus(deviceId, cmdSeq));

    private void SignalCompletion(CmdInfo command)
    {
        var waiterKey = BuildWaiterKey(command.DeviceId, command.Seq);
        if (_completionWaiters.TryGetValue(waiterKey, out var waiter))
            waiter.TrySetResult(command);
    }

    private static string BuildWaiterKey(string deviceId, long cmdSeq)
        => $"{RedisKeys.Normalize(deviceId)}:{cmdSeq}";

    /// <summary>
    /// Redis Stream 中的最新命令输入格式。仅用于入口解析；
    /// 展开后继续使用原有 CmdInfo，因此不会把批量概念扩散到 MQTT 和回复处理代码。
    /// </summary>
    private sealed class BatchCommandEnvelope
    {
        public long Seq { get; set; }
        public BatchDeviceInfo[]? DeviceInfos { get; set; }
        public string Action { get; set; } = string.Empty;
        public Newtonsoft.Json.Linq.JToken? Data { get; set; }
        public string Status { get; set; } = CommandStates.Queued;
        public string? CreatedAtUtc { get; set; }
    }

    private sealed class BatchDeviceInfo
    {
        public string DeviceId { get; set; } = string.Empty;
        public string RecordId { get; set; } = string.Empty;
    }

    private static BatchDeviceInfo[] NormalizeDeviceInfos(BatchDeviceInfo[]? source)
    {
        if (source is null || source.Length == 0)
            return [];

        var result = new List<BatchDeviceInfo>(source.Length);
        var seenDeviceIds = new HashSet<string>(StringComparer.Ordinal);

        foreach (var item in source)
        {
            if (item is null || string.IsNullOrWhiteSpace(item.DeviceId))
                continue;

            var deviceId = RedisKeys.Normalize(item.DeviceId);
            if (!seenDeviceIds.Add(deviceId))
                throw new InvalidOperationException($"DeviceInfos 中存在重复 DeviceId：{deviceId}");

            result.Add(new BatchDeviceInfo
            {
                DeviceId = deviceId,
                RecordId = item.RecordId?.Trim() ?? string.Empty
            });
        }

        return [.. result];
    }

    private static DateTime ParseCreatedAtUtc(string? value)
    {
        if (DateTime.TryParse(
            value,
            System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal,
            out var parsed))
        {
            return parsed;
        }

        // CreatedAtUtc 只用于记录，不影响命令下发。格式异常时使用当前 UTC，
        // 避免因为一个时间字符串导致整条批量命令无法分发。
        return DateTime.UtcNow;
    }

    private static string NormalizeAction(string action)
    {
        if (string.IsNullOrWhiteSpace(action))
            throw new ArgumentException("Action 不能为空", nameof(action));

        var value = action.Trim().ToLowerInvariant();
        if (value.Any(ch => !(char.IsLetterOrDigit(ch) || ch == '_')))
            throw new ArgumentException("Action 只能包含字母、数字和下划线", nameof(action));
        return value;
    }
}
