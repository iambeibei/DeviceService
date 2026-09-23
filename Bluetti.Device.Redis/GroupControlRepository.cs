using Bluetti.Device.Redis.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using StackExchange.Redis;

namespace Bluetti.Device.Redis;

/// <summary>
/// 按 AgingNumber 隔离的群控状态仓储。
/// </summary>
public sealed class GroupControlRepository
{
    private static readonly TimeSpan RunTtl = TimeSpan.FromDays(7);
    private readonly RedisHelper _redis;

    public GroupControlRepository(RedisHelper redis) => _redis = redis;

    public Task<bool> RegisterRackAsync(string agingNumber)
        => _redis.SetAddAsync(
            RedisKeys.GroupControlRacks,
            RedisKeys.NormalizeAgingNumber(agingNumber));

    public async Task<IReadOnlyList<string>> GetRackNumbersAsync()
    {
        var members = await _redis.SetMembersAsync(RedisKeys.GroupControlRacks);
        var result = new HashSet<string>(StringComparer.Ordinal);
        foreach (var member in members)
        {
            try
            {
                result.Add(RedisKeys.NormalizeAgingNumber(member));
            }
            catch (ArgumentException)
            {
                // 非法历史架号不参与恢复。
            }
        }

        return result.OrderBy(value => value, StringComparer.Ordinal).ToArray();
    }

    public async Task<GroupControlState> GetStateAsync(string agingNumber)
    {
        agingNumber = RedisKeys.NormalizeAgingNumber(agingNumber);
        return await _redis.HashToObjectAsync<GroupControlState>(
                   RedisKeys.GroupControlState(agingNumber))
               ?? new GroupControlState { AgingNumber = agingNumber };
    }

    public async Task SaveStateAsync(string agingNumber, GroupControlState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        agingNumber = RedisKeys.NormalizeAgingNumber(agingNumber);
        state.AgingNumber = agingNumber;
        state.UpdatedAtUtc = DateTime.UtcNow;

        await RegisterRackAsync(agingNumber);
        await _redis.ObjectToHashAsync(RedisKeys.GroupControlState(agingNumber), state);
    }

    public Task<GroupControlRunState?> GetRunStateAsync(string agingNumber, long runSeq)
        => _redis.HashToObjectAsync<GroupControlRunState>(
            RedisKeys.GroupControlRunState(agingNumber, runSeq));

    public async Task SaveRunStateAsync(
        string agingNumber,
        GroupControlRunState runState)
    {
        ArgumentNullException.ThrowIfNull(runState);
        agingNumber = RedisKeys.NormalizeAgingNumber(agingNumber);
        if (runState.RunSeq <= 0)
            throw new ArgumentOutOfRangeException(nameof(runState.RunSeq));

        runState.AgingNumber = agingNumber;
        runState.UpdatedAtUtc = DateTime.UtcNow;
        await _redis.ObjectToHashAsync(
            RedisKeys.GroupControlRunState(agingNumber, runState.RunSeq),
            runState,
            RunTtl);
    }

    public async Task AddRunParticipantsAsync(
        string agingNumber,
        long runSeq,
        IEnumerable<string> deviceIds)
    {
        var key = RedisKeys.GroupControlRunParticipants(agingNumber, runSeq);
        foreach (var deviceId in deviceIds.Distinct(StringComparer.Ordinal))
            await _redis.SetAddAsync(key, RedisKeys.Normalize(deviceId));

        await _redis.KeyExpireAsync(key, RunTtl);
    }

    public async Task<IReadOnlySet<string>> GetRunParticipantsAsync(
        string agingNumber,
        long runSeq)
        => new HashSet<string>(
            await _redis.SetMembersAsync(
                RedisKeys.GroupControlRunParticipants(agingNumber, runSeq)),
            StringComparer.Ordinal);

    public Task<bool> AddRunStartedDeviceAsync(
        string agingNumber,
        long runSeq,
        string deviceId)
        => AddRunSetMemberAsync(
            RedisKeys.GroupControlRunStartedDevices(agingNumber, runSeq),
            deviceId);

    public async Task<IReadOnlySet<string>> GetRunCompletedDeviceIdsAsync(
        string agingNumber,
        long runSeq)
        => new HashSet<string>(
            await _redis.SetMembersAsync(
                RedisKeys.GroupControlRunCompletedDevices(agingNumber, runSeq)),
            StringComparer.Ordinal);

    public Task<bool> AddRunCompletedDeviceAsync(
        string agingNumber,
        long runSeq,
        string deviceId)
        => AddRunSetMemberAsync(
            RedisKeys.GroupControlRunCompletedDevices(agingNumber, runSeq),
            deviceId);

    public Task ClearRunCompletedDevicesAsync(string agingNumber, long runSeq)
        => _redis.KeyDeleteAsync(
            RedisKeys.GroupControlRunCompletedDevices(agingNumber, runSeq));

    public async Task<IReadOnlySet<string>> GetRunTerminalDeviceIdsAsync(
        string agingNumber,
        long runSeq)
        => new HashSet<string>(
            await _redis.SetMembersAsync(
                RedisKeys.GroupControlRunTerminalDevices(agingNumber, runSeq)),
            StringComparer.Ordinal);

    public Task<bool> AddRunTerminalDeviceAsync(
        string agingNumber,
        long runSeq,
        string deviceId)
        => AddRunSetMemberAsync(
            RedisKeys.GroupControlRunTerminalDevices(agingNumber, runSeq),
            deviceId);

    public async Task<IReadOnlySet<string>> GetOnlinePointDeviceIdsAsync(string agingNumber)
    {
        agingNumber = RedisKeys.NormalizeAgingNumber(agingNumber);
        var items = await _redis.ListRangeAsync(
            RedisKeys.AgingRackOnlineDeviceList(agingNumber));

        return ParseDeviceIds(items, expectedType: "1", agingNumber);
    }

    public async Task<IReadOnlyList<string>> GetOnlineControllerIdsAsync(string agingNumber)
    {
        agingNumber = RedisKeys.NormalizeAgingNumber(agingNumber);
        var items = await _redis.ListRangeAsync(RedisKeys.ControlOnlineDeviceList);

        return ParseDeviceIds(items, expectedType: "2", agingNumber)
            .OrderBy(value => value, StringComparer.Ordinal)
            .ToArray();
    }

    public async Task<IReadOnlySet<string>> GetCompletedDeviceIdsAsync(string agingNumber)
        => new HashSet<string>(
            await _redis.SetMembersAsync(
                RedisKeys.GroupControlCompletedDevices(
                    RedisKeys.NormalizeAgingNumber(agingNumber))),
            StringComparer.Ordinal);

    public Task<bool> AddCompletedDeviceAsync(string agingNumber, string deviceId)
        => _redis.SetAddAsync(
            RedisKeys.GroupControlCompletedDevices(
                RedisKeys.NormalizeAgingNumber(agingNumber)),
            RedisKeys.Normalize(deviceId));

    public Task ClearCompletedDevicesAsync(string agingNumber)
        => _redis.KeyDeleteAsync(
            RedisKeys.GroupControlCompletedDevices(
                RedisKeys.NormalizeAgingNumber(agingNumber)));

    public async Task<long?> GetLastTaskSeqAsync(string agingNumber, string deviceId)
    {
        var value = await _redis.HashGetAsync(
            RedisKeys.GroupControlLastTaskSeq(
                RedisKeys.NormalizeAgingNumber(agingNumber)),
            RedisKeys.Normalize(deviceId));

        return !value.IsNullOrEmpty && long.TryParse(value.ToString(), out var seq)
            ? seq
            : null;
    }

    public Task SetLastTaskSeqAsync(string agingNumber, string deviceId, long seq)
    {
        if (seq <= 0)
            throw new ArgumentOutOfRangeException(nameof(seq), "TaskCP Seq 必须大于 0");

        return _redis.HashSetAsync(
            RedisKeys.GroupControlLastTaskSeq(
                RedisKeys.NormalizeAgingNumber(agingNumber)),
            [new HashEntry(RedisKeys.Normalize(deviceId), seq)]);
    }

    public Task<long> NextControlSequenceAsync()
        => _redis.NextSequenceAsync(RedisKeys.CommandSequence);

    public static string SerializePending(PendingGroupControlCommand command)
        => JsonConvert.SerializeObject(command);

    public static PendingGroupControlCommand? DeserializePending(string? value)
        => string.IsNullOrWhiteSpace(value)
            ? null
            : JsonConvert.DeserializeObject<PendingGroupControlCommand>(value);

    private async Task<bool> AddRunSetMemberAsync(string key, string deviceId)
    {
        var added = await _redis.SetAddAsync(key, RedisKeys.Normalize(deviceId));
        await _redis.KeyExpireAsync(key, RunTtl);
        return added;
    }

    private static HashSet<string> ParseDeviceIds(
        IEnumerable<string> items,
        string expectedType,
        string agingNumber)
    {
        var result = new HashSet<string>(StringComparer.Ordinal);

        foreach (var item in items)
        {
            if (string.IsNullOrWhiteSpace(item))
                continue;

            try
            {
                var json = JObject.Parse(item);
                var id = json.GetValue("id", StringComparison.OrdinalIgnoreCase)?.Value<string>();
                var type = json.GetValue("type", StringComparison.OrdinalIgnoreCase)?.Value<string>();
                var numberToken = json.GetValue("AgingNumber", StringComparison.OrdinalIgnoreCase);
                var number = numberToken?.Type == JTokenType.String
                    ? numberToken.Value<string>()
                    : null;

                if (!string.IsNullOrWhiteSpace(id) &&
                    string.Equals(type, expectedType, StringComparison.Ordinal) &&
                    number is not null &&
                    string.Equals(
                        RedisKeys.NormalizeAgingNumber(number),
                        agingNumber,
                        StringComparison.Ordinal))
                {
                    result.Add(RedisKeys.Normalize(id));
                }
            }
            catch (JsonException)
            {
                // 非法历史元素不参与群控。
            }
            catch (ArgumentException)
            {
                // 非法 DeviceId 不参与群控。
            }
        }

        return result;
    }
}
