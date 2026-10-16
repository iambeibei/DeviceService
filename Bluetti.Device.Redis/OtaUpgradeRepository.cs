using Bluetti.Device.Redis.Models;
using StackExchange.Redis;

namespace Bluetti.Device.Redis;

/// <summary>
/// OTA 批量升级批次仓储。批次按老化架隔离，批次号直接复用 ota 指令 Seq。
/// </summary>
public sealed class OtaUpgradeRepository
{
    private readonly RedisHelper _redis;

    public OtaUpgradeRepository(RedisHelper redis) => _redis = redis;

    public Task<bool> RegisterRackAsync(string agingNumber)
        => _redis.SetAddAsync(RedisKeys.OtaRacks, RedisKeys.NormalizeAgingNumber(agingNumber));

    public async Task<string[]> GetRackNumbersAsync()
        => await _redis.SetMembersAsync(RedisKeys.OtaRacks);

    public async Task<OtaBatchState?> GetBatchStateAsync(string agingNumber, long seq)
    {
        var state = await _redis.HashToObjectAsync<OtaBatchState>(
            RedisKeys.OtaBatchState(agingNumber, seq));
        return state?.Seq > 0 ? state : null;
    }

    public async Task SaveBatchStateAsync(string agingNumber, OtaBatchState state)
    {
        state.AgingNumber = RedisKeys.NormalizeAgingNumber(agingNumber);
        state.UpdatedAtUtc = DateTime.UtcNow;
        await _redis.ObjectToHashAsync(
            RedisKeys.OtaBatchState(agingNumber, state.Seq),
            state,
            RedisTtl.OtaBatch);
    }

    /// <summary>
    /// 读取某架当前活跃批次号；没有活跃批次返回 null。
    /// </summary>
    public async Task<long?> GetActiveBatchSeqAsync(string agingNumber)
    {
        var raw = await _redis.StringGetAsync(RedisKeys.OtaActiveBatch(agingNumber));
        return raw is not null && long.TryParse(raw, out var seq) && seq > 0 ? seq : null;
    }

    public async Task SetActiveBatchAsync(string agingNumber, long seq)
    {
        await _redis.StringSetAsync(
            RedisKeys.OtaActiveBatch(agingNumber),
            seq.ToString(),
            RedisTtl.OtaBatch);
    }

    public Task<bool> ClearActiveBatchAsync(string agingNumber)
        => _redis.KeyDeleteAsync(RedisKeys.OtaActiveBatch(agingNumber));

    public async Task AddDispatchedAsync(string agingNumber, long seq, string deviceId)
    {
        var key = RedisKeys.OtaBatchDispatched(agingNumber, seq);
        await _redis.SetAddAsync(key, RedisKeys.Normalize(deviceId));
        await _redis.KeyExpireAsync(key, RedisTtl.OtaBatch);
    }

    public async Task<IReadOnlySet<string>> GetDispatchedAsync(string agingNumber, long seq)
        => new HashSet<string>(
            await _redis.SetMembersAsync(RedisKeys.OtaBatchDispatched(agingNumber, seq)),
            StringComparer.Ordinal);

    public async Task AddCompletedAsync(string agingNumber, long seq, string deviceId)
    {
        var key = RedisKeys.OtaBatchCompleted(agingNumber, seq);
        await _redis.SetAddAsync(key, RedisKeys.Normalize(deviceId));
        await _redis.KeyExpireAsync(key, RedisTtl.OtaBatch);
    }

    public async Task<IReadOnlySet<string>> GetCompletedAsync(string agingNumber, long seq)
        => new HashSet<string>(
            await _redis.SetMembersAsync(RedisKeys.OtaBatchCompleted(agingNumber, seq)),
            StringComparer.Ordinal);

    /// <summary>
    /// 群控 / OTA 直接下发的指令共用同一个全局递增序列，保证 Seq 不会与命令流冲突。
    /// </summary>
    public Task<long> NextSequenceAsync()
        => _redis.NextSequenceAsync(RedisKeys.CommandSequence);
}
