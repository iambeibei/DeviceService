using Bluetti.Device.Redis.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using StackExchange.Redis;

namespace Bluetti.Device.Redis;

/// <summary>
/// OTA 分层升级批次仓储。批次按老化架隔离，批次号直接复用 ota 指令 Seq。
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

    /// <summary>
    /// 初始化升级名单。名单是平台 DeviceInfos 全集，也是成功率统计的分母。
    /// </summary>
    public async Task SetTargetsAsync(
        string agingNumber,
        long seq,
        IEnumerable<string> deviceIds,
        string status)
    {
        var entries = deviceIds
            .Select(RedisKeys.Normalize)
            .Distinct(StringComparer.Ordinal)
            .Select(deviceId => new HashEntry(deviceId, status))
            .ToArray();

        if (entries.Length == 0)
            return;

        var key = RedisKeys.OtaBatchTargets(agingNumber, seq);
        await _redis.HashSetAsync(key, entries);
        await _redis.KeyExpireAsync(key, RedisTtl.OtaBatch);
    }

    public async Task SetTargetStatusAsync(
        string agingNumber,
        long seq,
        string deviceId,
        string status)
    {
        var key = RedisKeys.OtaBatchTargets(agingNumber, seq);
        await _redis.HashSetAsync(
            key,
            [new HashEntry(RedisKeys.Normalize(deviceId), status)]);
        await _redis.KeyExpireAsync(key, RedisTtl.OtaBatch);
    }

    public async Task<Dictionary<string, string>> GetTargetsAsync(string agingNumber, long seq)
    {
        var members = await _redis.HashGetAllAsync(RedisKeys.OtaBatchTargets(agingNumber, seq));
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var member in members)
        {
            var id = member.Name.ToString();
            if (!string.IsNullOrWhiteSpace(id))
                result[id] = member.Value.ToString();
        }

        return result;
    }

    /// <summary>保存名单中每台设备的 RecordId 与指令 Data，下发 ota 时原样带出。</summary>
    public async Task SetRecordIdAsync(
        string agingNumber,
        long seq,
        string deviceId,
        string recordId)
    {
        var key = RedisKeys.OtaBatchRecordIds(agingNumber, seq);
        await _redis.HashSetAsync(
            key,
            [new HashEntry(RedisKeys.Normalize(deviceId), recordId)]);
        await _redis.KeyExpireAsync(key, RedisTtl.OtaBatch);
    }

    public async Task<string?> GetRecordIdAsync(string agingNumber, long seq, string deviceId)
    {
        var raw = await _redis.HashGetAsync(
            RedisKeys.OtaBatchRecordIds(agingNumber, seq),
            RedisKeys.Normalize(deviceId));

        return raw.IsNullOrEmpty ? null : raw.ToString();
    }

    /// <summary>记录某台设备在本层升级前的固件版本号，供升级后对比判断是否重启成功。</summary>
    public async Task SetLevelVersionAsync(
        string agingNumber,
        long seq,
        int level,
        string deviceId,
        string version)
    {
        var key = RedisKeys.OtaBatchLevelVersions(agingNumber, seq, level);
        await _redis.HashSetAsync(
            key,
            [new HashEntry(RedisKeys.Normalize(deviceId), version)]);
        await _redis.KeyExpireAsync(key, RedisTtl.OtaBatch);
    }

    public async Task<string?> GetLevelVersionAsync(
        string agingNumber,
        long seq,
        int level,
        string deviceId)
    {
        var raw = await _redis.HashGetAsync(
            RedisKeys.OtaBatchLevelVersions(agingNumber, seq, level),
            RedisKeys.Normalize(deviceId));

        return raw.IsNullOrEmpty ? null : raw.ToString();
    }

    /// <summary>
    /// 写入本层的成员快照（进入该层时算一次）。已存在则追加，天然幂等。
    /// 见 <see cref="RedisKeys.OtaBatchLevelMembers"/> 的注释：分组必须基于快照，
    /// 否则设备掉线被移出实时层清单后，本层会被误判成"全部完成"。
    /// </summary>
    public async Task SetLevelMembersAsync(
        string agingNumber,
        long seq,
        int level,
        IEnumerable<string> deviceIds)
    {
        var key = RedisKeys.OtaBatchLevelMembers(agingNumber, seq, level);

        var members = deviceIds
            .Where(deviceId => !string.IsNullOrWhiteSpace(deviceId))
            .Select(RedisKeys.Normalize)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        if (members.Length == 0)
            return;

        await _redis.SetAddAsync(key, members);
        await _redis.KeyExpireAsync(key, RedisTtl.OtaBatch);
    }

    /// <summary>读取本层的成员快照；返回空数组表示该层尚未建立快照。</summary>
    public async Task<string[]> GetLevelMembersAsync(string agingNumber, long seq, int level)
        => await _redis.SetMembersAsync(RedisKeys.OtaBatchLevelMembers(agingNumber, seq, level));

    /// <summary>记录当前层已下发 ota 指令的设备。进入下一层时清空。</summary>
    public async Task AddDispatchedAsync(string agingNumber, long seq, string deviceId)
    {
        var key = RedisKeys.OtaBatchDispatched(agingNumber, seq);
        await _redis.SetAddAsync(key, RedisKeys.Normalize(deviceId));
        await _redis.KeyExpireAsync(key, RedisTtl.OtaBatch);
    }

    /// <summary>清空当前层的已下发集合，准备记录下一层。</summary>
    public Task<bool> ClearDispatchedAsync(string agingNumber, long seq)
        => _redis.KeyDeleteAsync(RedisKeys.OtaBatchDispatched(agingNumber, seq));

    /// <summary>累计记录本批下发过的设备，只用于收尾补发时与 targets 取差集。</summary>
    public async Task AddDispatchedAllAsync(string agingNumber, long seq, string deviceId)
    {
        var key = RedisKeys.OtaBatchDispatchedAll(agingNumber, seq);
        await _redis.SetAddAsync(key, RedisKeys.Normalize(deviceId));
        await _redis.KeyExpireAsync(key, RedisTtl.OtaBatch);
    }

    public async Task<IReadOnlySet<string>> GetDispatchedAsync(string agingNumber, long seq)
        => new HashSet<string>(
            await _redis.SetMembersAsync(RedisKeys.OtaBatchDispatched(agingNumber, seq)),
            StringComparer.Ordinal);

    public async Task<IReadOnlySet<string>> GetDispatchedAllAsync(string agingNumber, long seq)
        => new HashSet<string>(
            await _redis.SetMembersAsync(RedisKeys.OtaBatchDispatchedAll(agingNumber, seq)),
            StringComparer.Ordinal);

    /// <summary>
    /// 读取某架某 Mesh 层级的设备清单 lgw:ota:{AgingNumber}:{MeshLevel}，
    /// 元素与其它在线列表同构：{"id":"...","type":"...","AgingNumber":"88","MeshLevel":1}
    /// </summary>
    public async Task<IReadOnlySet<string>> GetMeshLevelDeviceIdsAsync(
        string agingNumber,
        int level)
    {
        var items = await _redis.ListRangeAsync(
            RedisKeys.OtaMeshLevelDevices(agingNumber, level));

        var result = new HashSet<string>(StringComparer.Ordinal);
        foreach (var item in items)
        {
            if (string.IsNullOrWhiteSpace(item))
                continue;

            try
            {
                var json = JObject.Parse(item);
                var id = json.GetValue("id", StringComparison.OrdinalIgnoreCase)?.Value<string>();
                if (!string.IsNullOrWhiteSpace(id))
                    result.Add(RedisKeys.Normalize(id));
            }
            catch (JsonException)
            {
                // 非法历史元素不参与升级名单匹配。
            }
        }

        return result;
    }

    /// <summary>
    /// 群控 / OTA 直接下发的指令共用同一个全局递增序列，保证 Seq 不会与命令流冲突。
    /// </summary>
    public Task<long> NextSequenceAsync()
        => _redis.NextSequenceAsync(RedisKeys.CommandSequence);
}
