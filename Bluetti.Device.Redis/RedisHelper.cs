using Newtonsoft.Json;
using StackExchange.Redis;
using System.Globalization;
using System.Reflection;

namespace Bluetti.Device.Redis;

/// <summary>
/// Redis 底层访问封装。业务键名和设备隔离由 Repository/Helper 决定。
/// </summary>
public sealed class RedisHelper
{
    private readonly IDatabase _db;
    private readonly ISubscriber _subscriber;

    public RedisHelper(IConnectionMultiplexer redis)
    {
        _db = redis.GetDatabase();
        _subscriber = redis.GetSubscriber();
    }

    public async Task ObjectToHashAsync<T>(string hashKey, T model, TimeSpan? expire = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(model);

        var entries = model.GetType()
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead)
            .Select(p => new HashEntry(p.Name, SerializeHashValue(p.GetValue(model), p.PropertyType)))
            .ToArray();

        if (entries.Length > 0)
            await _db.HashSetAsync(hashKey, entries);

        if (expire.HasValue)
            await _db.KeyExpireAsync(hashKey, expire.Value);
    }

    public async Task<T?> HashToObjectAsync<T>(string hashKey) where T : class, new()
    {
        var hashData = await _db.HashGetAllAsync(hashKey);
        if (hashData.Length == 0)
            return null;

        var fields = hashData.ToDictionary(x => x.Name.ToString(), x => x.Value.ToString(), StringComparer.OrdinalIgnoreCase);
        var model = new T();

        foreach (var property in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.CanWrite))
        {
            if (!fields.TryGetValue(property.Name, out var raw))
                continue;

            property.SetValue(model, DeserializeHashValue(raw, property.PropertyType));
        }

        return model;
    }

    public Task<bool> SetAddAsync(string key, string value) => _db.SetAddAsync(key, value);

    public Task<bool> SetRemoveAsync(string key, string value) => _db.SetRemoveAsync(key, value);

    public async Task<string[]> SetMembersAsync(string key)
        => (await _db.SetMembersAsync(key)).Select(x => x.ToString()).ToArray();

    public Task<bool> SortedSetAddAsync(string key, string member, double score)
        => _db.SortedSetAddAsync(key, member, score);

    public Task<RedisValue> HashGetAsync(string key, string field) => _db.HashGetAsync(key, field);

    public Task HashSetAsync(string key, HashEntry[] entries) => _db.HashSetAsync(key, entries);

    public Task<bool> HashSetIfNotExistsAsync(string key, string field, string value)
        => _db.HashSetAsync(key, field, value, When.NotExists);

    public Task<bool> HashDeleteAsync(string key, string field)
        => _db.HashDeleteAsync(key, field);

    public Task<bool> KeyDeleteAsync(string key) => _db.KeyDeleteAsync(key);

    public Task<bool> KeyExpireAsync(string key, TimeSpan expire) => _db.KeyExpireAsync(key, expire);

    public Task<bool> StringSetAsync(string key, string value, TimeSpan? expiry = null)
        => _db.StringSetAsync(
            key,
            value,
            expiry,
            keepTtl: false,
            when: When.Always,
            flags: CommandFlags.None);

    public async Task<string?> StringGetAsync(string key)
    {
        var value = await _db.StringGetAsync(key);
        return value.IsNull ? null : value.ToString();
    }

    public Task<long> ListRightPushAsync(string key, string value)
        => _db.ListRightPushAsync(key, value);

    public async Task<string[]> ListRangeAsync(string key, long start = 0, long stop = -1)
        => (await _db.ListRangeAsync(key, start, stop))
            .Select(value => value.ToString())
            .ToArray();

    public Task<long> ListRemoveAsync(string key, string value, long count = 0)
        => _db.ListRemoveAsync(key, value, count);

    public async Task<long> NextSequenceAsync(string key)
    {
        var value = await _db.StringIncrementAsync(key);
        if (value > int.MaxValue)
            throw new InvalidOperationException($"命令序号超过 ESP32 cJSON valueint 可表示范围：{value}");
        return value;
    }

    public Task<RedisValue> StreamAddAsync(
        string streamKey,
        NameValueEntry[] entries,
        int? maxLength = null,
        bool approximate = true)
        => _db.StreamAddAsync(streamKey, entries, maxLength: maxLength, useApproximateMaxLength: approximate);

    public async Task ObjectToStreamAsync<T>(string streamKey, T model, int? maxLength = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(model);
        var json = JsonConvert.SerializeObject(model);
        await StreamAddAsync(streamKey, [new NameValueEntry("data", json)], maxLength);
    }

    public async Task EnsureConsumerGroupAsync(string streamKey, string groupName, string startPosition = "0-0")
    {
        try
        {
            await _db.StreamCreateConsumerGroupAsync(streamKey, groupName, startPosition, createStream: true);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP", StringComparison.OrdinalIgnoreCase))
        {
            // 消费组已存在。
        }
    }

    public Task<StreamEntry[]> StreamReadGroupAsync(
        string streamKey,
        string groupName,
        string consumerName,
        string position,
        int count)
        => _db.StreamReadGroupAsync(streamKey, groupName, consumerName, position, count);

    public Task<long> StreamAcknowledgeAsync(string streamKey, string groupName, RedisValue messageId)
        => _db.StreamAcknowledgeAsync(streamKey, groupName, messageId);

    public Task PublishAsync(string channel, string message)
        => _subscriber.PublishAsync(channel, message);

    public Task PublishObjectAsync<T>(string channel, T value)
        => PublishAsync(channel, JsonConvert.SerializeObject(value));

    private static string SerializeHashValue(object? value, Type declaredType)
    {
        if (value is null)
            return string.Empty;

        var type = Nullable.GetUnderlyingType(declaredType) ?? declaredType;
        if (type == typeof(DateTime))
            return ((DateTime)value).ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
        if (type == typeof(DateTimeOffset))
            return ((DateTimeOffset)value).ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
        if (IsSimpleType(type))
            return Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;

        return JsonConvert.SerializeObject(value);
    }

    private static object? DeserializeHashValue(string value, Type declaredType)
    {
        var nullableType = Nullable.GetUnderlyingType(declaredType);
        var targetType = nullableType ?? declaredType;

        if (string.IsNullOrEmpty(value))
            return nullableType is not null || !targetType.IsValueType ? null : Activator.CreateInstance(targetType);

        if (targetType == typeof(string)) return value;
        if (targetType == typeof(DateTime)) return DateTime.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        if (targetType == typeof(DateTimeOffset)) return DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        if (targetType == typeof(Guid)) return Guid.Parse(value);
        if (targetType.IsEnum) return Enum.Parse(targetType, value, ignoreCase: true);
        if (IsSimpleType(targetType)) return Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture);

        return JsonConvert.DeserializeObject(value, targetType);
    }

    private static bool IsSimpleType(Type type)
        => type.IsPrimitive || type.IsEnum || type == typeof(string) || type == typeof(decimal) ||
           type == typeof(Guid) || type == typeof(DateTime) || type == typeof(DateTimeOffset);
}
