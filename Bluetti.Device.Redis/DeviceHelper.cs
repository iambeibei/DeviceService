using Bluetti.Device.Redis.Models;

namespace Bluetti.Device.Redis;

/// <summary>
/// 提供给管理端/API 层读取设备运行状态、在线 JSON 列表和四类最新回复。
/// </summary>
public sealed class DeviceHelper
{
    private readonly DeviceRepository _repository;

    public DeviceHelper(DeviceRepository repository) => _repository = repository;

    public Task<DeviceNode?> GetDeviceAsync(string deviceId)
        => _repository.GetAsync(deviceId);

    public Task<IReadOnlyList<DeviceNode>> GetDevicesAsync()
        => _repository.GetAllAsync();

    /// <summary>
    /// 返回 lgw:devices:online 全量 List 中的 JSON 字符串。
    /// </summary>
    public Task<string[]> GetOnlineDeviceJsonListAsync()
        => _repository.GetOnlineDeviceJsonListAsync();

    public Task<string[]> GetAgingRackOnlineDeviceJsonListAsync(string agingNumber)
        => _repository.GetAgingRackOnlineDeviceJsonListAsync(agingNumber);

    public Task<string[]> GetControlOnlineDeviceJsonListAsync()
        => _repository.GetControlOnlineDeviceJsonListAsync();

    public Task<bool> RemoveOnlineDeviceAsync(string deviceId)
        => _repository.RemoveOnlineDeviceAsync(deviceId);

    public Task<DeviceReply?> GetPointReplyAsync(string deviceId)
        => _repository.GetLatestReplyAsync(deviceId, "point");

    public Task<DeviceReply?> GetStateReplyAsync(string deviceId)
        => _repository.GetLatestReplyAsync(deviceId, "state");

    public Task<DeviceReply?> GetPnReplyAsync(string deviceId)
        => _repository.GetLatestReplyAsync(deviceId, "pn");

    public Task<DeviceReply?> GetConfigReplyAsync(string deviceId)
        => _repository.GetLatestReplyAsync(deviceId, "config");
}
