using Bluetti.Device.Redis.Models;

namespace Bluetti.Device.Redis;

/// <summary>
/// 提供给管理端/API 层调用的命令入口。
/// </summary>
public sealed class CmdHelper
{
    private readonly CommandRepository _repository;

    public CmdHelper(CommandRepository repository) => _repository = repository;

    public async Task<long> SendCmdAsync(
        string deviceId,
        string action,
        object? data = null,
        string? recordId = null)
        => (await _repository.EnqueueAsync(deviceId, action, data, recordId)).Seq;

    public Task<CmdInfo?> GetCmdAsync(string deviceId, long cmdSeq)
        => _repository.GetAsync(deviceId, cmdSeq);
}
