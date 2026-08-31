using Newtonsoft.Json;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Channels;

namespace DeviceService.Services;

/// <summary>
/// 老化状态业务 API 回调后台队列。
/// MQTT state 处理器只负责快速入队，真正 HTTP 请求由本后台服务串行执行，
/// 避免 API 慢/重试阻塞 MQTT 消息路由和老化数据接收。
/// </summary>
public sealed class AgingRecordCallbackClient : BackgroundService
{
    private readonly Channel<AgingCallbackRequest> _queue;
    private readonly HttpClient _httpClient;
    private readonly ILogger<AgingRecordCallbackClient> _logger;
    private readonly Uri _callbackUri;
    private readonly int _retryCount;
    private readonly TimeSpan _retryDelay;

    public AgingRecordCallbackClient(
        IConfiguration configuration,
        ILogger<AgingRecordCallbackClient> logger)
    {
        _logger = logger;

        _queue = Channel.CreateUnbounded<AgingCallbackRequest>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

        var url = configuration.GetValue<string>("AgingCallback:Url")
            ?? "https://192.168.40.116:7088/DeviceServiceCallback/SaveAgingRecord";

        if (!Uri.TryCreate(url, UriKind.Absolute, out var callbackUri))
            throw new InvalidOperationException($"AgingCallback:Url 无效：{url}");

        _callbackUri = callbackUri;

        var timeoutSeconds = configuration.GetValue<double?>("AgingCallback:TimeoutSeconds") ?? 5;
        if (timeoutSeconds <= 0)
            throw new InvalidOperationException("AgingCallback:TimeoutSeconds 必须大于 0");

        _retryCount = configuration.GetValue<int?>("AgingCallback:RetryCount") ?? 3;
        if (_retryCount <= 0)
            throw new InvalidOperationException("AgingCallback:RetryCount 必须大于 0");

        var retryDelayMilliseconds = configuration.GetValue<int?>("AgingCallback:RetryDelayMilliseconds") ?? 500;
        if (retryDelayMilliseconds < 0)
            throw new InvalidOperationException("AgingCallback:RetryDelayMilliseconds 不能小于 0");
        _retryDelay = TimeSpan.FromMilliseconds(retryDelayMilliseconds);

        var allowInvalidCertificate = configuration.GetValue<bool?>("AgingCallback:AllowInvalidServerCertificate") ?? false;
        var handler = new HttpClientHandler();
        if (allowInvalidCertificate)
        {
            // 仅用于当前局域网 HTTPS 接口使用自签名/证书名称不匹配的情况。
            // 正式部署使用可信证书后应在配置中关闭。
            handler.ServerCertificateCustomValidationCallback =
                HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;

            _logger.LogWarning(
                "AgingCallback 已允许无效 HTTPS 服务器证书，仅建议用于受控局域网环境，Url={Url}",
                _callbackUri);
        }

        _httpClient = new HttpClient(handler)
        {
            Timeout = TimeSpan.FromSeconds(timeoutSeconds)
        };
        _httpClient.DefaultRequestHeaders.Accept.Add(
            new MediaTypeWithQualityHeaderValue("application/json"));
    }

    /// <summary>
    /// 将一个明确的下位机老化状态加入回调队列。
    /// agingStart -> 运行中；agingIdle -> 成功；agingStop/agingError -> 失败。
    /// 未识别状态不会入队。
    /// </summary>
    public ValueTask QueueAsync(
        string recordId,
        string agingState,
        CancellationToken cancellationToken = default)
    {
        recordId = recordId?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(recordId))
        {
            _logger.LogWarning("老化状态回调缺少 RecordId，AgingState={AgingState}", agingState);
            return ValueTask.CompletedTask;
        }

        var callbackStatus = MapStatus(agingState);
        if (callbackStatus is null)
            return ValueTask.CompletedTask;

        return _queue.Writer.WriteAsync(
            new AgingCallbackRequest(recordId, agingState.Trim(), callbackStatus),
            cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("老化状态 API 回调后台队列已启动，Url={Url}", _callbackUri);

        try
        {
            await foreach (var request in _queue.Reader.ReadAllAsync(stoppingToken))
            {
                try
                {
                    _ = await SendWithRetryAsync(request, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "老化状态 API 回调后台处理异常，RecordId={RecordId}, AgingState={AgingState}",
                        request.RecordId,
                        request.AgingState);
                }
            }
        }
        finally
        {
            _logger.LogInformation("老化状态 API 回调后台队列已停止");
        }
    }

    private async Task<bool> SendWithRetryAsync(
        AgingCallbackRequest request,
        CancellationToken cancellationToken)
    {
        var payload = JsonConvert.SerializeObject(new
        {
            RecordId = request.RecordId,
            Statu = request.CallbackStatus
        });

        for (var attempt = 1; attempt <= _retryCount; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                using var content = new StringContent(payload, Encoding.UTF8, "application/json");
                using var response = await _httpClient.PostAsync(_callbackUri, content, cancellationToken);

                if (response.IsSuccessStatusCode)
                {
                    _logger.LogInformation(
                        "老化状态 API 回调成功，RecordId={RecordId}, AgingState={AgingState}, Statu={Statu}, Attempt={Attempt}",
                        request.RecordId,
                        request.AgingState,
                        request.CallbackStatus,
                        attempt);
                    return true;
                }

                var responseText = await response.Content.ReadAsStringAsync(cancellationToken);
                _logger.LogWarning(
                    "老化状态 API 回调返回非成功状态，RecordId={RecordId}, AgingState={AgingState}, Statu={Statu}, HttpStatus={HttpStatus}, Attempt={Attempt}/{RetryCount}, Response={Response}",
                    request.RecordId,
                    request.AgingState,
                    request.CallbackStatus,
                    (int)response.StatusCode,
                    attempt,
                    _retryCount,
                    responseText);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                _logger.LogWarning(
                    "老化状态 API 回调超时，RecordId={RecordId}, AgingState={AgingState}, Statu={Statu}, Attempt={Attempt}/{RetryCount}",
                    request.RecordId,
                    request.AgingState,
                    request.CallbackStatus,
                    attempt,
                    _retryCount);
            }
            catch (HttpRequestException ex)
            {
                _logger.LogWarning(
                    ex,
                    "老化状态 API 回调网络异常，RecordId={RecordId}, AgingState={AgingState}, Statu={Statu}, Attempt={Attempt}/{RetryCount}",
                    request.RecordId,
                    request.AgingState,
                    request.CallbackStatus,
                    attempt,
                    _retryCount);
            }

            if (attempt < _retryCount && _retryDelay > TimeSpan.Zero)
                await Task.Delay(_retryDelay, cancellationToken);
        }

        _logger.LogError(
            "老化状态 API 回调最终失败，RecordId={RecordId}, AgingState={AgingState}, Statu={Statu}, RetryCount={RetryCount}",
            request.RecordId,
            request.AgingState,
            request.CallbackStatus,
            _retryCount);

        return false;
    }

    public static string? MapStatus(string? agingState)
    {
        var normalized = agingState?.Trim().ToLowerInvariant();
        return normalized switch
        {
            "agingstart" => "运行中",
            "agingidle" => "成功",
            "agingstop" => "失败",
            "agingerror" => "失败",
            _ => null
        };
    }

    public override void Dispose()
    {
        _queue.Writer.TryComplete();
        _httpClient.Dispose();
        base.Dispose();
    }

    private sealed record AgingCallbackRequest(
        string RecordId,
        string AgingState,
        string CallbackStatus);
}
