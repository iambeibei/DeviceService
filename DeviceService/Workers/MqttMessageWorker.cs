using DeviceService.Handlers;
using DeviceService.Mqtt;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DeviceService.Works
{
    public class MqttMessageWorker : BackgroundService
    {
        private readonly MqttConnection _connection;
        private readonly IServiceProvider _services;
        private readonly ILogger<MqttMessageWorker> _logger;

        public MqttMessageWorker(MqttConnection connection, IServiceProvider services, ILogger<MqttMessageWorker> logger)
        {
            _connection = connection;
            _services = services;
            _logger = logger;
        }

    /// <summary>
    /// 未匹配处理器的主题计数（按主题原文聚合）。
    /// 不能声明为 readonly：汇总时需要用 Interlocked.Exchange 整体替换取快照。
    /// </summary>
    private ConcurrentDictionary<string, long> _unhandledCounts =
        new(StringComparer.Ordinal);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var reader = _connection.GetMessageReader();
        _logger.LogInformation("消息路由器已启动，开始监听通道...");

        using var unhandledTimer = new Timer(_ => ReportUnhandledTopics(),
            null, TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(60));

            await foreach (var message in reader.ReadAllAsync(stoppingToken))//没有消息时，不会反复轮询占用 CPU，而是异步挂起
            {
                try
                {
                    using var scope = _services.CreateScope();

                    var handlers = scope.ServiceProvider.GetServices<IMqttMessageHandler>();//会根据类型自动注入所有实现了 IMqttMessageHandler 接口的实例

                    bool handled = false;
                    foreach (var handler in handlers)
                    {
                        if (handler.CanHandle(message.Topic))
                        {
                            // 正常处理完不打日志（多数 handler 的正常路径也是静默的），
                            // 想知道"这条消息落到哪个 handler"时把日志级别调到 Debug。
                            _logger.LogDebug(
                                "消息已交由 {Handler} 处理，Topic={Topic}",
                                handler.GetType().Name,
                                message.Topic);

                            await handler.HandleAsync(message);
                            handled = true;
                            break; // 一个消息仅由一个处理器处理，可根据需求调整
                        }
                    }

                    if (!handled)
                    {
                        // 逐条打 Warning 会让 device/{id}/event/OTACP、cmd/reply/setlog
                        // 这类无处理器的主题持续刷屏。改为按主题聚合、周期汇总，
                        // 既能看出是哪些主题没有处理器，又不会淹没其它日志。
                        _unhandledCounts.AddOrUpdate(
                            message.Topic,
                            _ => 1,
                            (_, count) => count + 1);

                        _logger.LogDebug("未找到能处理主题 {Topic} 的处理器", message.Topic);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "处理消息时发生异常，Topic: {Topic}", message.Topic);
                }
            }

            _logger.LogInformation("消息路由器停止");
        }

        /// <summary>
        /// 汇总未匹配主题。用整体替换的方式取快照，保证每条消息只被统计一次。
        /// </summary>
        private void ReportUnhandledTopics()
        {
            if (_unhandledCounts.IsEmpty)
                return;

            var snapshot = Interlocked.Exchange(
                ref _unhandledCounts,
                new ConcurrentDictionary<string, long>(StringComparer.Ordinal));

            if (snapshot.IsEmpty)
                return;

            var details = snapshot
                .OrderByDescending(item => item.Value)
                .Take(10)
                .Select(item => $"{item.Key}={item.Value}");

            _logger.LogWarning(
                "近 60 秒共有 {TopicCount} 类主题未找到处理器，合计 {Total} 条：{Details}",
                snapshot.Count,
                snapshot.Values.Sum(),
                string.Join(", ", details));
        }
    }
}
