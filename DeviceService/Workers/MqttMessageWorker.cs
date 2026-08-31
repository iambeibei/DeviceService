using DeviceService.Handlers;
using DeviceService.Mqtt;
using System;
using System.Collections.Generic;
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

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var reader = _connection.GetMessageReader();
            _logger.LogInformation("消息路由器已启动，开始监听通道...");

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
                            await handler.HandleAsync(message);
                            handled = true;
                            break; // 一个消息仅由一个处理器处理，可根据需求调整
                        }
                    }

                    if (!handled)
                    {
                        _logger.LogWarning("未找到能处理主题 {Topic} 的处理器", message.Topic);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "处理消息时发生异常，Topic: {Topic}", message.Topic);
                }
            }

            _logger.LogInformation("消息路由器停止");
        }
    }
}
