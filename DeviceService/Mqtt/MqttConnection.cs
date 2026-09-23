using DeviceService.Handlers;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Protocol;
using System;
using System.Collections.Generic;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Threading.Channels;

namespace DeviceService.Mqtt
{
    public class MqttConnection : IHostedService
    {

        private readonly IMqttClient _client;
        private readonly MqttClientOptions _options;
        private readonly Channel<MqttApplicationMessage> _messageChannel;
        private readonly ILogger<MqttConnection> _logger;

        private readonly MqttSettings _mqttSettings;




        private readonly object _reconnectLock = new();
        private bool _isReconnecting;
        private CancellationTokenSource _reconnectCts;



        public MqttConnection(IConfiguration config, ILogger<MqttConnection> logger)
        {
            _logger = logger;

            //创建一个 MQTTnet 客户端对象
            var factory = new MqttClientFactory();
            _client = factory.CreateMqttClient();

            // 注册断线回调（关键：掉线自动触发）
            _client.DisconnectedAsync += ClientOnDisconnectedAsync;

            _mqttSettings = config.GetSection("MqttSettings").Get<MqttSettings>()
               ?? throw new InvalidOperationException("MqttSettings not configured");

            var optionBuilder = new MqttClientOptionsBuilder()
                  .WithTcpServer(_mqttSettings.Broker, _mqttSettings.Port)
                  .WithClientId(_mqttSettings.ClientId)
                  .WithCleanSession(false)
                  .WithKeepAlivePeriod(TimeSpan.FromSeconds(60))
                  .WithWillTopic("device/status")
                  .WithWillPayload("{\"status\":\"offline\"}")
                  .WithWillQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                  .WithWillRetain(true);

            // 补充账号密码配置
            if (!string.IsNullOrEmpty(_mqttSettings.Username))
            {
                optionBuilder.WithCredentials(_mqttSettings.Username, _mqttSettings.Password);
            }

            _options = optionBuilder.Build();

            // 容量 10000 的消息通道，防止突发流量压垮服务
            _messageChannel = Channel.CreateBounded<MqttApplicationMessage>(
                new BoundedChannelOptions(10000)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = false,
                    SingleWriter = true
                });

            _reconnectCts = new CancellationTokenSource();
        }

        // 启动时连接并订阅主题
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            await ConnectWithRetryAsync(cancellationToken);

            //接收回调
            _client.ApplicationMessageReceivedAsync += async e =>
            {
                // 异步写入通道，不阻塞 MQTT 事件循环
                await _messageChannel.Writer.WriteAsync(new MqttApplicationMessage
                {
                    Topic = e.ApplicationMessage.Topic,
                    Payload = e.ApplicationMessage.Payload,
                    QualityOfServiceLevel = e.ApplicationMessage.QualityOfServiceLevel
                });
            };

            // 订阅通配符主题（根据业务调整）
            await SubscribeAllTopicsAsync();

            _logger.LogInformation("MQTT 连接成功，并完成主题订阅");
        }

        private async Task SubscribeAllTopicsAsync()
        {
            await _client.SubscribeAsync("device/public/up_line");
            await _client.SubscribeAsync("device/public/down_line");
            await _client.SubscribeAsync("device/public/deviceMode");
            await _client.SubscribeAsync("device/public/agingStage");
            await _client.SubscribeAsync("device/+/event/+");
            await _client.SubscribeAsync("device/+/data/aging");
            await _client.SubscribeAsync("device/+/log/+");
            await _client.SubscribeAsync("device/+/cmd/reply/+");
        }

        private async Task ConnectWithRetryAsync(CancellationToken ct)
        {
            var retryCount = 0;
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var result = await _client.ConnectAsync(_options, ct);
                    if (result.ResultCode == MqttClientConnectResultCode.Success)
                        return;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "MQTT 连接失败，{Retry}秒后重试", Math.Pow(2, retryCount));
                }
                await Task.Delay(TimeSpan.FromSeconds(Math.Min(Math.Pow(2, retryCount++), 60)), ct);
            }
        }

        public ChannelReader<MqttApplicationMessage> GetMessageReader() => _messageChannel.Reader;

        public bool IsConnected => _client.IsConnected;

        public async Task PublishAsync(string topic, string payload, CancellationToken ct = default)
        {
            if (!_client.IsConnected)
            {
                _logger.LogWarning(
                    "MQTT 发布被拒绝：客户端当前未连接，Topic={Topic}",
                    topic);
                throw new InvalidOperationException("MQTT 客户端当前未连接 Broker");
            }

            var msg = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payload)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();

            _logger.LogInformation(
                "准备发布 MQTT，Topic={Topic}, Payload={Payload}",
                topic,
                payload);

            await _client.PublishAsync(msg, ct);

            _logger.LogInformation(
                "MQTT PublishAsync 已完成，Topic={Topic}",
                topic);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            _reconnectCts.Cancel();
            if (_client.IsConnected)
                await _client.DisconnectAsync();
            _messageChannel.Writer.TryComplete();
        }




        /// <summary>
        /// 断线回调入口
        /// </summary>
        private async Task ClientOnDisconnectedAsync(MqttClientDisconnectedEventArgs args)
        {
            // 服务正常关闭不重连
            if (args.ClientWasConnected == false) return;
            if (_reconnectCts.Token.IsCancellationRequested) return;

            _logger.LogWarning(args.Exception, "MQTT客户端已断开连接，准备自动重连");

            // 加锁防止多个断线事件同时触发重连
            lock (_reconnectLock)
            {
                if (_isReconnecting) return;
                _isReconnecting = true;
            }

            try
            {
                await ReconnectLoopAsync(_reconnectCts.Token);
            }
            finally
            {
                lock (_reconnectLock)
                {
                    _isReconnecting = false;
                }
            }
        }

        /// <summary>
        /// 断线后重连循环（指数退避，最大60秒间隔）
        /// </summary>
        private async Task ReconnectLoopAsync(CancellationToken ct)
        {
            var retryCount = 0;
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var connectResult = await _client.ConnectAsync(_options, ct);
                    if (connectResult.ResultCode == MqttClientConnectResultCode.Success)
                    {
                        _logger.LogInformation("MQTT重连成功，重新订阅所有主题");
                        await SubscribeAllTopicsAsync();
                        return;
                    }

                    _logger.LogWarning("MQTT重连失败，返回码：{Code}", connectResult.ResultCode);
                }
                catch (Exception ex)
                {
                    var delaySec = Math.Min(Math.Pow(2, retryCount), 60);
                    _logger.LogWarning(ex, "MQTT重连异常，等待{Delay}秒后重试", delaySec);
                }

                var waitTime = TimeSpan.FromSeconds(Math.Min(Math.Pow(2, retryCount++), 60));
                await Task.Delay(waitTime, ct);
            }
        }


        private class MqttSettings
        {
            public string Broker { get; set; } = "localhost";
            public int Port { get; set; } = 1883;
            public string ClientId { get; set; } = "worker";
            public string Topic { get; set; } = "#";
            public string Username { get; set; } = "";
            public string Password { get; set; } = "";
        }
    }
}
