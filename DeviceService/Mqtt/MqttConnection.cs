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

        /// <summary>
        /// 是否打印每一条收到的上行消息。默认关闭：老化采样数据频率很高，
        /// 全量打印会淹没其它日志。排查"消息到底有没有进来"时打开。
        /// </summary>
        private readonly bool _logIncoming;
        private const int MaxLoggedPayloadLength = 500;




        private readonly object _reconnectLock = new();
        private bool _isReconnecting;
        private CancellationTokenSource _reconnectCts;

        // 收发观测：接收侧原先完全无计数，无法回答"到底丢了多少"。
        // 用 long + Interlocked：MQTT 回调与发布在不同线程上并发。
        private long _receivedCount;
        private long _enqueuedCount;
        private long _channelNearFullCount;
        private long _publishFailCount;
        private long _publishNotConnectedCount;
        private Timer? _metricsTimer;

        /// <summary>接近该积压量即开始计数，用于区分"MQTT 丢"和"消费端处理不过来"。</summary>
        private const int ChannelNearFullThreshold = 9000;



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

            _logIncoming = config.GetValue<bool?>("MqttSettings:LogIncomingMessages") ?? false;

            var optionBuilder = new MqttClientOptionsBuilder()
                  .WithTcpServer(_mqttSettings.Broker, _mqttSettings.Port)
                  .WithClientId(_mqttSettings.ClientId)
                  .WithCleanSession(false)
                  // MQTT 5 下 CleanSession=false 并不产生持久会话，SessionExpiryInterval
                  // 默认为 0（连接断开即丢弃）。单位是**秒(uint)**，不是 TimeSpan。
                  // 这只能覆盖秒级~分钟级抖动，不要指望它补数小时停机（Broker 队列容量不足）。
                  .WithSessionExpiryInterval(3600u)
                  .WithKeepAlivePeriod(TimeSpan.FromSeconds(60))
                  // 原遗嘱挂在 device/ 下且 retain=true：既没有任何订阅方，
                  // 又会在崩溃后永久留下一条脏 retained 消息。改为服务端自身的命名空间且不保留。
                  .WithWillTopic($"server/{_mqttSettings.ClientId}/status")
                  .WithWillPayload(
                      "{\"status\":\"offline\",\"clientId\":\"" +
                      _mqttSettings.ClientId +
                      "\",\"ts\":" + DateTimeOffset.UtcNow.ToUnixTimeSeconds() + "}")
                  .WithWillQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                  .WithWillRetain(false);

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

            // 接收回调必须在 ConnectAsync **之前**注册。
            // 原先注册放在 StartAsync 的连接之后，连接成功到注册之间存在窗口，
            // 期间到达的消息没有任何订阅者，会被静默丢弃。
            _client.ApplicationMessageReceivedAsync += OnApplicationMessageReceivedAsync;
        }

        private async Task OnApplicationMessageReceivedAsync(
            MqttApplicationMessageReceivedEventArgs e)
        {
            Interlocked.Increment(ref _receivedCount);

            if (_messageChannel.Reader.Count >= ChannelNearFullThreshold)
                Interlocked.Increment(ref _channelNearFullCount);

            // 接收侧本来是完全静默的：消息写进通道后就没有任何痕迹，
            // 控制台上看不出"到底有没有收到"。排查时打开这个开关即可。
            if (_logIncoming)
            {
                var payload = e.ApplicationMessage.ConvertPayloadToString();
                if (payload.Length > MaxLoggedPayloadLength)
                    payload = payload[..MaxLoggedPayloadLength] + "...";

                _logger.LogInformation(
                    "收到 MQTT 消息，Topic={Topic}, Payload={Payload}",
                    e.ApplicationMessage.Topic,
                    payload);
            }

            // 异步写入通道，不阻塞 MQTT 事件循环
            await _messageChannel.Writer.WriteAsync(new MqttApplicationMessage
            {
                Topic = e.ApplicationMessage.Topic,
                Payload = e.ApplicationMessage.Payload,
                QualityOfServiceLevel = e.ApplicationMessage.QualityOfServiceLevel
            });

            Interlocked.Increment(ref _enqueuedCount);
        }

        // 启动时连接并订阅主题
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            // 接收回调已在构造函数中注册（早于连接），这里只负责连接与订阅。
            await ConnectWithRetryAsync(cancellationToken);

            // 订阅通配符主题（根据业务调整）
            await SubscribeAllTopicsAsync();

            _metricsTimer ??= new Timer(_ =>
            {
                _logger.LogInformation(
                    "MQTT 指标：收到={Received} 入队={Enqueued} 通道接近满={NearFull} 发布失败={PubFail} 未连接拒绝={PubNoConn} 通道积压={Backlog}",
                    Interlocked.Read(ref _receivedCount),
                    Interlocked.Read(ref _enqueuedCount),
                    Interlocked.Read(ref _channelNearFullCount),
                    Interlocked.Read(ref _publishFailCount),
                    Interlocked.Read(ref _publishNotConnectedCount),
                    _messageChannel.Reader.Count);
            }, null, TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(60));

            _logger.LogInformation("MQTT 连接成功，并完成主题订阅");
        }

        /// <summary>
        /// 订阅的上行主题与各自请求的 QoS。**分层定级，不是一刀切**。
        ///
        /// QoS 1（丢一条会直接导致业务状态错误）：
        /// - 上下线：丢了会让在线名单永久失真（现在靠 OnlineStateRecoveryWorker 兜底）
        /// - 群控模式 / 阶段：丢了会导致供电控制与阶段推进错误
        /// - event（state/fault/warn/stepid）：丢了会丢老化状态与告警
        /// - cmd/reply：丢了会让命令被误判成 timed_out
        ///
        /// 保持 QoS 0（高频、可容忍个别丢失、且"重复"比"丢失"更难处理）：
        /// - data/aging 老化采样：**Seq 是发送时刻**，重投时 Seq 会变，
        ///   无法用它去重；改 QoS 1 会产生拦不住的重复数据并重复 HTTP 上报。
        ///   丢一条采样的影响远小于重复一条。
        /// - log 日志：量大、可容忍丢失，同样不适合引入重投。
        ///
        /// 注意端到端有效 QoS = min(设备发布 QoS, 本端订阅 QoS)，
        /// 因此最终仍以 Broker SUBACK 的授予值为准（下面会校验降级并报错）。
        /// </summary>
        private static readonly (string Topic, MqttQualityOfServiceLevel QoS)[] SubscribedTopics =
        {
            ("device/public/up_line", MqttQualityOfServiceLevel.AtLeastOnce),
            ("device/public/down_line", MqttQualityOfServiceLevel.AtLeastOnce),
            ("device/public/deviceMode", MqttQualityOfServiceLevel.AtLeastOnce),
            ("device/public/agingStage", MqttQualityOfServiceLevel.AtLeastOnce),
            ("device/+/event/+", MqttQualityOfServiceLevel.AtLeastOnce),
            ("device/+/cmd/reply/+", MqttQualityOfServiceLevel.AtLeastOnce),
            ("device/+/data/aging", MqttQualityOfServiceLevel.AtMostOnce),
            ("device/+/log/+", MqttQualityOfServiceLevel.AtMostOnce)
        };

        private async Task SubscribeAllTopicsAsync()
        {
            var builder = new MqttClientSubscribeOptionsBuilder();
            foreach (var (topic, qos) in SubscribedTopics)
            {
                builder.WithTopicFilter(
                    filter => filter.WithTopic(topic).WithQualityOfServiceLevel(qos));
            }

            var result = await _client.SubscribeAsync(builder.Build());

            foreach (var item in result.Items)
            {
                var requested = item.TopicFilter.QualityOfServiceLevel;

                // 授予等级编码在返回码里（GrantedQoS0/1/2），没有独立的字段。
                var granted = item.ResultCode switch
                {
                    MqttClientSubscribeResultCode.GrantedQoS0 => MqttQualityOfServiceLevel.AtMostOnce,
                    MqttClientSubscribeResultCode.GrantedQoS1 => MqttQualityOfServiceLevel.AtLeastOnce,
                    MqttClientSubscribeResultCode.GrantedQoS2 => MqttQualityOfServiceLevel.ExactlyOnce,
                    _ => (MqttQualityOfServiceLevel?)null
                };

                if (granted is null)
                {
                    // 订阅被拒绝必须报错，否则"完成主题订阅"会伪装成成功。
                    _logger.LogError(
                        "订阅被拒绝：{Filter} 返回码={Code}",
                        item.TopicFilter.Topic,
                        item.ResultCode);
                    continue;
                }

                if (granted.Value < requested)
                {
                    // 请求 1 却被授予 0 = 端到端仍是 QoS 0，必须显式暴露，不能静默接受。
                    _logger.LogError(
                        "订阅 QoS 被降级：{Filter} 请求={Requested} 授予={Granted}",
                        item.TopicFilter.Topic,
                        requested,
                        granted.Value);
                    continue;
                }

                _logger.LogInformation(
                    "订阅完成：{Filter} 请求QoS={Requested} 授予QoS={Granted}",
                    item.TopicFilter.Topic,
                    requested,
                    granted.Value);
            }
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
                Interlocked.Increment(ref _publishNotConnectedCount);

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

            // 必须检查发布结果：MQTTnet 对 PUBACK 的原因码是"返回结果而不抛异常"，
            // 未授权、配额拒绝等情况会被当成发布成功，群控还会照常清掉 pending。
            var result = await _client.PublishAsync(msg, ct);

            if (!result.IsSuccess)
            {
                Interlocked.Increment(ref _publishFailCount);

                _logger.LogError(
                    "MQTT 发布未被 Broker 接受，Topic={Topic}, ReasonCode={ReasonCode}",
                    topic,
                    result.ReasonCode);

                throw new InvalidOperationException(
                    $"MQTT 发布未被 Broker 接受，Topic={topic}, ReasonCode={result.ReasonCode}");
            }

            _logger.LogInformation(
                "MQTT PublishAsync 已完成，Topic={Topic}",
                topic);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            _metricsTimer?.Dispose();
            _metricsTimer = null;

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
