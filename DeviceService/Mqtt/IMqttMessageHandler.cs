using MQTTnet;
using System;
using System.Collections.Generic;
using System.Text;

namespace DeviceService.Mqtt
{
    public interface IMqttMessageHandler
    {
        /// <summary>
        /// 判断是否能处理该主题
        /// </summary>
        bool CanHandle(string topic);

        /// <summary>
        /// 处理消息
        /// </summary>
        Task HandleAsync(MqttApplicationMessage message);
    }



    public static class MqttHandlerExtensions
    {
        public static IServiceCollection AddMqttHandlers(this IServiceCollection services)
        {
            // 从当前程序集扫描所有实现了 IMqttMessageHandler 的非抽象类
            var handlerTypes = typeof(IMqttMessageHandler).Assembly
                .GetTypes()
                .Where(t => t.IsClass && !t.IsAbstract && typeof(IMqttMessageHandler).IsAssignableFrom(t));

            foreach (var type in handlerTypes)
            {
                // 注册为 Scoped 生命周期（每次消息处理创建新的作用域）
                services.AddScoped(typeof(IMqttMessageHandler), type);
            }

            return services;
        }
    }

}
