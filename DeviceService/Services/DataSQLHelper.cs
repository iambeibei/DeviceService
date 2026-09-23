using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;

namespace DeviceService.Services
{
    public static class DataSQLHelper
    {
        // 将 HttpClient 作为静态字段复用，避免频繁创建导致端口耗尽（Socket Exhaustion）
        private static readonly HttpClient _httpClient;
        static DataSQLHelper()
        {
            // 忽略 SSL 证书验证（解决自签名证书或证书过期导致的连接失败）
            var handler = new HttpClientHandler();
            handler.ServerCertificateCustomValidationCallback = (message, cert, chain, errors) => true;
            _httpClient = new HttpClient(handler);
            _httpClient.Timeout = TimeSpan.FromSeconds(30);
            _httpClient.DefaultRequestHeaders.UserAgent.ParseAdd("CSharp-HttpClient/1.0");
        }

        //老化数据API
        public static async Task<string> JsonHttpPostAsync(string Url, object ParamsObj)
        {
            try
            {
                // 序列化参数为 JSON 字符串
                string jsonString = ParamsObj != null ? JsonConvert.SerializeObject(ParamsObj) : "{}";

                // 使用 StringContent 封装请求体，替代手动获取 RequestStream
                var content = new StringContent(jsonString, Encoding.UTF8, "application/json");

                // 直接通过 HttpClient 发送 POST 请求，异步读取响应
                var response = await _httpClient.PostAsync(Url, content);

                string retString = await response.Content.ReadAsStringAsync();
                return retString;
            }
            catch (Exception ex)
            {
                return "false: " + ex.Message;
            }
        }
    }
}
