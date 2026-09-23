using Bluetti.Device.Redis;
using DeviceService.Mqtt;
using DeviceService.Services;
using DeviceService.Workers;
using DeviceService.Works;
using StackExchange.Redis;

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.AddDebug();

var redisConn = builder.Configuration.GetValue<string>("RedisSettings:ConnectionString")
    ?? "localhost:6379,abortConnect=false,defaultDatabase=0";

//添加 Redis 连接
builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
{
    var options = ConfigurationOptions.Parse(redisConn);
    options.AbortOnConnectFail = false;
    return ConnectionMultiplexer.Connect(options);
});

builder.Services.AddSingleton<RedisHelper>();
builder.Services.AddSingleton<CommandRepository>();
builder.Services.AddSingleton<DeviceRepository>();
builder.Services.AddSingleton<GroupControlRepository>();
builder.Services.AddSingleton<CmdHelper>();
builder.Services.AddSingleton<DeviceHelper>();
builder.Services.AddSingleton<AgingCommandGate>();
builder.Services.AddSingleton<AgingRecordCallbackClient>();
builder.Services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<AgingRecordCallbackClient>());

builder.Services.AddSingleton<MqttConnection>();
builder.Services.AddSingleton<GroupControlService>();
builder.Services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<GroupControlService>());
builder.Services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<MqttConnection>());

//添加托管服务
builder.Services.AddHostedService<MqttMessageWorker>();
builder.Services.AddHostedService<RedisCmdWorker>();

builder.Services.AddMqttHandlers();

await builder.Build().RunAsync();
