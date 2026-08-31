# AGENTS.md

## 项目概览

.NET 10（需 .NET 10 SDK）双项目解决方案，解决方案文件是新格式 `DeviceService.slnx`：

- `DeviceService/`：Worker 后台服务，桥接 ESP32 老化设备（MQTT）与平台（Redis）。
- `Bluetti.Device.Redis/`：Redis 数据层类库，所有键定义集中在 `RedisKeys.cs`。

无测试项目、无 CI、无 Docker。**验证手段就是 `dotnet build DeviceService.slnx` 通过（0 错误）。**

## 核心数据流

平台命令：写入 Redis Stream `{Prefix}:cmd:queue`（字段名必须是 `data`）→ `Workers/RedisCmdWorker` 以消费者组 `device-service` 消费 → 按设备串行队列发布 MQTT `server/{deviceId}/cmd/{action}`，payload 只有 `{Seq, Data}`（无参数时 `"Data": null` 也必须存在）→ 等待四类回复主题之一中匹配 `CmdSeq` 的回复或超时 → 才 XACK。

设备上行：`MqttMessageWorker` 按 `CanHandle(topic)` 分发给 `Handlers/` 下各 `IMqttMessageHandler` 实现（`AddMqttHandlers()` 程序集反射自动注册，新增 Handler 无需改 Program.cs）。

## 关键约定与陷阱

- **Redis 键前缀以代码为准**：实际前缀是 `RedisKeys.Prefix = "lgwtest2"`；根目录所有 `*.md` 文档示例写的是 `lgw:`，已过时，不要照抄文档前缀。
- 根目录的中文 `*.md` 是主要协议/变更文档（尤其 `REDIS_STRUCTURE.md`、`CHANGELOG_LATEST_PROTOCOL.md`），改动协议前先读；修改行为后应同步更新对应变更文档。
- 同设备命令严格串行（收到回复或超时才发下一条）；不同设备并行。`start_aging` 走独立队列 + `AgingCommandGate` 门控：只有设备上报 `agingIdle / agingStop / agingError` 才放行下一条，`timed_out` 不释放门。
- 全链路至少一次投递：重启恢复会用原 Seq 重发 MQTT，设备端按 `Seq/CmdSeq` 幂等去重是前提。不要把"published 状态"当作"一定已发送成功"。
- 手工向 Stream 写命令必须用字段名 `data`（否则 `ReadAsync` 解析不出）；正常入口用 `CmdHelper.SendCmdAsync()`。
- `{Prefix}:devices:online` 必须是 List（元素为 `{"id":"...","type":"..."}` JSON），历史上曾被错误写成 Set；生产库禁止 `FLUSHDB`。
- `DeviceId`、PN 等键片段经 `RedisKeys.Normalize*` 校验：不能含冒号、斜杠、空白；ReplyType 只允许 point/state/pn/config。
- 命名空间陷阱：`namespace DeviceService.Works` 实际位于 `Workers/MqttMessageWorker.cs`（文件夹名与命名空间不一致）。
- `appsettings.json` 指向局域网环境（MQTT broker、Redis、回调 API 均为内网 IP）；`AgingCallback:AllowInvalidServerCertificate=true` 是为自签名证书故意保留的。
- 回复等待基于进程内 `TaskCompletionSource` + Redis Hash 双重确认（见 `CommandRepository.WaitForCompletionAsync`），命令最终状态以 Redis Hash 为准，不要只依赖内存通知。

## 配置

`CommandProcessing.ReplyTimeoutSeconds`（默认 15s）与 `PublishRetryDelayMilliseconds`（默认 1000ms）在 `appsettings.json` 中调整；消费组内 Consumer 名取 `RedisSettings:ConsumerName`，缺省为本机名。
