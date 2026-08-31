# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 常用命令

```powershell
dotnet build DeviceService.slnx          # 构建（唯一的验证手段，要求 0 错误）
dotnet run --project DeviceService       # 运行服务（连接 appsettings.json 里的内网 MQTT/Redis）
```

无测试项目、无 CI、无 Docker、无 lint 配置。解决方案文件是新格式 `.slnx`，不是 `.sln`。

## 项目结构

.NET 10 双项目 Worker 服务（需 .NET 10 SDK），桥接 ESP32 老化设备（MQTT）与平台（Redis）：

- `DeviceService/`
  - `Mqtt/MqttConnection`：MQTTnet 客户端，订阅全部上行主题，断线指数退避重连并重订阅；收到的消息写入 Channel。
  - `Workers/MqttMessageWorker`：从 Channel 读消息，按 `CanHandle(topic)` 分发给第一个匹配的 Handler。注意命名空间是 `DeviceService.Works`（文件夹却是 `Workers/`，不一致是历史原因）。
  - `Handlers/`：各 `IMqttMessageHandler` 实现（上线/下线、event、log、aging 数据、cmd/reply）。通过 `AddMqttHandlers()` 程序集反射自动注册为 Scoped，新增 Handler 不需要改 Program.cs。
  - `Workers/RedisCmdWorker`：消费 Redis 命令队列、发布 MQTT、等待回复、XACK。
  - `Services/AgingCommandGate`：start_aging 状态门控；`Services/AgingRecordCallbackClient`：老化状态回调业务 API 的后台 FIFO 队列（HTTPS 重试不阻塞消息处理）。
- `Bluetti.Device.Redis/`：Redis 数据层类库。所有键定义与校验集中在 `RedisKeys.cs`；仓储为 `CommandRepository` / `DeviceRepository`；对外入口是 `CmdHelper.SendCmdAsync()`。

## 核心数据流

命令下发链路：

1. 平台写 Redis Stream `{Prefix}:cmd:queue`，字段名必须是 `data`；一条 Entry 的 `DeviceInfos` 可含多个 `{DeviceId, RecordId}`（批量）。
2. `RedisCmdWorker` 以消费者组 `device-service` 消费，展开为单设备 `CmdInfo`，按 DeviceId 投入各自的 Channel 串行队列（不同设备并行）。
3. 每条命令发布 MQTT `server/{deviceId}/cmd/{action}`，payload 固定 `{Seq, Data}`——无参数时 `"Data": null` 也必须存在。`RecordId` 不进 MQTT payload，只存 Redis。
4. 等四类回复主题 `device/{id}/cmd/reply/{point|state|pn|config}` 中匹配 `CmdSeq` 的回复落库，或超时标 `timed_out`，才处理同设备下一条；一条 Stream Entry 的所有设备子命令都结束后才 XACK。

回复等待机制：进程内 `TaskCompletionSource` 唤醒 + 超时边界复查 Redis Hash（`CommandRepository.WaitForCompletionAsync`）。命令最终状态以 Redis Hash `{Prefix}:device:{id}:cmd:{seq}` 为准，不要只依赖内存通知。

设备上行链路：MQTT → Channel → `MqttMessageWorker` → Handler 写 Redis（最新值 Hash + 历史 Stream）。`device/{id}/event/state` 中的老化状态（agingStart/agingIdle/agingStop/agingError）还会经 `CurrentAgingSeq → cmd:{seq} → RecordId` 触发业务 API 回调。

主题解析集中在 `Mqtt/MqttTopic.cs`，严格匹配最新协议（event 只认 state/fault/warn，log 只认 runtime/error/local），不兼容旧主题。

## 关键约定与陷阱

- **键前缀以代码为准**：实际前缀是 `RedisKeys.Prefix = "lgwtest2"`；根目录所有 `*.md` 文档示例写的 `lgw:` 已过时，不要照抄文档前缀。
- 根目录中文 `*.md` 是主要协议/变更文档（尤其 `REDIS_STRUCTURE.md`、`CHANGELOG_LATEST_PROTOCOL.md`）：改协议前先读，改完行为要同步更新对应文档。另有 `AGENTS.md` 与本文件内容相近，改动约定时两处都要更新。
- 同设备命令严格串行（收到回复或超时才发下一条）；`start_aging` 走独立队列 + `AgingCommandGate` 门控：只有设备上报 `agingIdle / agingStop / agingError` 才放行下一条，**`timed_out` 不释放门**（超时只说明没等到回复，不能证明 ESP32 没执行）。
- 全链路至少一次投递：重启恢复 Pending 时即使状态已是 published 也可能用原 Seq 重发 MQTT（防"假 published"），设备端按 `Seq/CmdSeq` 幂等去重是前提。
- 手工向 Stream 写命令必须用字段名 `data`（否则 `ReadAsync` 解析不出）；正常入口用 `CmdHelper.SendCmdAsync()`。
- `{Prefix}:devices:online` 必须是 List（元素为 `{"id":"...","type":"..."}` JSON），历史上曾被错误写成 Set；生产库禁止 `FLUSHDB`。
- `DeviceId`、PN 等键片段经 `RedisKeys.Normalize*` 校验：不能含冒号、斜杠、空白；ReplyType 只允许 point/state/pn/config。
- `appsettings.json` 全部指向局域网环境（MQTT broker、Redis、回调 API 均为内网 IP）；`AgingCallback:AllowInvalidServerCertificate=true` 是为自签名证书故意保留的，不要移除。

## 配置

`CommandProcessing.ReplyTimeoutSeconds`（默认 15）、`PublishRetryDelayMilliseconds`（默认 1000）、`RedisSettings:ConsumerName`（缺省取本机名）在 `appsettings.json` 中调整。
