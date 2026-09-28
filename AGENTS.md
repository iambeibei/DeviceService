# AGENTS.md

## 项目概览

.NET 10（需 .NET 10 SDK）双项目解决方案，解决方案文件是新格式 `DeviceService.slnx`：

- `DeviceService/`：Worker 后台服务，桥接 ESP32 老化设备（MQTT）与平台（Redis）。
- `Bluetti.Device.Redis/`：Redis 数据层类库，所有键定义集中在 `RedisKeys.cs`。

无测试项目、无 CI、无 Docker。**验证手段就是 `dotnet build DeviceService.slnx` 通过（0 错误）。**

## 核心数据流

平台命令：写入 Redis Stream `{Prefix}:cmd:queue`（字段名必须是 `data`）→ `Workers/RedisCmdWorker` 以消费者组 `device-service` 消费 → 按设备拆分为普通串行队列或 `start_aging` 专用队列 → 发布 MQTT `server/{deviceId}/cmd/{action}`，payload 只有 `{Seq, Data}`（无参数时 `"Data": null` 也必须存在）→ 等待四类回复主题之一中匹配 `CmdSeq` 的回复或超时 → 才 XACK。

- 普通命令：同设备串行，收到回复或超时即放行下一条。
- `start_aging`：受 `AgingCommandGate` 门控，只有设备上报 `agingIdle / agingStop / agingError` 才放行下一条；`timed_out` 不释放门。

设备上行：`MqttMessageWorker` 按 `CanHandle(topic)` 分发给 `Handlers/` 下各 `IMqttMessageHandler` 实现（`AddMqttHandlers()` 程序集反射自动注册，新增 Handler 无需改 Program.cs）。

群控模式按字符串类型的 `AgingNumber` 隔离：`device/public/deviceMode` 和 `device/public/agingStage` 的 Data 都必须携带 AgingNumber。Mode=2 时，批量 `start_aging` 的 Seq 作为内部 RunSeq、DeviceInfos 作为候选参与集合；只有设备真实上报 `agingStart` 才激活该批。type=1 点位通过 `device/{id}/event/TaskCP` 上报阶段完成，服务以当前批固定参与集合为屏障，全部完成后按架隔离发布 `server/rack/{AgingNumber}/CanNextstep` 放行（设备端订阅本架主题，不再使用公共主题）；实时在线列表只用于在线状态和诊断，不决定屏障成员。agingStage 对应的电源控制定向发布到该架唯一在线 type=2 总控的 `server/{controllerId}/cmd/CPower`，Standing/Discharge 关电，Recharge 开电。

Action 与回复主题映射：`get_state` → `state`，`set_pn` → `pn`，`get_config` → `config`，其它 → `point`（见 `CommandReplyTypeResolver`）。

## 关键约定与陷阱

- **Redis 键前缀以代码为准**：`RedisKeys.Prefix = "lgw"`。若看到历史/示例文档出现其它前缀（如 `lgw:` 或 `lgwtest2`）一律视为过时，不要照抄。
- **Redis 过期时间统一由 `RedisTtl.cs` 控制**：改 `DefaultDays` 即可调整，不要在各 Repository 里写私有 TTL 常量。当前**只有两类键设过期**：`lgw:device:{id}:cmd:{seq}`（命令 Hash）与 `lgw:device:{id}:aging:{pn}:history`（采样历史 Stream）。给新键加 TTL 前必须先看 `RedisTtl` 的注释清单：`lgw:cmd:seq`（Seq 回退）、`lgw:cmd:queue`（整键过期会连消费者组 `device-service` 一起消失，而消费组只在启动时创建一次 → 之后持续 NOGROUP 卡死，只能靠 MAXLEN 裁剪）、`runtime` 与 `event:state`（老化任务关联恢复源）、在线列表与设备目录、`group-control:racks` 与 `{n}:state` **一律不设过期**。另注意 TTL 是整键删除，不是按条目裁剪。
- 本项目当前无 `*.md` 协议/变更文档（历史引用的 `REDIS_STRUCTURE.md`、`CHANGELOG_LATEST_PROTOCOL.md` 已不存在），改动协议/行为时以 `RedisKeys.cs` 与 `AGENTS.md` 为准。
- 同设备命令严格串行（收到回复或超时才发下一条）；不同设备并行。`start_aging` 走独立队列 + `AgingCommandGate` 门控：只有设备上报 `agingIdle / agingStop / agingError` 才放行下一条，`timed_out` 不释放门。
- 全链路至少一次投递：重启恢复会用原 Seq 重发 MQTT，设备端按 `Seq/CmdSeq` 幂等去重是前提。不要把"published 状态"当作"一定已发送成功"。
- 手工向 Stream 写命令必须用字段名 `data`（否则 `ReadAsync` 解析不出）；正常入口用 `CmdHelper.SendCmdAsync()`。
- `AgingNumber` 必须是非负整数格式的字符串（如 `"0"`）；数字 JSON 值 `0` 属于无效协议数据。
- `{Prefix}:devices:online` 必须是全量在线 List（元素为 `{"id":"...","type":"...","AgingNumber":"0"|null}` JSON）；type=1 同时进入 `{Prefix}:devices:online:aging:{AgingNumber}`，type=2 同时进入 `{Prefix}:devices:online:control`。历史上曾被错误写成 Set；生产库禁止 `FLUSHDB`。
- `DeviceId`、PN 等键片段经 `RedisKeys.Normalize*` 校验：不能含冒号、斜杠、空白；ReplyType 只允许 point/state/pn/config。
- 命名空间陷阱：`namespace DeviceService.Works` 实际位于 `Workers/MqttMessageWorker.cs`（文件夹名与命名空间不一致）。
- `appsettings.json` 指向局域网环境（MQTT broker、Redis、回调 API 均为内网 IP）；`AgingCallback:AllowInvalidServerCertificate=true` 是为自签名证书故意保留的。
- 回复等待基于进程内 `TaskCompletionSource` + Redis Hash 双重确认（见 `CommandRepository.WaitForCompletionAsync`），命令最终状态以 Redis Hash 为准，不要只依赖内存通知。
- **群控阶段超时与半数放行**（`GroupControlService.EvaluateNextStepUnsafeAsync`，顺序固定为"先超时剔除 → 再判放行 → 最后按需起表"）：本阶段 `completed` 数达到 `Ceil(participants.Count / 2)` 起 10 分钟倒计时（deadline 存 `state.StageDeadlineUtc`，复用现有 1s 轮询，**不引入 Timer**）；到期把 `participants \ completed` 从 participants 剔除、记入 `...run:{runSeq}:excluded` 并写 `event:fault`，随后放行剩余设备。被剔除设备**本批次不再回归**（屏障以 participants 为准），只能等下一个 RunSeq。全员完成会清掉倒计时直接放行。详见 `docs/群控阶段超时与TTL改造方案.md`。
- **放行回复校验**：`CanNextstep` 发布前先为每台 participant `EnsureStoredAsync` 一条 `Action="can_next"` 的命令记录（群控指令不进 Stream），设备按现有协议回 `device/{id}/cmd/reply/point` + `Data.CmdSeq`，`Code==1` 才算确认。后台并发等待 → 失败则 `MarkTimedOut` 后**用原 Seq 整架重发一次**，重发前必须 `CommandRepository.ResetForRetryAsync`（否则 `WaitForCompletionAsync` 遇终态立即返回、重试形同虚设）→ 仍失败写 `event:fault` 并 publish `event:fault`。校验只留痕，**不回滚放行、不联动剔除 participants**。
- **终态 / 掉线设备必须立即移出 participants**：放行判定只看 `completed`，设备一旦上报 `agingStop/agingError/agingIdle` 或掉线（`device/public/down_line`），就不会再上报 TaskCP，若留在 participants 会永久阻塞整架放行。由 `GroupControlService.HandleAgingStateAsync`（终态分支）与 `HandleDeviceOfflineAsync`（掉线，`DeviceOfflineHandler` 调用）统一走 `ExcludeParticipantUnsafeAsync`：移出 participants → 记 `excluded` → 按 `RaiseFaultOnExclusion` 写 fault。被移出设备**本批次不回归**，只能等下一个 RunSeq。参与集合被清空时走 `FinishRunUnsafeAsync` 直接结束批次，否则既不放行也不结束会挂死。
- **阶段倒计时起表条件默认不要求过半数**（`StageTimeoutRequireHalf=false`，只要本阶段有任意设备完成就起表）：若要求过半数，当多数设备掉线/终态时会永远起不了表，批次无兜底挂起。
- **放行回复校验会跳过离线设备**（`FilterOnlineParticipantsAsync`）：架广播送不到离线设备，等待它们只会产生"未收到确认"的假故障。
- **`AgingCommandGate` 有离线兜底释放**：设备掉线后不会再上报终态，发送门等待超过 `AgingCommandGate:OfflineReleaseTimeoutMinutes`（默认 30）且设备已离线时强制释放门。注意只在**设备已离线**时释放——设备在线时老化可能仍在进行（可能持续数小时），不能按时间强行插队。
- **服务端自产故障必须走 `DeviceRepository.SaveGroupControlFaultAsync`**，不要用 `SaveEventAsync`：后者内部会 `MarkSeenAsync()` 把 `runtime.Online` 置 true，会把离线设备"复活"成在线，进而污染总控唯一性判断。
- 群控状态保存在 `lgw:group-control:{AgingNumber}:*`；候选批次不会替换当前批次，首次真实 `agingStart` 才激活。当前批结束前若另一 RunSeq 上报 `agingStart`，拒绝切换并告警。TaskCP 仅接受当前批参与设备，并按 AgingNumber + DeviceId + 单调递增 Seq 去重。`CanNextstep` 发布到 `server/rack/{AgingNumber}/CanNextstep`（架号经 `RedisKeys.NormalizeAgingNumber` 规范化后拼接，架号非法时保留 pending 重试，不退化成公共广播），不依赖总控在线状态；只有 `CPower` 要求该架恰好存在一个在线总控，无总控或多个总控时保留电源 pending。群控直接指令不进入命令 Stream、不等待命令回复，发布失败或重启时使用原 Seq 重试。

## 配置

`CommandProcessing.ReplyTimeoutSeconds`（默认 15s）、`PublishRetryDelayMilliseconds`（默认 1000ms，同时是群控后台轮询间隔）、`GroupControlReleaseReplyTimeoutSeconds`（默认 15s，单台放行回复等待）在 `appsettings.json` 中调整；消费组内 Consumer 名取 `RedisSettings:ConsumerName`，缺省为本机名。

`GroupControl` 段（群控阶段超时与放行校验）：`StageTimeoutEnabled`、`StageTimeoutMinutes`（默认 10）、`StageTimeoutRequireHalf`（默认 false = 有任意设备完成即起表）、`HalfReleaseThresholdMode`（`Ceil`/`Floor`，默认 Ceil）、`ExcludedRejoinMode`（当前仅 `None`）、`RaiseFaultOnStageTimeout`、`ExcludeOnAgingTerminal`（默认 true）、`ExcludeOnDeviceOffline`（默认 true）、`RaiseFaultOnExclusion`（默认 true）、`ReleaseReplyVerificationEnabled`、`ReleaseReplyRetryCount`（默认 1）、`RaiseFaultOnReleaseReplyFailure`。

`AgingCommandGate` 段：`OfflineReleaseTimeoutMinutes`（默认 30，≤0 表示禁用离线兜底释放）。
