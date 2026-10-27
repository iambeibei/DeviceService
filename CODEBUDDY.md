# CODEBUDDY.md This file provides guidance to CodeBuddy when working with code in this repository.

## 常用命令

```powershell
dotnet build DeviceService.slnx           # 构建（唯一的验证手段，要求 0 错误）
dotnet run --project DeviceService        # 本地运行（连接 appsettings.json 中的内网 MQTT/Redis）
```

无测试项目、无 CI、无 lint 配置、无 Docker。解决方案文件是新格式 `.slnx`（不是 `.sln`）。`appsettings.Development.json` 仅覆盖日志级别与命令处理超时/重试间隔。跑单条命令冒烟：手工向 Stream 写命令必须用字段名 `data`（否则 `ReadAsync` 解析不出），正常入口用 `CmdHelper.SendCmdAsync()`。

## 项目概览

.NET 10（需 .NET 10 SDK）双项目 Worker 后台服务，桥接 ESP32 老化设备（MQTT）与平台（Redis）：

- `DeviceService/`：MQTTnet 客户端 + 两个 Channel 消费 Worker + 群控/OTA 协调服务。
- `Bluetti.Device.Redis/`：Redis 数据层类库。所有键定义与片段校验集中在 `RedisKeys.cs`，过期时间统一在 `RedisTtl.cs`；仓储为 `CommandRepository` / `DeviceRepository` / `GroupControlRepository` / `OtaUpgradeRepository`，对外写命令入口是 `CmdHelper.SendCmdAsync()`。

### 进程内组件（`Program.cs` 注册）

- `Mqtt/MqttConnection`：MQTTnet 客户端，订阅全部上行主题，断线指数退避重连并重订阅；收到的消息写入有界 Channel。
- `Workers/MqttMessageWorker`：从 Channel 读消息，按 `CanHandle(topic)` 分发给第一个匹配的 Handler。注意其命名空间是 `DeviceService.Works`（文件夹却是 `Workers/`，不一致是历史原因）。
- `Handlers/`：7 个 `IMqttMessageHandler` 实现（上/下线、event、log、老化数据、cmd/reply、群控 `GroupControlHandlers.cs`），经 `AddMqttHandlers()` 程序集反射注册为 Scoped，新增 Handler 不需要改 Program.cs。
- `Workers/RedisCmdWorker`：消费 Redis 命令 Stream、发布 MQTT、等待回复、XACK。为每个 DeviceId 维护一条普通命令串行队列，并为 `start_aging` 单独维护一条专用队列。
- `Services/AgingCommandGate`：`start_aging` 状态门控。
- `Services/AgingRecordCallbackClient`：老化状态回调业务 API 的后台 FIFO 队列（HTTPS 重试不阻塞消息处理）。
- `Services/GroupControlService`：按 AgingNumber 隔离多个老化架的模式、阶段屏障和总控指令，状态及 pending 保存在 Redis。
- `Services/OtaUpgradeService`：批量 OTA 的 Mesh 分层升级调度，状态保存在 Redis。
- `Mqtt/MqttTopic.cs`：主题解析集中处，严格匹配最新协议（event 只认 `state/fault/warn`，log 只认 `runtime/error/local`，cmd/reply 只认 `point/state/pn/config`），不兼容旧主题。

## 核心数据流

### 命令下发（平台 → 设备）

1. 平台写 Redis Stream `{Prefix}:cmd:queue`，字段名必须是 `data`；一条 Entry 的 `DeviceInfos` 可含多个 `{DeviceId, RecordId}`（批量）。
2. `RedisCmdWorker` 以消费者组 `device-service` 消费，展开为单设备命令，按 DeviceId 投入各自的 Channel 串行队列（不同设备并行）：
   - 普通命令：收到回复或超时后立即处理下一条。
   - `start_aging`：受 `AgingCommandGate` 门控，只有设备上报 `agingIdle / agingStop / agingError` 才放行下一条。**每次下发等待 `StartAgingReplyTimeoutSeconds`（默认 20s），超时未收到 `cmd/reply` 就用新的下发 Seq 重发，最多 `StartAgingMaxAttempts`（默认 5）次**；耗尽仍无任何回复则标 `failed`、释放发送门、写 `event:fault` 并剔除群控参与集合（判定节点异常）。**例外**：若设备此时已上报 `agingStart`（说明命令其实生效、只是回复丢了），则不判异常，命令只标 `timed_out` 且发送门继续保持。
3. 每条命令发布 MQTT `server/{deviceId}/cmd/{action}`，payload 固定 `{Seq, Data}`——无参数时 `"Data": null` 也必须存在。`RecordId` 不进 MQTT payload，只存 Redis。
4. 等四类回复主题 `device/{id}/cmd/reply/{point|state|pn|config}` 中匹配 `CmdSeq` 的回复落库，或超时标 `timed_out`，才处理同设备下一条；一条 Stream Entry 的所有设备子命令都结束后才 XACK。`Code == 1` 才算成功（含固件"指令重复，老化已启动"这类幂等回复），其它 `Code` 一律 `failed`；`Msg` 不参与判定，只落库留痕。

回复等待机制：进程内 `TaskCompletionSource` 唤醒 + 超时边界复查 Redis Hash（`CommandRepository.WaitForCompletionAsync`）。命令最终状态以 Redis Hash `{Prefix}:device:{id}:cmd:{seq}` 为准，不要只依赖内存通知。

**`start_aging` 的两种 Seq 不能混用**：逻辑 Seq（Stream Entry 里那条）绑定 `CurrentAgingSeq`、群控 `RunSeq`、批次 ACK 与平台读取的命令 Hash，全程不变；下发 Seq 只在第 2 次起的重发时新分配，仅用于 MQTT 报文 `Seq` 与 `Data.CmdSeq` 回复匹配，每次尝试单独落一条 `cmd:{下发Seq}` 便于追溯。最终结果由 `CommandRepository.FinalizeAttemptsAsync` 回写到逻辑记录——它刻意不做终态保护（耗尽时要能把 `timed_out` 改成 `failed`），但**不会把 `completed` 降级**，用来兜住"更早某次尝试的迟到回复"。

Action 与回复主题映射（`CommandReplyTypeResolver`）：`get_state` → `state`，`set_pn` → `pn`，`get_config` → `config`，其它 → `point`。

### 设备上行（设备 → 平台）

MQTT → Channel → `MqttMessageWorker` → Handler 写 Redis（最新值 Hash + 历史 Stream）。`device/{id}/event/state` 中的老化状态（agingStart/agingIdle/agingStop/agingError）经 `runtime.CurrentAgingSeq → cmd:{seq} → RecordId` 关联，触发 `AgingRecordCallbackClient` 回调业务 API。

群控按字符串类型的 AgingNumber 独立运行：`device/public/deviceMode` 和 `device/public/agingStage` 的 Data 都必须携带 AgingNumber。Mode=2 时，批量 `start_aging` 的 Seq 作为内部 RunSeq、DeviceInfos 作为候选参与集合；只有设备真实上报 `agingStart` 才激活该批。type=1 点位通过 `device/{id}/event/TaskCP` 上报阶段完成，服务以当前批固定参与集合为屏障，全部完成后按架隔离发布 `server/rack/{AgingNumber}/CanNextstep` 放行（设备端订阅本架主题，不再使用公共主题）；实时在线列表只用于在线状态和诊断，不决定屏障成员。agingStage 对应的电源控制定向发布到该架唯一在线 type=2 总控的 `server/{controllerId}/cmd/CPower`，Standing/Discharge 关电，Recharge 开电。

## 关键约定与陷阱

- **Redis 键前缀以代码为准**：`RedisKeys.Prefix = "lgw"`。若看到历史/示例文档出现其它前缀（如 `lgw:` 或 `lgwtest2`）一律视为过时，不要照抄。
- **Redis 过期时间统一由 `RedisTtl.cs` 控制**：改 `DefaultDays`（当前 7 天）即可调整，不要在各 Repository 里写私有 TTL 常量。当前**有三类键设过期**：`lgw:device:{id}:cmd:{seq}`（命令 Hash）、`lgw:device:{id}:aging:{pn}:history`（采样历史 Stream）、`lgw:ota:{架}:run:{seq}:*`（OTA 批次 state/targets/recordids/level:*，正常结束时会主动清 `active`，TTL 只是防异常残留）。给新键加 TTL 前必须先看 `RedisTtl` 的注释清单，以下键**一律不设过期**：`lgw:cmd:seq`（Seq 回退）、`lgw:cmd:queue`（整键过期会连消费者组 `device-service` 一起消失，而消费组只在启动时创建一次 → 之后持续 NOGROUP 卡死，只能靠 MAXLEN 裁剪）、`runtime` 与 `event:state`（老化任务关联恢复源）、在线列表与设备目录、`group-control:racks` 与 `{n}:state`、`lgw:ota:racks`。另注意 TTL 是整键删除，不是按条目裁剪。
- 协议/行为约定以本文件和 `RedisKeys.cs` 为准。`docs/群控阶段超时与TTL改造方案.md` 是群控阶段超时、TTL 收敛、放行校验三项改造的设计文档（已全部实施），涉及该部分逻辑时可对照；历史引用的 `REDIS_STRUCTURE.md`、`CHANGELOG_LATEST_PROTOCOL.md` 已不存在。
- 全链路至少一次投递：重启恢复 Pending 时即使状态已是 published 也可能用原 Seq 重发 MQTT（防"假 published"），设备端按 `Seq/CmdSeq` 幂等去重是前提。
- 手工向 Stream 写命令必须用字段名 `data`；正常入口用 `CmdHelper.SendCmdAsync()`。
- `AgingNumber` 必须是非负整数格式的字符串（如 `"0"`）；数字 JSON 值 `0` 属于无效协议数据。
- `{Prefix}:devices:online` 必须是全量在线 List（元素为 `{"id":"...","type":"...","AgingNumber":"0"|null}` JSON）；type=1 同时进入 `{Prefix}:devices:online:aging:{AgingNumber}`，type=2 同时进入 `{Prefix}:devices:online:control`；此外**不分类型**地进入 `{Prefix}:ota:{AgingNumber}`（架号有效即收录）。三处都在 `DeviceRepository.SaveOnlineAsync` 里统一写入，下线由 `RemoveDeviceFromClassifiedOnlineListAsync` 统一清理（换架时会先清旧架条目，避免一台设备同时留在两个架）。`{Prefix}:ota:{AgingNumber}` 是 List、由上下线生命周期维护、**不设过期**；注意别和带层级后缀的 `{Prefix}:ota:{AgingNumber}:{MeshLevel}`（OTA 分层清单）混淆。历史上曾被错误写成 Set；生产库禁止 `FLUSHDB`。
- `DeviceId`、PN 等键片段经 `RedisKeys.Normalize*` 校验：不能含冒号、斜杠、空白；ReplyType 只允许 point/state/pn/config。
- **MQTT 的 QoS 与观测（2026-10-22 改造）**：**下发统一 QoS 1**（`MqttConnection.PublishAsync`），**上行订阅按主题分层定级**（`SubscribedTopics`，`(Topic, QoS)` 数组）：QoS 1 = 上下线、群控 deviceMode/agingStage、`device/+/event/+`、`device/+/cmd/reply/+`；**QoS 0 = `device/+/data/aging` 老化采样与 `device/+/log/+`**。采样保持 0 的原因：**Seq 是发送时刻**（每次发重新取），重投时 Seq 会变，**无法用 Seq 去重**——改 QoS 1 会产生拦不住的重复数据并重复 HTTP 上报，而丢一条采样的影响远小于重复一条。将来若要用 `PN + Data.TIMESTAMP` 做去重键，需先确认 TIMESTAMP 是采样时刻且精度不粗于采样间隔。端到端有效 QoS = `min(设备发布 QoS, 本端订阅 QoS)`，最终以 Broker SUBACK 授予值为准，**请求 1 被授予 0 会打 Error（降级不再静默）**；停机期间 QoS 0 主题的消息仍会全丢，靠 `OnlineStateRecoveryWorker` 主动对账补偿。订阅时打印"请求 QoS + 返回码"，返回码非 Granted 会打 Error（订阅被拒不再伪装成功）。发布**必须检查 `MqttClientPublishResult.IsSuccess/ReasonCode`**：MQTTnet 对 PUBACK 原因码是返回结果而不抛异常，未授权/配额拒绝会被当成成功。接收回调在**构造函数**中注册（早于 `ConnectAsync`），否则连接与注册之间存在丢消息窗口。`MqttConnection` 每 60 秒汇总一次收发计数（收到/入队/通道接近满/发布失败/未连接拒绝/通道积压）。
- **在线状态对账**（`OnlineStateRecoveryWorker`，配置段 `OnlineStateRecovery`）：服务启动延迟后、以及检测到 MQTT 从断开变为连接时，取「全量设备目录 `lgw:devices` ∪ 在线名单」的并集，逐台发 `get_state` 探测（并发 `MaxConcurrency`），**以命令回复为唯一在线判据**。在线但不在名单 → `DeviceLifecycleService.RecoverOnlineAsync` 补登记；不在但仍在名单 → `MarkOfflineAsync` 清理。`LastSeenUtc` 是**业务消息**时间、MQTT PING 不刷新它，所以**只能用来跳过探测，绝不能反过来判定离线**（空闲总控会被误杀）。`ProbeTimeoutSeconds` 必须大于 `ReplyTimeoutSeconds`。
  **⚠ 当前 `Enabled: false`（2026-10-22 关闭）**，原因：判据是"设备回了 get_state 才算在线"，若固件不响应这条命令、或回复不是 `device/{id}/cmd/reply/state` + 匹配 `CmdSeq`，则**在线但业务静默的设备会被误判离线并清掉**——风险最大的就是空闲总控。**重新开启前必须先验证**：手动给一台确定在线的设备发 `get_state`，确认日志出现 `设备回复已落库 ... ReplyType=state`。只探测"业务静默超过 `StaleMinutes`(5 分钟)"的设备，活跃设备不会被探。日志里 `Action=get_state` 且 `RecordId` 为空的命令就是对账探测（平台命令会带 RecordId）。
- **离线必须走 `DeviceLifecycleService.MarkOfflineAsync`**：它一次性完成"清三个在线列表 + 清 OTA 分层清单 + 群控剔除与重估"。`DeviceOfflineHandler` 与在线状态对账都调它。**禁止**在对账 Worker 里注入 `GroupControlService`/`RedisCmdWorker`/`OtaUpgradeService`（它们构造函数吃 `MqttConnection`，会形成 DI 环）。
- **日志约定：正常路径静默，只在异常/边界分支打点**，所以"控制台没输出"不等于"没在处理"。排查时别靠猜：① MQTT 接收侧（`MqttConnection` 的 `ApplicationMessageReceivedAsync`）默认只写 Channel、**不打日志**，开 `MqttSettings:LogIncomingMessages`（代码默认 false，`appsettings.json` 当前配 true）后会打印每条上行消息的 Topic+Payload（截断 500 字符）；② `MqttMessageWorker` 只在"没匹配到 handler"时 Warning，正常分发只打 `LogDebug`（需把 `Logging:LogLevel:Default` 调到 Debug 才看得到"交由哪个 handler 处理"）；③ `AgingDataHandler`（老化采样）、`DeviceLogHandler`、`DeviceEventHandler` 的 warn/fault/stepid 分支与正常老化状态回调**全程无日志**，只有解析失败/缺 PN/找不到 RecordId 才打；④ 确认消息有没有落库看 Redis（`lgw:devices:online`、`runtime`、`aging:{pn}:history`）比看控制台可靠。另注意 `MqttSettings:ClientId` 是固定值 + `WithCleanSession(false)`，**同时跑两个实例会互相踢**，表现为日志时有时无。
- `appsettings.json` 指向局域网环境（MQTT broker、Redis、回调 API 均为内网 IP）；`AgingCallback:AllowInvalidServerCertificate=true` 是为自签名证书故意保留的，不要移除。
- **群控阶段超时与半数放行**（`GroupControlService.EvaluateNextStepUnsafeAsync`，顺序固定为"先超时剔除 → 再判放行 → 最后按需起表"）：本阶段 `completed` 数达到 `Ceil(participants.Count / 2)` 起 10 分钟倒计时（deadline 存 `state.StageDeadlineUtc`，复用现有 1s 轮询，**不引入 Timer**）；到期把 `participants \ completed` 从 participants 剔除、记入 `...run:{runSeq}:excluded` 并写 `event:fault`，随后放行剩余设备。被剔除设备**本批次不再回归**（屏障以 participants 为准），只能等下一个 RunSeq。全员完成会清掉倒计时直接放行。详见 `docs/群控阶段超时与TTL改造方案.md`。
- **放行回复校验**：`CanNextstep` 发布前先为每台 participant `EnsureStoredAsync` 一条 `Action="can_next"` 的命令记录（群控指令不进 Stream），设备按现有协议回 `device/{id}/cmd/reply/point` + `Data.CmdSeq`，`Code==1` 才算确认。后台并发等待 → 失败则 `MarkTimedOut` 后**用原 Seq 整架重发一次**，重发前必须 `CommandRepository.ResetForRetryAsync`（否则 `WaitForCompletionAsync` 遇终态立即返回、重试形同虚设）→ 仍失败写 `event:fault` 并 publish `event:fault`。校验只留痕，**不回滚放行、不联动剔除 participants**。
- **终态 / 掉线设备必须立即移出 participants**：放行判定只看 `completed`，设备一旦上报 `agingStop/agingError/agingIdle` 或掉线（`device/public/down_line`），就不会再上报 TaskCP，若留在 participants 会永久阻塞整架放行。由 `GroupControlService.HandleAgingStateAsync`（终态分支）与 `HandleDeviceOfflineAsync`（掉线，`DeviceOfflineHandler` 调用）统一走 `ExcludeParticipantUnsafeAsync`：移出 participants → 记 `excluded` → 按 `RaiseFaultOnExclusion` 写 fault。参与集合被清空时走 `FinishRunUnsafeAsync` 直接结束批次，否则既不放行也不结束会挂死。
- **掉线设备可在时限内回归本批次**：剔除时同步写 `lgw:group-control:{n}:run:{seq}:excluded:meta`（Hash，field=DeviceId，值=JSON `{DeviceId,Reason,Stage,ExcludedAtUtc}`）。设备重新**上报 agingStart** 时（不是上线消息时——`runtime.AgingState` 只在状态上报时更新，上线瞬间读到的是掉线前的残留值，区分不出"网络抖动后仍在跑"和"掉电重启任务已丢"）走 `TryRejoinExcludedParticipantUnsafeAsync`：仅 `Reason=device_offline` 且离线时长 ≤ `ExcludedRejoinTimeoutMinutes`（默认 10）才加回 participants 并清掉 excluded/meta。阶段对齐：剔除期间若全架已推进到下一阶段（`meta.Stage != state.CurrentStage`），该设备错过了本阶段放行，按当前阶段已完成计入 `completed` 做豁免、从下一阶段起正常参与；否则按正常节点等它报 TaskCP。超时或终态/超时剔除的设备不回归，只能等下一个 RunSeq。
- **阶段倒计时起表条件默认不要求过半数**（`StageTimeoutRequireHalf=false`，只要本阶段有任意设备完成就起表）：若要求过半数，当多数设备掉线/终态时会永远起不了表，批次无兜底挂起。
- **放行回复校验会跳过离线设备**（`FilterOnlineParticipantsAsync`）：架广播送不到离线设备，等待它们只会产生"未收到确认"的假故障。
- **`AgingCommandGate` 有离线兜底释放**：设备掉线后不会再上报终态，发送门等待超过 `AgingCommandGate:OfflineReleaseTimeoutMinutes`（默认 30）且设备已离线时强制释放门。注意只在**设备已离线**时释放——设备在线时老化可能仍在进行（可能持续数小时），不能按时间强行插队。释放时除了清 `Reserved` 还会打 `ForceReleased` 标记允许**一条** `start_aging` 穿透 `agingStart`（`AgingState` 本身不变，否则会影响 `RedisCmdWorker` 依据该状态判断"是否重发已发布命令"）；穿透一条后立即清除，设备上报任何真实状态时也会清除，因此重新上线若仍报 `agingStart`，门会再次关闭。离线判据必须用 `IsDeviceOnlineAsync`（`lgw:devices:online`），不能读 `runtime.Online`——后者会被任意业务消息刷成 true。
- **OTA 分层升级**（`OtaUpgradeService`）：`Action=ota` 的批量指令分两种走向——**单设备直接按普通命令转发**（无 mesh 依赖，不管控）；**多设备由服务接管**（`RedisCmdWorker` 调 `TryTakeOverAsync` 后整条命令项 `MarkCompleted` 并 ACK，实际下发由服务后台调度）。架号取 `DeviceInfos[0]` 的 `runtime.AgingNumber`。
  - **任务表 = `DeviceInfos` ∩ 该架在线列表 `lgw:ota:{AgingNumber}`**（`DeviceRepository.GetOtaRackOnlineDeviceIdsAsync`）。为什么取交集：只用 `DeviceInfos` 会把离线设备算进来、白等一轮超时；只用在线列表会丢掉 `RecordId`（命令状态回写与老化关联都依赖它），还会升级平台没点名的设备。交集为空则**拒绝接管**。结果存 `lgw:ota:{架}:run:{seq}:targets`（Hash：deviceId → pending/upgraded/failed/skipped），RecordId 存 `:recordids`。
  - **完成判据 = 设备上报 OTACP（2026-10-22 变更）**：`device/{id}/event/OTACP`，`Data.IsComplete=1`，由 `Handlers/OtaCompletionHandler.cs` 解析后调 `OtaUpgradeService.MarkDeviceUpgradedAsync` 标 `upgraded`。**不再比对固件版本号**——新固件升级**不再重启**，版本号可能根本不变；`level:{层}:versions` 版本基线、`VerifyLevelAsync` 的版本比对、`VersionChanged` 全部已删除。OTACP **不携带 ota 指令 Seq**（payload 里的 `Seq` 是设备自己的消息序号），批次归属只能按 `deviceId` 反查该架活跃批次；`Data.AgingNumber` 会校验，非法则回退按 runtime 反查。**固件若不上报 OTACP，该设备必然超时标 failed。**
  - **层内再分组（`LevelGroupSize`，当前 2 台；第 2 层用 `SecondLevelGroupSize`，当前 2 台）**：该层设备按组大小切成若干组，不足一组也单独成组。**一组处理完才下发下一组，本层所有组处理完才进入下一层**。组号存在 `OtaBatchState.CurrentGroupIndex`（进新层重置为 1）。
  - **分组必须基于落库的层成员快照（`lgw:ota:{架}:run:{seq}:level:{层}:members`，Set）**：进入该层时按「`targets` ∩ 实时层清单」算一次并写入，本层内所有判定都读它（`BuildLevelGroupsAsync`）。**两个坑都踩过**：① 用"仍待升级的设备"切 → 每完成一台就少一台，组边界漂移、组号指向错误设备；② **每次按实时 `lgw:ota:{架}:{层}` 清单重算** → 设备一掉线就被移出该清单（`MarkOfflineAsync` 会清 MeshLevel 条目），成员数缩水甚至变 0，`CurrentGroupIndex > groups.Count` 成立 → **本层被误判成"全部完成"**（日志表现：`本层 0 组全部处理完成`），设备其实没升级且重新上线后也不会再被下发。用快照后，掉线设备只会停在 `pending`，继续等补发或等超时。
  - **组完成判定**：该组成员在 `targets` 中**都不再是 `pending`**（即 upgraded / failed / skipped）即本组处理完 → `AdvanceGroupAsync` 组号 +1、状态回 `Pending`，并置 `GroupDelayUntilUtc = 现在 + GroupDelaySeconds`（当前 5 秒）。`StartLevelAsync` 下发下一组前会检查该时刻，**组间固定延时 5 秒**。本层所有组处理完后 `StartLevelAsync` 判定"组号超过组数"→ `CompleteLevelAsync` 走层间延时；**这条路径不受组间延时影响**（最后一组后面没有下一组，不额外等 5 秒）。`AdvanceLevelAsync` / `CompleteLevelAsync` / `FinishBatchAsync` 都会清 `GroupDelayUntilUtc`，新层第 1 组不继承上一层的延时。
  - **层间固定延时** `LevelStabilizeSeconds`（**当前 10s**）后进入下一层。**mesh 稳定计数判定已删除**（`ExpectedOnlineCount` 一并移除）：不重启就不会掉线、mesh 不会重组，在线数恒定，"回到基准即稳定"恒为真、等于没有。
  - **层内周期性补发（`ResendIntervalMinutes`，当前 5 分钟）**：当前小组有设备迟迟不上报 OTACP 时，每 5 分钟把升级指令**重发一次**（`ResendGroupAsync`），只发给**仍是 `pending`** 的设备（已 upgraded 的不重刷），离线的跳过并计数。补发**不改变 payload 的 `Seq`**（`BuildOtaPayload` 固定用 `state.Seq`），设备端可按 Seq 幂等去重。每次补发 `state.ResendCount++` 并落库，日志以 `OTA 补发第 N 次（第 X 层 a/b 组）` 明确标记；进入新小组时 `ResendCount` 清零、`NextResendUtc` 重置。补发一直持续到本组 30 分钟超时为止（按 5 分钟算，最多约 5~6 次）。
  - **超时按小组独立计时，每层每小组 30 分钟**：每组下发后单独起计时，**前一组卡住不会吃掉后面组的时间预算**。当前 `BaseTimeoutMinutes=30`、`TimeoutIncrementMinutes=0`、`MaxLevelTimeoutMinutes=0`；`FirstLevelTimeoutMinutes` **未配置**（不配 = 沿用 `Base`，所以所有层统一 30 分钟）。该键仍保留在代码里，将来若要"第 1 层比后面长"再配——现有公式 `Base + (层-1)×Increment` 表达不了非递增关系，必须靠这个独立键。
  - **任何一层超时 = 本次 OTA 失败，直接取消后续一切**：`AbortOnLevelTimeout=true`（当前）+ `AbortOnFirstLevelTimeout=true`（默认）双重生效。超时设备先标 `failed`，然后**不进下一组、不进补发、不广播 CanRestart**（`allowRestart: false`），直接 `FinishBatchAsync`，原因写"第 N 层第 x/y 组升级超时，判定 OTA 升级失败，取消后续所有层级"。
  - 若把 `AbortOnLevelTimeout` 改回 `false`，第 2 层起超时只标 `failed` 并继续下一组（`failed` 可在更高层重试）——当前配置下这条路径走不到。
  - 注意 `dispatched` 现在只记录**当前小组**的下发情况、纯诊断用途；完成判定看的是组成员在 `targets` 里的状态。`dispatched:all`（累计）仍用于收尾补发算差集。
  - **收尾**：层数超过 `MaxMeshLevel` 先进 `finalizing` 补发阶段（取 `targets − dispatched:all` 差集直发），再 `FinishBatchAsync`；故障只在收尾时对**最终仍为 failed** 的设备写一次 `ota_upgrade_failed`，避免逐层重复告警。
  - **重启广播**：`FinishBatchAsync` 末尾向 `server/rack/{架}/CanRestart` 发 `{"Seq":<unix秒>,"Data":{"Restart":1}}`，让设备加载新固件。**不能放在"最后一层完成"那一刻**——`finalizing` 还会补发 ota，先重启会让补发设备没下完就重启。要求 `upgraded > 0` 才发（一台都没成时重启无意义）。开关 `SendCanRestartOnComplete`（默认 true）；发布失败只记 Error，**不影响批次收尾**（状态已落库、active 已释放）。注意此主题与群控的 `CanNextstep` 不同，前者在 `OtaUpgradeService` 里发，后者在 `GroupControlService` 里发。
  - **重启前等待剩余设备（`restarting` 阶段）**：补发结束后**不直接收尾**，由 `EnterRestartPhaseAsync` 判定——名单全部 upgraded → 立即收尾重启；**有未完成设备 → 进入 `Restarting`，起 `RestartWaitTimeoutMinutes`（当前 20 分钟）窗口**，窗口内全部补齐就立即广播重启，到期仍有未完成也照常重启（不再等）；一台都没升级成功 → 直接收尾、不重启。`VerifyRestartAsync` 每秒轮询该窗口。**关键：不能在这个阶段把 pending 标成 skipped**，否则窗口内上报 OTACP 的设备会被当成从未完成、统计也不准；`pending → skipped` 的统一转换留在 `FinishBatchAsync`。
  - **`FinishBatchAsync` 的 `allowRestart` 参数**：第 1 层超时判定整批失败时传 `false`，跳过重启广播（取消后续所有操作）；其它层中止仍按已升级情况决定是否重启。
- **下发过滤：只要"还没升级成功"就再给机会**（`IsDispatchable` 只排除 `upgraded`）。`pending`=未下发过、`skipped`=曾因离线/发布失败未下发、`failed`=下发过但时限内没确认版本号变化——**三者都会在后续层重新下发**。允许重试 failed 的原因：节点重启后 **MeshLevel 可能上移**，失败节点会在更高层重新进入清单；若 MeshLevel 没变它就不会出现在更高层清单里，重试次数被"真实的层级变化"天然限制住，不需要额外重试计数。下发前会先把状态置回 `pending`，避免同一台设备在多层被并发重复下发。
- **空层快跳**：某层没有可下发设备时**不等 mesh 稳定期**，直接进上一层——层数上移不能靠"连续空层"判定收尾。收尾只有两条路：名单中已无待升级设备直接结束；或层数超过 `MaxMeshLevel` 时**先进入补发阶段**（`LevelStatus=finalizing`）再结束。补发的原因：**分层下发必然覆盖不全**——节点重启后 `MeshLevel` 会漂移，某些设备在所有层的清单里都没出现过一次。`lgw:ota:{架}:run:{seq}:dispatched:all`（Set，累计）记录本批真正下发过的设备，补发阶段取 `targets − dispatched:all` 的差集（注意别用 `dispatched`，那个是当前层的、会被清空），**直接发 ota、不再按 mesh 分层**，离线的标 skipped，发完按约定**不等确认**立即收尾（刚下发的仍是 pending，会被 `FinishBatchAsync` 统一标 skipped，补发名单在"开始兜底补发"那条 Warning 日志里可追溯）。注意 `dispatched` **不参与层内过滤**（failed 设备仍可在更高层重试），只用于算这个差集。结束时统计 Total/Upgraded/Failed/Skipped 并清 `lgw:ota:{架}:active`。同架已有活跃批次时拒绝新指令（`RejectConcurrentBatchPerRack`）。OTACP 上报与 CanRestart 广播已一并移除。
- **上线报文的 `Data.MeshLevel` 维护 OTA 分层清单**：`DeviceOnlineHandler` 解析 `Data.MeshLevel`（整数，老固件没有则告警跳过）后写入 `lgw:ota:{AgingNumber}:{MeshLevel}`（List，元素 `{"id":"...","type":"...","AgingNumber":"88","MeshLevel":1}`，与在线列表同构），并把层级持久化到 `runtime.MeshLevel`。三处必须配套维护，否则清单会失真：① 写入前先移除该键内同设备的旧元素（重复上线不产生重复条目）；② 换架或换层级时按 `runtime` 里的旧值清掉旧分组条目（否则同设备同时留在两个分组）；③ `DeviceOfflineHandler` 下线时移除。该键**不设过期**（与在线列表一致，由上下线生命周期维护）。
- **服务端自产故障必须走 `DeviceRepository.SaveGroupControlFaultAsync`**，不要用 `SaveEventAsync`：后者内部会 `MarkSeenAsync()` 把 `runtime.Online` 置 true，会把离线设备"复活"成在线，进而污染总控唯一性判断。
- 群控状态保存在 `lgw:group-control:{AgingNumber}:*`；候选批次不会替换当前批次，首次真实 `agingStart` 才激活。当前批结束前若另一 RunSeq 上报 `agingStart`，拒绝切换并告警。TaskCP 仅接受当前批参与设备，并按 AgingNumber + DeviceId + 单调递增 Seq 去重。`CanNextstep` 发布到 `server/rack/{AgingNumber}/CanNextstep`（架号经 `RedisKeys.NormalizeAgingNumber` 规范化后拼接，架号非法时保留 pending 重试，不退化成公共广播），不依赖总控在线状态；只有 `CPower` 要求该架恰好存在一个在线总控，无总控或多个总控时保留电源 pending。群控直接指令不进入命令 Stream、不等待命令回复，发布失败或重启时使用原 Seq 重试。

## 配置

`CommandProcessing.ReplyTimeoutSeconds`（默认 15s）、`PublishRetryDelayMilliseconds`（默认 1000ms，同时是群控后台轮询间隔）、`GroupControlReleaseReplyTimeoutSeconds`（默认 15s，单台放行回复等待）、`StartAgingStaggerMilliseconds`（默认 0 = 不间隔）、`StartAgingReplyTimeoutSeconds`（默认 20s，start_aging 单次等待回复超时）、`StartAgingMaxAttempts`（默认 5，start_aging 总下发次数含首次，1 = 不重发）、`StartAgingRetryDelayMilliseconds`（默认 1000ms，两次下发间隔，0 = 不间隔）在 `appsettings.json` 中调整；消费组内 Consumer 名取 `RedisSettings:ConsumerName`，缺省为本机名。

**批量 `start_aging` 的错峰下发**：不同设备的命令队列是并行消费的，入队即意味着马上发布，因此同批指令默认会在同一瞬间打到整架设备。`StartAgingStaggerMilliseconds`（当前配 100）让 `DispatchBatchAsync` 在同一条批量指令内按顺序间隔入队，只对 `start_aging` 生效、最后一台后面不再等待。注意该延迟施加在 Stream 消费主循环里，**会阻塞新命令读取 N×间隔毫秒**（30 台约 3 秒）；已在各设备队列中的命令和 MQTT 上行不受影响。填 0 即恢复"无间隔并行下发"。

`GroupControl` 段（群控阶段超时与放行校验）：`StageTimeoutEnabled`（默认 true）、`StageTimeoutMinutes`（默认 10）、`StageTimeoutRequireHalf`（默认 false = 有任意设备完成即起表）、`HalfReleaseThresholdMode`（`Ceil`/`Floor`，默认 Ceil）、`ExcludedRejoinMode`（`OfflineWithinTimeout` = 掉线剔除后可在时限内回归，默认；`None` = 恢复"剔除后本批次不再回归"的旧行为）、`ExcludedRejoinTimeoutMinutes`（默认 10）、`RaiseFaultOnStageTimeout`、`ExcludeOnAgingTerminal`（默认 true）、`ExcludeOnDeviceOffline`（默认 true）、`RaiseFaultOnExclusion`（默认 true）、`ReleaseReplyVerificationEnabled`、`ReleaseReplyRetryCount`（默认 1）、`RaiseFaultOnReleaseReplyFailure`。

`OtaUpgrade` 段（OTA 分层升级）：`Enabled`（默认 true，关闭则所有 ota 都按普通命令直接转发）、`BaseTimeoutMinutes`（默认 10，**当前配 30**：每层每个小组的超时）、`FirstLevelTimeoutMinutes`（默认 0 = 沿用 `BaseTimeoutMinutes`，**当前未配置**；只在需要"第 1 层单独更长"时才加）、`TimeoutIncrementMinutes`（默认 10，每层递增，即代码缺省 L1=10、L2=20、L3=30）、`MaxLevelTimeoutMinutes`（默认 0 = 不封顶；单层超时上限，配成与"第 2 层理论值"相同可实现"第 1 层一个值、之后各层固定值"）。实际单层超时 = `min(Base + (层-1)×Increment, Max)`，**当前 `appsettings.json` 配为 10/0/0，即所有层一律 10 分钟**——注意 `MaxLevelTimeoutMinutes` 若留着非 0 旧值会封顶，改 `Base` 时必须同步检查它、`PollIntervalMilliseconds`（默认 1000）、`LevelStabilizeSeconds`（默认 60，**当前配 10**：本层**所有小组**处理完后的**固定层间延时**，不是超时窗口，也没有"在线数达标提前结束"的判定了；超时跳层不使用该等待）、`LevelGroupSize`（默认 3，**当前 2**：层内分组大小，一组处理完才发下一组）、`SecondLevelGroupSize`（默认 1，**当前 2**：第 2 层专用分组大小）、`GroupDelaySeconds`（默认 5，**当前 5**：组与组之间的固定延时；0 = 不延时，下一轮 1 秒轮询就发下一组）、`ResendIntervalMinutes`（默认 5，**当前 5**：组内设备迟迟不上报 OTACP 时的补发间隔；0 = 不补发，只等超时）、`MaxMeshLevel`（默认 10）、`RejectConcurrentBatchPerRack`（默认 true）、`RaiseFaultOnIncomplete`（默认 true）、`AbortOnLevelTimeout`（默认 false = 跳层继续；**当前 true**：任意层超时都判定 OTA 失败、取消后续所有层级与补发，且不广播重启）、`AbortOnFirstLevelTimeout`（默认 true，**当前 true**：第 1 层超时即判定失败；与上一项叠加，当前行为是"任何层超时都作废"）、`SendCanRestartOnComplete`（默认 true，批次收尾时广播 `server/rack/{架}/CanRestart` 让设备重启）、`RestartWaitTimeoutMinutes`（默认 20，**当前 20**：收尾时若仍有未升级设备，最多等这么久再重启；窗口内全部补齐则立即重启）。

`MqttSettings` 段：`LogIncomingMessages`（默认 false；true 时打印每条收到的上行消息 Topic+Payload，老化采样高频，排查完记得关）。

`AgingCommandGate` 段：`OfflineReleaseTimeoutMinutes`（默认 30，≤0 表示禁用离线兜底释放）。
