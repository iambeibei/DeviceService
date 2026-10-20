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
   - `start_aging`：受 `AgingCommandGate` 门控，只有设备上报 `agingIdle / agingStop / agingError` 才放行下一条；`timed_out` 不释放门（超时只说明没等到回复，不能证明 ESP32 没执行）。
3. 每条命令发布 MQTT `server/{deviceId}/cmd/{action}`，payload 固定 `{Seq, Data}`——无参数时 `"Data": null` 也必须存在。`RecordId` 不进 MQTT payload，只存 Redis。
4. 等四类回复主题 `device/{id}/cmd/reply/{point|state|pn|config}` 中匹配 `CmdSeq` 的回复落库，或超时标 `timed_out`，才处理同设备下一条；一条 Stream Entry 的所有设备子命令都结束后才 XACK。

回复等待机制：进程内 `TaskCompletionSource` 唤醒 + 超时边界复查 Redis Hash（`CommandRepository.WaitForCompletionAsync`）。命令最终状态以 Redis Hash `{Prefix}:device:{id}:cmd:{seq}` 为准，不要只依赖内存通知。

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
- `{Prefix}:devices:online` 必须是全量在线 List（元素为 `{"id":"...","type":"...","AgingNumber":"0"|null}` JSON）；type=1 同时进入 `{Prefix}:devices:online:aging:{AgingNumber}`，type=2 同时进入 `{Prefix}:devices:online:control`。历史上曾被错误写成 Set；生产库禁止 `FLUSHDB`。
- `DeviceId`、PN 等键片段经 `RedisKeys.Normalize*` 校验：不能含冒号、斜杠、空白；ReplyType 只允许 point/state/pn/config。
- `appsettings.json` 指向局域网环境（MQTT broker、Redis、回调 API 均为内网 IP）；`AgingCallback:AllowInvalidServerCertificate=true` 是为自签名证书故意保留的，不要移除。
- **群控阶段超时与半数放行**（`GroupControlService.EvaluateNextStepUnsafeAsync`，顺序固定为"先超时剔除 → 再判放行 → 最后按需起表"）：本阶段 `completed` 数达到 `Ceil(participants.Count / 2)` 起 10 分钟倒计时（deadline 存 `state.StageDeadlineUtc`，复用现有 1s 轮询，**不引入 Timer**）；到期把 `participants \ completed` 从 participants 剔除、记入 `...run:{runSeq}:excluded` 并写 `event:fault`，随后放行剩余设备。被剔除设备**本批次不再回归**（屏障以 participants 为准），只能等下一个 RunSeq。全员完成会清掉倒计时直接放行。详见 `docs/群控阶段超时与TTL改造方案.md`。
- **放行回复校验**：`CanNextstep` 发布前先为每台 participant `EnsureStoredAsync` 一条 `Action="can_next"` 的命令记录（群控指令不进 Stream），设备按现有协议回 `device/{id}/cmd/reply/point` + `Data.CmdSeq`，`Code==1` 才算确认。后台并发等待 → 失败则 `MarkTimedOut` 后**用原 Seq 整架重发一次**，重发前必须 `CommandRepository.ResetForRetryAsync`（否则 `WaitForCompletionAsync` 遇终态立即返回、重试形同虚设）→ 仍失败写 `event:fault` 并 publish `event:fault`。校验只留痕，**不回滚放行、不联动剔除 participants**。
- **终态 / 掉线设备必须立即移出 participants**：放行判定只看 `completed`，设备一旦上报 `agingStop/agingError/agingIdle` 或掉线（`device/public/down_line`），就不会再上报 TaskCP，若留在 participants 会永久阻塞整架放行。由 `GroupControlService.HandleAgingStateAsync`（终态分支）与 `HandleDeviceOfflineAsync`（掉线，`DeviceOfflineHandler` 调用）统一走 `ExcludeParticipantUnsafeAsync`：移出 participants → 记 `excluded` → 按 `RaiseFaultOnExclusion` 写 fault。参与集合被清空时走 `FinishRunUnsafeAsync` 直接结束批次，否则既不放行也不结束会挂死。
- **掉线设备可在时限内回归本批次**：剔除时同步写 `lgw:group-control:{n}:run:{seq}:excluded:meta`（Hash，field=DeviceId，值=JSON `{DeviceId,Reason,Stage,ExcludedAtUtc}`）。设备重新**上报 agingStart** 时（不是上线消息时——`runtime.AgingState` 只在状态上报时更新，上线瞬间读到的是掉线前的残留值，区分不出"网络抖动后仍在跑"和"掉电重启任务已丢"）走 `TryRejoinExcludedParticipantUnsafeAsync`：仅 `Reason=device_offline` 且离线时长 ≤ `ExcludedRejoinTimeoutMinutes`（默认 10）才加回 participants 并清掉 excluded/meta。阶段对齐：剔除期间若全架已推进到下一阶段（`meta.Stage != state.CurrentStage`），该设备错过了本阶段放行，按当前阶段已完成计入 `completed` 做豁免、从下一阶段起正常参与；否则按正常节点等它报 TaskCP。超时或终态/超时剔除的设备不回归，只能等下一个 RunSeq。
- **阶段倒计时起表条件默认不要求过半数**（`StageTimeoutRequireHalf=false`，只要本阶段有任意设备完成就起表）：若要求过半数，当多数设备掉线/终态时会永远起不了表，批次无兜底挂起。
- **放行回复校验会跳过离线设备**（`FilterOnlineParticipantsAsync`）：架广播送不到离线设备，等待它们只会产生"未收到确认"的假故障。
- **`AgingCommandGate` 有离线兜底释放**：设备掉线后不会再上报终态，发送门等待超过 `AgingCommandGate:OfflineReleaseTimeoutMinutes`（默认 30）且设备已离线时强制释放门。注意只在**设备已离线**时释放——设备在线时老化可能仍在进行（可能持续数小时），不能按时间强行插队。释放时除了清 `Reserved` 还会打 `ForceReleased` 标记允许**一条** `start_aging` 穿透 `agingStart`（`AgingState` 本身不变，否则会影响 `RedisCmdWorker` 依据该状态判断"是否重发已发布命令"）；穿透一条后立即清除，设备上报任何真实状态时也会清除，因此重新上线若仍报 `agingStart`，门会再次关闭。离线判据必须用 `IsDeviceOnlineAsync`（`lgw:devices:online`），不能读 `runtime.Online`——后者会被任意业务消息刷成 true。
- **OTA 分层升级**（`OtaUpgradeService`，已取代旧的"整架下发 + 广播 CanRestart"方案）：`Action=ota` 的批量指令分两种走向——**单设备直接按普通命令转发**（无 mesh 依赖，不管控）；**多设备由服务接管**：`RedisCmdWorker` 调 `TryTakeOverAsync` 后把整条指令的命令项一次性 `MarkCompleted`（消息写明已接管）并 ACK，实际下发由服务后台调度。架号只取 `DeviceInfos[0]` 的 `runtime.AgingNumber`。升级名单 = `DeviceInfos` 全集（`lgw:ota:{架}:run:{seq}:targets`，Hash：deviceId → pending/upgraded/failed/skipped），**分母就是 DeviceInfos 总数**。调度按 **Mesh 层级从 1（根节点）开始逐层向上**，下发对象 = 名单中仍为 pending ∩ `lgw:ota:{架}:{层}` 清单；层内并行、层间串行。每层下发前记录该层设备 `runtime.FirewareVersion`（**模型里没这个字段，必须走 `DeviceRepository.GetFirmwareVersionAsync` 单独取**），升级后对比：旧版本非空看变化，旧版本为空则"变为非空"即成功。本层 pending 清空即完成 → 进入 `LevelStabilizeSeconds`（默认 60s）的 mesh 稳定等待（上层重启会让下层短暂掉线，不等待会把它们误判成不在名单里）→ 下一层。单层超时 = `BaseTimeoutMinutes + (层-1)×TimeoutIncrementMinutes`（代码缺省 L1=10、L2=20…，当前 `appsettings.json` 配成 L1=20、L2=30…），超时设备标 failed + fault 后**跳层继续**（`AbortOnLevelTimeout=true` 改为任意层超时都中止整批）。**首层门禁**（`AbortOnFirstLevelFailure`，默认 true，优先级高于 `AbortOnLevelTimeout`）：第 1 层（根节点）任一设备超时失败即直接 `FinishBatchAsync` 结束整批，后续层不再下发——根节点没起来整条 mesh 就收不到包，继续下发只会逐层空等。只认"已下发但未确认"的超时失败，**空层快跳不经过该判定**，不会被误伤。
- **下发过滤只看 targets 状态，不重复下发**：只有 `pending`（未下发过）与 `skipped`（曾因离线/发布失败未下发，允许后续层重试）会被下发；`upgraded`、`failed` 一律跳过。节点重启后 **MeshLevel 可能上移**，它会进入更高层的清单，但状态已是 upgraded 所以不会被二次下发；下发前会先把状态置回 pending，避免 skipped 设备在多个层被重复下发。
- **空层快跳**：某层没有可下发设备时**不等 mesh 稳定期**，直接进上一层——层数上移不能靠"连续空层"判定收尾。结束条件只有两个：名单中已无 pending/skipped 设备，或层数超过 `MaxMeshLevel`。结束时把剩余 pending 标 skipped，统计 Total/Upgraded/Failed/Skipped 并清 `lgw:ota:{架}:active`。同架已有活跃批次时拒绝新指令（`RejectConcurrentBatchPerRack`）。OTACP 上报与 CanRestart 广播已一并移除。
- **上线报文的 `Data.MeshLevel` 维护 OTA 分层清单**：`DeviceOnlineHandler` 解析 `Data.MeshLevel`（整数，老固件没有则告警跳过）后写入 `lgw:ota:{AgingNumber}:{MeshLevel}`（List，元素 `{"id":"...","type":"...","AgingNumber":"88","MeshLevel":1}`，与在线列表同构），并把层级持久化到 `runtime.MeshLevel`。三处必须配套维护，否则清单会失真：① 写入前先移除该键内同设备的旧元素（重复上线不产生重复条目）；② 换架或换层级时按 `runtime` 里的旧值清掉旧分组条目（否则同设备同时留在两个分组）；③ `DeviceOfflineHandler` 下线时移除。该键**不设过期**（与在线列表一致，由上下线生命周期维护）。
- **服务端自产故障必须走 `DeviceRepository.SaveGroupControlFaultAsync`**，不要用 `SaveEventAsync`：后者内部会 `MarkSeenAsync()` 把 `runtime.Online` 置 true，会把离线设备"复活"成在线，进而污染总控唯一性判断。
- 群控状态保存在 `lgw:group-control:{AgingNumber}:*`；候选批次不会替换当前批次，首次真实 `agingStart` 才激活。当前批结束前若另一 RunSeq 上报 `agingStart`，拒绝切换并告警。TaskCP 仅接受当前批参与设备，并按 AgingNumber + DeviceId + 单调递增 Seq 去重。`CanNextstep` 发布到 `server/rack/{AgingNumber}/CanNextstep`（架号经 `RedisKeys.NormalizeAgingNumber` 规范化后拼接，架号非法时保留 pending 重试，不退化成公共广播），不依赖总控在线状态；只有 `CPower` 要求该架恰好存在一个在线总控，无总控或多个总控时保留电源 pending。群控直接指令不进入命令 Stream、不等待命令回复，发布失败或重启时使用原 Seq 重试。

## 配置

`CommandProcessing.ReplyTimeoutSeconds`（默认 15s）、`PublishRetryDelayMilliseconds`（默认 1000ms，同时是群控后台轮询间隔）、`GroupControlReleaseReplyTimeoutSeconds`（默认 15s，单台放行回复等待）、`StartAgingStaggerMilliseconds`（默认 0 = 不间隔）在 `appsettings.json` 中调整；消费组内 Consumer 名取 `RedisSettings:ConsumerName`，缺省为本机名。

**批量 `start_aging` 的错峰下发**：不同设备的命令队列是并行消费的，入队即意味着马上发布，因此同批指令默认会在同一瞬间打到整架设备。`StartAgingStaggerMilliseconds`（当前配 100）让 `DispatchBatchAsync` 在同一条批量指令内按顺序间隔入队，只对 `start_aging` 生效、最后一台后面不再等待。注意该延迟施加在 Stream 消费主循环里，**会阻塞新命令读取 N×间隔毫秒**（30 台约 3 秒）；已在各设备队列中的命令和 MQTT 上行不受影响。填 0 即恢复"无间隔并行下发"。

`GroupControl` 段（群控阶段超时与放行校验）：`StageTimeoutEnabled`（默认 true）、`StageTimeoutMinutes`（默认 10）、`StageTimeoutRequireHalf`（默认 false = 有任意设备完成即起表）、`HalfReleaseThresholdMode`（`Ceil`/`Floor`，默认 Ceil）、`ExcludedRejoinMode`（`OfflineWithinTimeout` = 掉线剔除后可在时限内回归，默认；`None` = 恢复"剔除后本批次不再回归"的旧行为）、`ExcludedRejoinTimeoutMinutes`（默认 10）、`RaiseFaultOnStageTimeout`、`ExcludeOnAgingTerminal`（默认 true）、`ExcludeOnDeviceOffline`（默认 true）、`RaiseFaultOnExclusion`（默认 true）、`ReleaseReplyVerificationEnabled`、`ReleaseReplyRetryCount`（默认 1）、`RaiseFaultOnReleaseReplyFailure`。

`OtaUpgrade` 段（OTA 分层升级）：`Enabled`（默认 true，关闭则所有 ota 都按普通命令直接转发）、`BaseTimeoutMinutes`（默认 10，第 1 层超时）、`TimeoutIncrementMinutes`（默认 10，每层递增，即代码缺省 L1=10、L2=20、L3=30；当前 `appsettings.json` 配置为 L1=20、L2=30）、`PollIntervalMilliseconds`（默认 1000）、`LevelStabilizeSeconds`（默认 60，层间 mesh 稳定等待，填 0 即不等；空层不使用该等待）、`MaxMeshLevel`（默认 10）、`RejectConcurrentBatchPerRack`（默认 true）、`RaiseFaultOnIncomplete`（默认 true）、`AbortOnLevelTimeout`（默认 false = 跳层继续；true = 中止整批）、`AbortOnFirstLevelFailure`（默认 true = 第 1 层任一设备超时失败即中止整批，后续层不再下发；false = 首层也只跳层继续）。

`AgingCommandGate` 段：`OfflineReleaseTimeoutMinutes`（默认 30，≤0 表示禁用离线兜底释放）。
