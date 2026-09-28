# 群控阶段超时、Redis TTL 统一、放行指令响应校验 — 改造方案

> 状态：**三项全部实施完成**。`dotnet build DeviceService.slnx` → **0 错误**（4+2 个警告均为改动前既有）。
> - 第 1 项 TTL：已实施，见 §1.4 / 顶部"已确认并已实施"。
> - 第 2 项阶段超时：已实施（**Ceil** 取整、10 分钟、剔除后**不回归**），见 **§7.1 / §8**。
> - 第 3 项放行回复校验：已实施（**线路 A**，设备按现有 `Data.CmdSeq` 协议回复），见 **§7.2 / §8**。
> - 用户最终裁决：① 半数取整 = **Ceil**；② `GroupControlRepository.RunTtl` **不改**，保持私有常量 7 天；③ 校验失败**不联动剔除 participants**。
> 验证手段：项目无测试项目，验证即为编译通过（0 错误）+ 手工冒烟（见 §7.4）。

### 已确认并已实施（第 1 项收敛范围）

用户确认：**只有两个键设置过期时间**，`lgw:cmd:queue` 不设。

| 键 | 类型 | 过期 | 实现 |
|---|---|---|---|
| `lgw:device:{id}:cmd:{seq}` | Hash | `RedisTtl.Command`（7 天） | `CommandRepository` 全部写入点引用常量 |
| `lgw:device:{id}:aging:{pn}:history` | Stream | `RedisTtl.DeviceAgingHistory`（7 天） | `DeviceRepository.SaveAgingDataAsync` 传 `ttl`，每次采样刷新 |
| `lgw:cmd:queue` | Stream | **不设** | 保持仅 MAXLEN 裁剪，避免整键过期连消费者组一起消失 |

改动文件：新增 `Bluetti.Device.Redis/RedisTtl.cs`（`DefaultDays = 7` 为唯一改动点）；
`RedisHelper.StreamAddAsync / ObjectToStreamAsync` 新增可选 `ttl` 参数（默认 null = 不设过期）；
`CommandRepository` 删除私有 `CommandTtl` 改为引用 `RedisTtl.Command`。
编译：`dotnet build DeviceService.slnx` → 0 错误（6 个警告均为改动前既有警告）。

> 过期语义提醒：**整键删除**。Hash 过期整条消失；Stream 过期整条流消失（不是按条目裁剪），
> 某 PN 停止采样超过 7 天后历史会整体清空。物理删除由 Redis 惰性 + 主动抽样完成，业务上"过期即不可见"。

---

## 0. 现状盘点（事实基线）

| 项 | 现状 |
|---|---|
| TTL 支持面 | `RedisHelper` 中**仅** `ObjectToHashAsync(ttl)` 与 `StringSetAsync(expiry)` 支持过期；`SetAddAsync / SortedSetAddAsync / HashSetAsync / HashSetIfNotExistsAsync / ListRightPushAsync / StreamAddAsync / ObjectToStreamAsync` **均无 TTL 参数** |
| 已设 TTL 的键 | 仅两处：`CommandRepository.CommandTtl`（7 天，命令 Hash）、`GroupControlRepository.RunTtl`（7 天，批次键），且是各自类的私有常量 |
| 群控放行 | `GroupControlService.TryPublishNextStepUnsafeAsync()` 发布后即视为成功，**不等待任何设备回复，也不登记命令状态** |
| 命令回复解析 | `CommandReplyPayload : PayloadBase<CommandReplyData>` 要求 `{"Seq":n,"Data":{"CmdSeq":..,"Code":..,"Msg":..,"CmdData":..}}` |
| 故障写入 | 仅 `DeviceEventHandler` 会写 `event/fault`；`SaveEventAsync()` 内部会 `MarkSeenAsync()`（**副作用：把设备标记为 Online=true**） |
| 群控后台节拍 | `GroupControlService.ExecuteAsync()` 每 `CommandProcessing:PublishRetryDelayMilliseconds`（默认 1s）遍历所有架，架内持 `SemaphoreSlim` 串行 |

---

## 1. Redis 写入过期时间统一

### 1.1 新增全局常量类

新增文件 `Bluetti.Device.Redis/RedisTtl.cs`：

```csharp
public static class RedisTtl
{
    public const int DefaultDays = 7;              // 唯一改动点
    public static readonly TimeSpan Default = TimeSpan.FromDays(DefaultDays);
    public static readonly TimeSpan Command      = Default;   // lgw:device:{id}:cmd:{seq}
    public static readonly TimeSpan GroupControl = Default;   // group-control 除 state/racks 外
    public static readonly TimeSpan DeviceState  = Default;   // event/log/reply/aging
    public static readonly TimeSpan Stream       = Default;   // 各类 history Stream
}
```

所有现有私有常量（`CommandRepository.CommandTtl`、`GroupControlRepository.RunTtl`）改为引用 `RedisTtl.*`，删除本地常量。

### 1.2 `RedisHelper` 改造点（新增可选 ttl 参数）

| 方法 | 改动 |
|---|---|
| `SetAddAsync(key, value)` | 加 `TimeSpan? ttl = RedisTtl.Default`（写后 `KeyExpireAsync`） |
| `SortedSetAddAsync(key, member, score)` | 同上 |
| `HashSetAsync(key, entries)` | 同上；传 `null` 表示不设 TTL |
| `HashSetIfNotExistsAsync(key, field, value)` | 同上 |
| `ListRightPushAsync(key, value)` | 同上 |
| `StreamAddAsync / ObjectToStreamAsync` | 加 `TimeSpan? ttl`，写后 `KeyExpireAsync` |
| `ObjectToHashAsync` | 已是 `TimeSpan? expire = null`，调用方统一传 `RedisTtl.*` |

约定：`ttl = null` 表示**显式不设过期**（用于下方排除清单），调用方必须显式传参，避免"默认忘记传"。

### 1.3 逐键调整清单

**A. 纳入统一 TTL（`RedisTtl.Default`，7 天，刷新式续期）**

| 键 / 模式 | 写入位置 |
|---|---|
| `lgw:device:{id}:cmd:{seq}` | `CommandRepository`（已 7 天，改为引用常量） |
| `lgw:device:{id}:reply:{type}` 与 `:history` | `DeviceRepository.SaveCommandReplyAsync` |
| `lgw:device:{id}:log:{type}` 与 `:history` | `DeviceRepository.SaveLogAsync` |
| `lgw:device:{id}:aging:{pn}:latest` / `:history` | `DeviceRepository.SaveAgingDataAsync` |
| `lgw:device:{id}:event:{type}:history`（Stream） | `DeviceRepository.SaveEventAsync` |
| `lgw:cmd:queue`（Stream） | `CommandRepository.EnqueueAsync` |
| `lgw:group-control:{n}:completed`、`{n}:taskcp:last-seq` | `GroupControlRepository` |
| `...:run:{runSeq}:participants/:started/:completed/:terminal`、`:state` | `GroupControlRepository`（已 7 天，改为引用常量） |
| 新增 `:run:{runSeq}:excluded`（见 §2） | 新增方法 |

**B. 必须排除（显式 `ttl = null`），理由如下**

| 键 | 排除理由 |
|---|---|
| `lgw:cmd:seq` | 自增序列，过期会导致 **Seq 回退、命令号重复**，是严重事故 |
| `lgw:devices`（设备目录 Set） | `DeviceRepository.GetAllAsync()` 依赖它做批次恢复，过期后设备"消失" |
| `lgw:devices:last_seen` | 设备目录附属 |
| `lgw:devices:online` / `:online:control` / `:online:aging:{n}` | 由 `up_line`/`down_line` 生命周期维护；若统一 7 天且长期无上下线事件，列表会整体消失，导致"无总控 → CPower 永久 pending"、"在线诊断失效" |
| `lgw:group-control:racks` | 架注册表；过期后所有架不再被轮询，群控整体停摆 |
| `lgw:group-control:{n}:state` | 架当前状态（Mode / CurrentRunSeq / Pending）；过期等于**整架状态被重置** |
| `lgw:device:{id}:runtime` | **高风险**：`GetAgingStateAsync`、`GetCurrentAgingTaskIdAsync`、`CurrentAgingRecordId`、设备 `DeviceType/AgingNumber` 全在此 Hash。若过期 → `HandleAgingStateAsync` 认为设备无 AgingNumber 直接 return，正在运行的批次**永远无法结束** |
| `lgw:device:{id}:event:state` | 存 `TaskQueue` / `CurrentAgingSeq`，是任务关联的回退读取源 |

> 结论：**"当前状态类"键保持永久，"历史/过程类"键统一 7 天**。这是本项最重要的判断。

### 1.4 边界与风险

1. **刷新式 TTL 的语义**：每次写入都 `EXPIRE` 续期，长期运行的键不会过期；连续 7 天无写入的历史键会整体消失（Stream 尤其明显）。若希望历史数据更持久，把 `DefaultDays` 调大即可。
2. **`runtime` 与 `event:state` 若日后要加 TTL**，必须先为 `CurrentAgingSeq`/`CurrentAgingRecordId` 建立独立不过期的恢复源，否则会击穿已有的"命令 Hash 过期 → 回退 runtime"容错链。
3. **Stream 的 TTL 与 MAXLEN 并存**：`XADD MAXLEN ~100000` + `EXPIRE` 双裁剪，超长历史仍可能被时间裁剪，属预期。
4. 改造后需回归：`CommandRepository` 命令 Hash 7 天过期 → `RecordId` 丢失的既有回退路径不能受影响。
5. `RedisHelper` 增加 TTL 参数后，**所有现有调用点都要显式传值**（编译器不强制，需人工核对），建议逐文件走查一遍。

---

## 2. 阶段超时与半数放行

### 2.1 目标行为

在 `Mode=2` 且有 `CurrentRunSeq` 的前提下：

1. 本阶段 `completed` 数量 **达到 participants 的半数阈值** → 启动 10 分钟倒计时；
2. 倒计时内 `completed` 覆盖全部 participants → **立即清除计时**，走现有正常放行；
3. 倒计时到期仍未全覆盖 → 把未完成设备**从 participants 剔除**、写 `event/fault` 故障记录，然后放行剩余设备进入下一阶段。

### 2.2 半数取整规则（需你确认）

推荐 **向上取整**：`threshold = (int)Math.Ceiling(participants.Count / 2.0)`

| participants | 阈值（Ceil，推荐） | 阈值（Floor，备选） |
|---|---|---|
| 1 | 1 | 0 |
| 2 | 1 | 1 |
| 3 | 2 | 1 |
| 4 | 2 | 2 |
| 5 | 3 | 2 |
| 6 | 3 | 3 |

选 Ceil 的理由：Floor 在 n=3/5 时只需 1~2 台完成就起表，倒计时覆盖面过大，容易过早剔除正常设备；Ceil 更接近"过半"语义。做成配置项 `HalfReleaseThresholdMode: Ceil | Floor`，默认 `Ceil`。
补充规则：n=1 时阈值=1，但完成即全覆盖，会直接放行，不会真的进入倒计时（无害）。

### 2.3 状态字段新增

`Bluetti.Device.Redis/Models/GroupControlState.cs` 增加：

```csharp
public DateTime? StageDeadlineUtc { get; set; }   // 倒计时到期时刻，null=未计时
public long? LastStageTimeoutSeq  { get; set; }   // 上一次超时放行使用的 Seq（审计/排重用）
```

持久化在 `lgw:group-control:{n}:state`，**重启后可恢复**（与现有 `PendingNextStep` 同样的持久化语义）。

### 2.4 涉及文件与函数

| 文件 | 函数 | 改动 |
|---|---|---|
| `Bluetti.Device.Redis/Models/GroupControlState.cs` | — | 新增 2 个字段 |
| `Bluetti.Device.Redis/GroupControlRepository.cs` | 新增 | `RemoveRunParticipantsAsync(agingNumber, runSeq, deviceIds)`、`AddRunExcludedDeviceAsync(...)`、`GetRunExcludedDeviceIdsAsync(...)` |
| `Bluetti.Device.Redis/RedisKeys.cs` | 新增 | `GroupControlRunExcludedDevices(agingNumber, runSeq)` |
| `DeviceService/Services/GroupControlService.cs` | `EvaluateNextStepUnsafeAsync` | ① 到期检查与剔除；② 达阈值启动计时；③ 全覆盖清除计时 |
| 同上 | `TryPublishNextStepUnsafeAsync` | 放行成功后清 `StageDeadlineUtc` |
| 同上 | `UpdateModeAsync` / `HandleAgingStateAsync`(终态) / `ResetRackActivity` | 批次切换、结束、Mode 切 1 时清计时 |
| 同上 | 新增 `ExpireStageTimeoutUnsafeAsync(...)` | 剔除 + 写故障 + publish 告警 |
| `DeviceService/Services/GroupControlService.cs` 构造 | — | 新增注入 `RedisHelper`（用于 publish `event:fault`） |
| `Bluetti.Device.Redis/DeviceRepository.cs` | 新增 `SaveGroupControlFaultAsync(...)` | 写故障但**不触发 MarkSeen**（见 §2.7） |

### 2.5 关键流程

`EvaluateNextStepUnsafeAsync`（持 `rackLock`）内，顺序固定为 **先超时、再放行、最后起表**：

```
participants = ...; completed = ...;
canRelease = participants.Count > 0 && participants.All(completed.Contains)

if (StageTimeoutEnabled && !canRelease && StageDeadlineUtc.HasValue && now >= StageDeadlineUtc)
{
    missing = participants \ completed
    foreach (d in missing) { SRem participants d; SAdd excluded d; 写 fault + publish }
    StageDeadlineUtc = null; LastStageTimeoutSeq = <本次放行 Seq>;
    SaveState;  // 重新读取 participants
    participants = 重新读取; canRelease = 重新计算;
}

if (canRelease) { StageDeadlineUtc = null; → TryPublishNextStepUnsafe(...); return; }

threshold = Ceil(participants.Count / 2.0)
if (StageTimeoutEnabled && completed.Count >= threshold && !StageDeadlineUtc.HasValue)
    StageDeadlineUtc = now + StageTimeoutMinutes; SaveState;
```

计时到期由 **现有后台轮询**触发（`ExecuteAsync → ProcessRackAsync → EvaluateNextStepUnsafeAsync`，1s 节拍），**不引入 `System.Timers`**，保证：架锁内串行、重启可恢复、无额外线程。

### 2.6 被剔除设备是否可再次参与

推荐 **方案 A（当前批次内不再回归）**：

- 剔除后从 `participants` 移除、写入 `...:run:{runSeq}:excluded`，当前批次后续所有阶段都不再等它（避免它落后一个阶段后补报的 `TaskCP` 打乱屏障计数）；
- 它随后到达的 `TaskCP` 会被 `RecordTaskCompletionAsync` 判为"非当前批次参与设备"丢弃并告警（现有逻辑，无需改动）；
- **新批次（新 RunSeq）会自动重新纳入**：`RegisterAgingBatchAsync` 重新登记 participants，excluded 随批次键 TTL 自然消亡。

备选 **方案 B（下一阶段允许回归）**：阶段放行后把 excluded 设备重新 SAdd 回 participants。风险是阶段错位 + `taskcp:last-seq` 单调去重导致其补报被丢弃，需额外清理 last-seq。**不推荐**，除非业务明确要求。

配置项 `GroupControl:ExcludedRejoinMode` = `None`（默认）| `NextStage`。

### 2.7 故障记录：字段与写入时机

写入位置：`lgw:device:{id}:event:fault`（Hash）+ `...:event:fault:history`（Stream），与现有 `DeviceEventHandler` 的 fault 完全同构，便于平台统一查询。

Hash 字段（复用 `DeviceRepository.SaveEventAsync` 的字段集）：

| 字段 | 值 |
|---|---|
| `DeviceId` | 被剔除设备 ID |
| `EventType` | `fault` |
| `Seq` | 触发本次放行的放行指令 Seq（便于与放行指令关联） |
| `State` | 空 |
| `Message` | `群控阶段等待超时，设备未上报阶段完成，已剔除出当前批次` |
| `Payload` | 构造 JSON（见下） |
| `ReceivedAtUtc` | UTC 现在 |

`Payload` 建议结构：

```json
{"Seq":123456,"Data":{"AgingNumber":"0","RunSeq":88,"Stage":"Discharge",
 "Reason":"StageTimeout","TimeoutMinutes":10,
 "CompletedCount":3,"ParticipantCount":5,"Excluded":true}}
```

同时 **publish** 到 `RedisKeys.channelFault`（`lgw:event:fault`），结构与 `DeviceEventHandler` 保持一致：
`{DeviceID, type:"fault", Message, ReceivedAtUtc}` —— 保证告警链路统一。

**写入时机**：与剔除动作在**同一把 rackLock 内、放行之前**完成（先留痕再放行，便于事后追溯）。故障写入失败只记日志，**不阻断放行**（沿用现有"Redis 异常不能卡住真实设备状态"的设计约定）。

**⚠ 必须处理的副作用**：现有 `SaveEventAsync()` 内部调用 `MarkSeenAsync()`，会把 `runtime.Online` 置为 `true` 并刷新 `LastSeenUtc`——给一台离线设备写故障会把它"复活"成在线，进而影响总控唯一性判断与在线诊断。因此**新增** `DeviceRepository.SaveGroupControlFaultAsync(...)`，落库字段与 `SaveEventAsync` 一致但**跳过 `MarkSeenAsync`**。

### 2.8 计时器的重置与并发

| 事件 | 处理 |
|---|---|
| 达阈值首次触发 | 仅当 `StageDeadlineUtc == null` 才设置（幂等，重复 TaskCP 不会续期） |
| completed 覆盖全部 → 正常放行 | 清 `StageDeadlineUtc`（`TryPublishNextStepUnsafeAsync` 成功分支） |
| 超时剔除后放行 | 清 `StageDeadlineUtc`，写 `LastStageTimeoutSeq` |
| `ClearRunCompletedDevicesAsync`（阶段推进） | 清计时，新阶段重新收集、重新达阈值再起表 |
| 批次 `finished`（全员终态） | 清计时 |
| `Mode` 2→1 / 1→2、`Generation++` | `ResetRackActivity()` 中一并清零 |
| 服务重启 | `StageDeadlineUtc` 在 Redis，重启后继续计时（不会静默丢失） |
| 并发 | 所有读写都在 `GetRackLock(agingNumber)` 内；后台轮询与 `TaskCP` 事件都走同一入口，无竞态 |
| **多实例部署** | ⚠ 现网假设单实例（`_states` 内存缓存 + 进程内锁）。若将来多实例，`_states` 缓存会与 Redis 不一致，需先改为每次读 Redis。本次**不处理**，仅记录 |

### 2.9 新增配置项

```json
"GroupControl": {
  "StageTimeoutEnabled": true,
  "StageTimeoutMinutes": 10,
  "HalfReleaseThresholdMode": "Ceil",
  "ExcludedRejoinMode": "None",
  "RaiseFaultOnStageTimeout": true
}
```

### 2.10 边界与风险

1. **阈值参与者数变化**：剔除后 participants 变少，下一阶段阈值按新基数算；若剩余 1 台，则它一完成就放行（不会再等 10 分钟，因 canRelease 优先）。
2. **completed 清空窗口**：放行瞬间 `ClearRunCompletedDevicesAsync` 与设备上报 `TaskCP` 存在竞态——某设备可能在清空后立刻上报下一阶段完成，导致下一阶段提前起表。属既有设计（放行即进入新阶段），可接受。
3. **离线设备被剔除**：设备掉线 10 分钟即被剔除，恢复后不再参与本批，其真实老化状态仍会推进，可能造成**平台记录与实际不一致**——这是本方案的核心业务取舍，请确认是否接受。
4. **故障记录风暴**：若某架持续异常，每阶段都会为同一批设备写 fault。建议（可选）同一 `(deviceId, runSeq, stage)` 幂等，只在首次剔除时写。
5. **`SaveEventAsync` 副作用**：见 §2.7，必须走新的不 MarkSeen 路径。
6. `StageTimeoutMinutes` 与后台轮询节拍无关，但精度受 1s 轮询限制（误差 ≤1s）。

---

## 3. 放行指令响应校验

### 3.1 现状与必须先确认的问题

现状：`TryPublishNextStepUnsafeAsync()` 只做 MQTT 发布，**不登记命令、不等回复、无超时**。现有回复链路（`device/{id}/cmd/reply/point` → `PointCommandReplyHandler` → `CommandRepository.CompleteAsync`）只服务于平台经 Stream 下发的命令。

**⚠ 阻塞点 — 报文格式**：现有解析器要求 `{"Seq":n,"Data":{"CmdSeq":..,"Code":..}}`，而你给的示例是**顶层** `{"CmdSeq":123213213,"Code":1,"Msg":null,"CmdData":null}`。若固件确实无 `Data` 包裹，现有 `CommandReplyHandlerBase` 会判定"缺少有效 Data.CmdSeq"并**丢弃**。需先确认固件真实报文，两种处理：
- (a) 固件改为 `Data` 包裹（推荐，与全协议一致）；
- (b) 代码兼容：解析时先取 `Data.CmdSeq`，取不到再回退顶层 `CmdSeq`（新增 `TryParseReply`，或扩展 `CommandReplyPayload`）。

另需确认：**Code 成功值是否恒为 1**（现有 `CompleteAsync` 用 `code == 1 → completed`）。

### 3.2 方案：复用现有命令基础设施（不新建等待框架）

关键可行性：命令状态键是 `lgw:device:{id}:cmd:{seq}`（**含 DeviceId**），等待器 key 也是 `{deviceId}:{cmdSeq}`。因此**同一放行 Seq 可为每台设备各建一条命令记录**，互不冲突，可复用 `WaitForCompletionAsync` 与四类回复处理器。

### 3.3 涉及文件与函数

| 文件 | 函数 | 改动 |
|---|---|---|
| `DeviceService/Services/GroupControlService.cs` | `TryPublishNextStepUnsafeAsync` | 发布前对每台 participant `EnsureStoredAsync`；发布成功后启动后台校验任务 |
| 同上 | 新增 `VerifyReleaseRepliesAsync(agingNumber, runSeq, seq, deviceIds, ct)` | 逐台等待回复、分类处理、重试、写故障 |
| 同上 | 新增 `_releaseVerificationStarted`（`ConcurrentDictionary`） | 防止重复启动校验（key = `{agingNumber}:{runSeq}:{generation}:{seq}`） |
| 同上 | 构造注入 | 增加 `CommandRepository`、已有 `DeviceRepository` |
| `Bluetti.Device.Redis/CommandRepository.cs` | `EnsureStoredAsync` | 已可用，无需改动（须保证 `Action` 合法：用 `can_next`，只允许字母数字下划线 ✓） |
| `DeviceService/Handlers/CommandResponseHandler.cs` | `HandleAsync` | 若走 §3.1(b)，增加顶层 `CmdSeq` 回退解析 |
| `Bluetti.Device.Redis/DeviceRepository.cs` | `SaveGroupControlFaultAsync` | 复用（§2.7） |

### 3.4 关键流程

```
TryPublishNextStepUnsafeAsync:
  1. 取 participants，对每台 EnsureStoredAsync(CmdInfo{Seq=pending.Seq, DeviceId, Action="can_next", ExpectedReplyType="point"})
  2. MQTT Publish server/rack/{n}/CanNextstep（原逻辑不变，失败仍保留 pending 重试）
  3. 成功后：清 completed / PendingNextStep / Generation++（原逻辑不变）
  4. 若 ReleaseReplyVerificationEnabled 且 _releaseVerificationStarted.TryAdd(key):
        _ = VerifyReleaseRepliesAsync(...)   // 后台任务，不阻塞轮询

VerifyReleaseRepliesAsync（每个设备并行等待，不持 rackLock）:
  res = await _commands.WaitForCompletionAsync(deviceId, seq, timeout, ct)
  ① res == null            → MarkTimedOutAsync  → 重试一次（原 Seq 重发）→ 仍无 → fault + 告警
  ② res.Code == 1 (completed) → 正常，无动作
  ③ res.Code != 1 (failed)    → 重试一次 → 仍失败 → fault + 告警（Message 含 Code/Msg）
```

- **重试**：沿用"原 Seq 重发"语义（`PublishCommandAsync` 同 Seq），设备端按 `CmdSeq` 幂等去重，安全。重试次数由配置控制，默认 1。
- **告警/故障**：与 §2.7 同构，写 `event:fault` + publish `lgw:event:fault`；`Message` 区分"放行指令超时未回复"与"放行指令返回失败 Code=xx"。
- **不回滚**：放行已完成、阶段已推进，校验只做留痕与重试，不改变放行结果（与 §2 的剔除逻辑解耦）。
- **超时口径**：独立于 `CommandProcessing:ReplyTimeoutSeconds`，新增 `GroupControl:ReleaseReplyTimeoutSeconds`（默认 15）。

### 3.5 新增配置项

```json
"CommandProcessing": {
  "ReplyTimeoutSeconds": 15,
  "PublishRetryDelayMilliseconds": 1000,
  "GroupControlReleaseReplyTimeoutSeconds": 15
},
"GroupControl": {
  "ReleaseReplyVerificationEnabled": true,
  "ReleaseReplyRetryCount": 1,
  "RaiseFaultOnReleaseReplyFailure": true
}
```

### 3.6 边界与风险

1. **报文格式**（§3.1）——未确认前不要实施，否则校验永远判定为失败/超时。
2. **Seq 复用冲突**：放行 Seq 与平台命令 Seq 共用 `lgw:cmd:seq` 自增，天然不冲突；但同一 Seq 会写入多个 `device:{id}:cmd:{seq}` 键，属预期（每台设备一条）。
3. **命令 Hash TTL**：统一 7 天（§1），复核 `RecordId` 回退链不受影响。
4. **后台任务生命周期**：校验任务是 fire-and-forget，需绑定 `CancellationToken`；服务停止时应等待或放弃（`_releaseVerificationStarted` 需清理，避免重启后键残留——建议键含 `Generation`，天然区分）。
5. **失败设备是否也剔除**：本项**只写故障、不剔除**；是否联动 §2 的 participants 剔除，取决于你希望的行为，可作为后续增强（建议先留痕观察）。
6. **校验期间设备已上报 agingIdle**：批次可能已结束，此时写 fault 仍有效（设备维度记录），但需注意 `SaveGroupControlFaultAsync` 不 MarkSeen（避免把离线设备复活）。
7. 每台设备一次 `EnsureStoredAsync` + 一次等待，participants 数量大时会有 N 个并发等待；当前规模（单架几十台）无压力。

---

## 4. 新增全局配置项汇总

| 配置键 | 默认 | 说明 |
|---|---|---|
| `RedisTtl.DefaultDays`（常量，非 appsettings） | 7 | 唯一 TTL 改动点 |
| `GroupControl:StageTimeoutEnabled` | true | 阶段超时总开关（可紧急关闭） |
| `GroupControl:StageTimeoutMinutes` | 10 | 半數达成后的超时时长 |
| `GroupControl:HalfReleaseThresholdMode` | Ceil | 半数取整规则 |
| `GroupControl:ExcludedRejoinMode` | None | 剔除设备是否回归 |
| `GroupControl:RaiseFaultOnStageTimeout` | true | 超时剔除是否写故障 |
| `GroupControl:ReleaseReplyVerificationEnabled` | true | 放行回复校验开关 |
| `CommandProcessing:GroupControlReleaseReplyTimeoutSeconds` | 15 | 放行回复等待超时 |
| `GroupControl:ReleaseReplyRetryCount` | 1 | 失败重试次数（原 Seq 重发） |
| `GroupControl:RaiseFaultOnReleaseReplyFailure` | true | 校验失败是否写故障 |

## 5. 建议实施顺序

1. §1 TTL 常量与 `RedisHelper` 签名（影响面最大、最独立）；
2. §2 阶段超时（状态字段 → 仓储方法 → 服务逻辑 → 配置）；
3. §3 放行校验（**依赖 §3.1 报文格式确认**）；
4. 每步结束 `dotnet build DeviceService.slnx` 验证 0 错误。

冒烟建议（无测试项目，只能手工）：单架 2 台 → 一台报 TaskCP、一台不报 → 观察 10 分钟后是否剔除 + 放行 + fault；放行后观察设备回复是否被正确匹配（`HGETALL lgw:device:{id}:cmd:{seq}` 状态应为 completed）。

## 6. 需你确认的问题（阻塞项已标红）

1. **【已确认 · 线路 A】放行回复采用现有协议**：主题 `device/{deviceId}/cmd/reply/point`，
   报文 `{"Seq":<消息序号>,"Data":{"CmdSeq":<放行指令Seq>,"Code":1,"Msg":null,"CmdData":null}}`。
   结论：**不需要改动 `CommandResponseHandler`**，现有 `PointCommandReplyHandler` 直接可接。
2. **【已确认】`Code = 1` 为成功**（设备收到放行指令后回复）；其它值视为失败。
3. 半数取整：确认 Ceil（n=5 → 3 台）还是 Floor（n=5 → 2 台）？**当前默认 Ceil**，配置项可改。
4. 被剔除设备在当前批次内是否确认不再回归（方案 A）？
5. TTL 统一默认 7 天是否可接受？其中 `runtime`/`event:state`/在线列表/架状态保持永久，是否认可？
6. 放行回复校验失败是否需要联动"从下一阶段 participants 剔除"？（当前计划：不联动，仅留痕）

---

## 7. 最终实施计划（线路 A 已确认，以此为准）

### 7.0 协议约定（线路 A，已确认，无需改现有 Handler）

```
下行：主题 server/rack/{AgingNumber}/CanNextstep
      报文 {"Seq":<放行Seq>,"Data":{"IsCanNext":1}}
上行：主题 device/{deviceId}/cmd/reply/point
      报文 {"Seq":<消息序号>,"Data":{"CmdSeq":<放行Seq>,"Code":1,"Msg":null,"CmdData":null}}
```

命中链路：`PointCommandReplyHandler` → `DeviceRepository.SaveCommandReplyAsync`
→ `CommandRepository.CompleteAsync(deviceId, CmdSeq, "point", Code, Msg, payload)`
→ 唤醒 `WaitForCompletionAsync` → `code == 1 ? completed : failed`。
**前提是发布前用 `EnsureStoredAsync` 建好 `lgw:device:{id}:cmd:{seq}`**，否则回复会被当成孤儿命令。

---

### 7.1 第 2 项：阶段超时与半数放行

**触发**：`Mode=2` 且有 `CurrentRunSeq`，本阶段 `completed` 数 ≥ `Ceil(participants.Count / 2)` 且未全覆盖 → 启动 10 分钟倒计时。
**到期**：剔除未完成设备 → 写故障 → 放行剩余。

| # | 文件 | 改动 |
|---|---|---|
| 1 | `Bluetti.Device.Redis/Models/GroupControlState.cs` | 新增 `StageDeadlineUtc`（`DateTime?`）、`LastStageTimeoutSeq`（`long?`） |
| 2 | `Bluetti.Device.Redis/RedisKeys.cs` | 新增 `GroupControlRunExcludedDevices(agingNumber, runSeq)` → `...:run:{runSeq}:excluded` |
| 3 | `Bluetti.Device.Redis/GroupControlRepository.cs` | 新增 `RemoveRunParticipantsAsync`、`AddRunExcludedDeviceAsync`、`GetRunExcludedDeviceIdsAsync`（复用 `RunTtl`） |
| 4 | `DeviceService/Services/GroupControlService.cs` | `EvaluateNextStepUnsafeAsync`：**先查超时 → 再判放行 → 最后起表**；新增 `ExpireStageTimeoutUnsafeAsync()`；`TryPublishNextStepUnsafeAsync` / `UpdateModeAsync` / `ResetRackActivity` 清计时 |
| 5 | `Bluetti.Device.Redis/DeviceRepository.cs` | 新增 `SaveGroupControlFaultAsync()`（落 `event:fault` 但**不调用 `MarkSeenAsync`**） |
| 6 | 同上 | 构造注入新增 `RedisHelper`（publish `lgw:event:fault`）、`DeviceRepository` 已有 |

**故障记录字段**（Hash `lgw:device:{id}:event:fault` + history Stream，与 `DeviceEventHandler` 同构）：
`DeviceId` / `EventType=fault` / `Seq=<放行Seq>` / `State=` / `Message="群控阶段等待超时，设备未上报阶段完成，已剔除出当前批次"` /
`Payload={"Seq":..,"Data":{"AgingNumber":"0","RunSeq":88,"Stage":"Discharge","Reason":"StageTimeout","TimeoutMinutes":10,"CompletedCount":3,"ParticipantCount":5,"Excluded":true}}`
写入时机：**与剔除同一把锁内、放行之前**；写入失败只记日志，不阻断放行。同步 publish `lgw:event:fault`。

**剔除后是否回归**：不回归（写 `excluded`，当前批次后续阶段不再等待；新 RunSeq 自动重新纳入）。
**计时器**：复用现有 1s 后台轮询，**不引入 Timer**；`StageDeadlineUtc` 存 Redis，重启继续计时；所有变更在 `rackLock` 内。

---

### 7.2 第 3 项：放行指令响应校验（线路 A）

| # | 文件 | 改动 |
|---|---|---|
| 1 | `DeviceService/Services/GroupControlService.cs` | 构造注入新增 `CommandRepository`；新增字段 `_serviceStoppingToken`、`_releaseVerifications`（`ConcurrentDictionary`） |
| 2 | 同上 `TryPublishNextStepUnsafeAsync` | 发布前对**每台 participant** 调 `EnsureStoredAsync`（`Action="can_next"`、`ExpectedReplyType="point"`、`Seq=pending.Seq`、`RecordId=null`）；发布成功后 `_ = VerifyReleaseRepliesAsync(...)`（不阻塞轮询） |
| 3 | 同上（新增） | `VerifyReleaseRepliesAsync(agingNumber, runSeq, seq, deviceIds, generation, ct)`：逐台 `WaitForCompletionAsync` → 分类 → 重试 → 写故障 |
| 4 | `Bluetti.Device.Redis/CommandRepository.cs` | 新增 `ResetForRetryAsync(deviceId, seq)`：强制把状态改回 `published` 并清空终态字段（见下方"坑"） |
| 5 | `Bluetti.Device.Redis/DeviceRepository.cs` | `SaveGroupControlFaultAsync()`（与 §7.1 共用） |

**为什么必须新增 `ResetForRetryAsync`（实现时最容易踩的坑）**：
`WaitForCompletionAsync` 开头若发现命令已处于终态（`completed/failed/timed_out`）会**立即返回**，不会等待；
而 `MarkPublishedAsync` / `MarkQueuedAsync` 都有终态保护（已是终态就直接 return，不会重置）。
→ 第一次等待超时后命令被置为 `timed_out`，若直接再调 `WaitForCompletionAsync` 会瞬间返回、重试形同虚设。
→ 因此重试前必须调用 `ResetForRetryAsync` 把状态强制改回 `published` 再等待。

**校验流程**
```
发布成功 → 清 completed / PendingNextStep / Generation++（现有逻辑不变）
        → 若 ReleaseReplyVerificationEnabled 且 _releaseVerifications.TryAdd("{n}:{runSeq}:{generation}:{seq}")
            → 后台：对每台设备 WaitForCompletionAsync(deviceId, seq, ReleaseReplyTimeoutSeconds=15)
                completed(Code=1) → 正常
                failed(Code≠1) 或 null(超时) → MarkTimedOut 记录 → 重发架广播一次（原 Seq）
                                              → ResetForRetryAsync → 再等一次
                                              → 仍失败/超时 → SaveGroupControlFaultAsync + publish 告警
```
- **重试是整架重发**（`CanNextstep` 是架广播），已确认设备会重复收到，**依赖设备按 `CmdSeq` 幂等去重**；
- **不回滚放行**，校验只留痕；
- 每台设备各自等待，用 `Task.WhenAll` 并发；
- 结果分类文案区分"未收到设备确认（超时）"与"设备返回失败 Code=xx"——注意这是**送达确认**，不是执行确认。

---

### 7.3 新增配置项（最终版）

```json
"CommandProcessing": {
  "ReplyTimeoutSeconds": 15,
  "PublishRetryDelayMilliseconds": 1000,
  "GroupControlReleaseReplyTimeoutSeconds": 15
},
"GroupControl": {
  "StageTimeoutEnabled": true,
  "StageTimeoutMinutes": 10,
  "HalfReleaseThresholdMode": "Ceil",
  "ExcludedRejoinMode": "None",
  "RaiseFaultOnStageTimeout": true,
  "ReleaseReplyVerificationEnabled": true,
  "ReleaseReplyRetryCount": 1,
  "RaiseFaultOnReleaseReplyFailure": true
}
```

### 7.4 实施顺序与验证

1. §7.1 阶段超时（状态字段 → 仓储 → 服务逻辑 → 配置）
2. §7.2 放行校验（`ResetForRetryAsync` → `EnsureStored` → 后台校验 → 故障记录）
3. 每步 `dotnet build DeviceService.slnx` 验证 0 错误

冒烟（无测试项目，只能手工）：
- 单架 2 台，只让 1 台报 `TaskCP` → 观察 10 分钟后是否剔除另一台 + 写 fault + 放行；
- 放行后 `HGETALL lgw:device:{id}:cmd:{放行Seq}` 应为 `completed` 且 `ResultCode=1`；
- 把某台断电模拟不回复 → 观察 15s 超时 → 重发一次 → 仍无 → fault 记录。

### 7.5 遗留待定项（**已全部裁决**）

| 待定项 | 裁决 |
|---|---|
| 半数取整 Ceil / Floor | **Ceil**（`(int)Math.Ceiling(count / 2.0)`；配置项 `HalfReleaseThresholdMode=Floor` 可切回） |
| `GroupControlRepository.RunTtl` 是否改引用 `RedisTtl` | **不用更改**，保持私有常量 7 天 |
| 校验失败是否联动"下一阶段剔除 participants" | **不用**，只留痕 + 告警，不影响放行判定 |

---

## 8. 实施记录（代码已落地）

### 8.1 数据层

| 文件 | 改动 |
|---|---|
| `Models/GroupControlState.cs` | + `StageDeadlineUtc`(`DateTime?`)、`LastStageTimeoutSeq`(`long?`) |
| `RedisKeys.cs` | + `GroupControlRunExcludedDevices(n, runSeq)` → `lgw:group-control:{n}:run:{runSeq}:excluded` |
| `GroupControlRepository.cs` | + `RemoveRunParticipantsAsync` / `AddRunExcludedDeviceAsync` / `GetRunExcludedDeviceIdsAsync` |
| `CommandRepository.cs` | + `ResetForRetryAsync(deviceId, cmdSeq)`：强制改回 `published` 并清空终态字段 |
| `DeviceRepository.cs` | + `SaveGroupControlFaultAsync(deviceId, messageSeq, message, rawPayload)`：**不调用 `MarkSeenAsync`** |

### 8.2 `DeviceService/Services/GroupControlService.cs`

| 位置 | 改动 |
|---|---|
| 字段/构造 | + `CommandRepository` / `RedisHelper` 注入；+ 8 个配置字段；+ `_releaseVerifications`、`_serviceToken`；+ `const ReleaseAction = "can_next"` |
| `ExecuteAsync` | 启动日志补充新开关；`_serviceToken = stoppingToken` 供后台校验任务使用 |
| `EvaluateNextStepUnsafeAsync` | 三段顺序固定：**先超时剔除 → 再判放行 → 最后按需起表** |
| `ExpireStageTimeoutUnsafeAsync` | 到期：算 `participants \ completed` → `SREM participants` + `SADD excluded` →（可选）写 fault + publish → 清 `StageDeadlineUtc`、写 `LastStageTimeoutSeq` |
| `ComputeHalfThreshold` | Ceil / Floor，默认 Ceil |
| `TryPublishNextStepUnsafeAsync` | 发布前 `StoreReleaseCommandsAsync`（每台 participant `EnsureStoredAsync`）；发布成功后 `StartReleaseVerification` |
| `StartReleaseVerification` | `TryAdd("{n}:{runSeq}:{generation}:{seq}")` 去重后 `Task.Run` 后台执行，结束移除去重键 |
| `VerifyReleaseRepliesAsync` | `Task.WhenAll` 并发等待 → `completed && ResultCode==1` 判成功；失败先 `MarkTimedOutAsync` → 整架重发（**原 Seq**）→ `ResetForRetryAsync` → 再等 → 仍失败写 fault + publish |
| `BuildGroupControlFaultPayload` / `PublishFaultAsync` | 故障载荷与告警格式（与 `DeviceEventHandler` 同构：`DeviceID/type/Message/ReceivedAtUtc`） |
| 清理点 | `HandleAgingStateAsync`（批次 finished）、`ResetRackActivity`（Mode 切换）、放行成功分支 均清 `StageDeadlineUtc` |

### 8.3 配置（`DeviceService/appsettings.json`）

已按 §7.3 原样落地 `CommandProcessing:GroupControlReleaseReplyTimeoutSeconds` 与 `GroupControl` 段。
`Program.cs` 无需改动：`RedisHelper` / `CommandRepository` / `DeviceRepository` 均已是 Singleton，构造注入可解析。

### 8.4 与计划的偏差（1 处，已对齐）


- 计划 §7.2 写"逐台等待"，实际实现改为 **`Task.WhenAll` 并发等待**（符合 §7.2 末尾"每台设备各自等待，用 `Task.WhenAll` 并发"）。单轮最长耗时 = `ReleaseReplyTimeoutSeconds`，与设备数无关。
- 故障 `Payload` 采用扁平 JSON（`AgingNumber/RunSeq/DeviceId/Seq/Reason/Message/OccurredAtUtc`），未嵌套进 `Data`，因为这是服务端自产故障而非设备上报报文。
- 剔除后设备**不会**被加回 participants；`excluded` 仅审计。

---

## 9. 异常设备兜底（终态 / 掉线 / 发送门）

§7 实施完成后复核发现：放行判定只看 `completed`，`terminal` 与在线状态都不参与，
因此**设备自报终态、报 fault、掉线三种情况都会永久阻塞整架放行**，且在"不足半数"时
连 10 分钟超时都起不了表，批次无兜底挂起。本节为已实施的补强。

### 9.1 终态 / 掉线立即移出 participants

| # | 文件 | 改动 |
|---|---|---|
| 1 | `Services/GroupControlService.cs` | 新增 `ExcludeParticipantUnsafeAsync(n, runSeq, deviceId, reason, message)`：`SREM participants` → `SADD excluded` → 按 `RaiseFaultOnExclusion` 写 fault + publish |
| 2 | 同上 | 新增 `FinishRunUnsafeAsync(n, state, runSeq, reason)`：批次置 finished、清 `CurrentRunSeq` / `PendingNextStep` / `StageDeadlineUtc` / `completed`；原 finished 分支改为复用它 |
| 3 | 同上 `HandleAgingStateAsync` | 终态分支在未全员终态时，除写 `terminal` 外**额外**把该设备移出 participants（仍在 terminal 集合中，批次结束判定不受影响）；移空则直接结束批次 |
| 4 | 同上 | 新增 `HandleDeviceOfflineAsync(deviceId)`：校验 type=1、Mode=2、在当前批次 participants 内 → 剔除（`reason=device_offline`）→ 移空则结束批次 |
| 5 | `Handlers/DeviceOfflineHandler.cs` | `SaveOfflineAsync` 后先调 `HandleDeviceOfflineAsync`，再 `ReevaluateMembershipAsync`（先剔除再重估，重估才会放行） |
| 6 | `Services/GroupControlService.cs` `EvaluateNextStepUnsafeAsync` | 兜底：`participants.Count == 0` 时直接 `FinishRunUnsafeAsync`。空集合下 `All()` 恒真但被 `Count > 0` 挡住，既不放行也不结束，会挂死 |

剔除原因取值：`aging_terminal`（自报终态）、`device_offline`（掉线）、`stage_timeout`（阶段超时，§7.1）。
被剔除设备**本批次不回归**，重新上线也要等下一个 RunSeq。

### 9.2 起表条件放宽

`StageTimeoutRequireHalf`（默认 **false**）：只要有任意一台完成就起表。
原实现要求 `completed >= Ceil(participants/2)`，多数设备异常时永远起不了表。
置 true 可恢复旧行为。

### 9.3 放行回复校验跳过离线设备

- 新增 `FilterOnlineParticipantsAsync(n, participants)`（用 `GetOnlinePointDeviceIdsAsync`）。
- `TryPublishNextStepUnsafeAsync`：只对在线设备 `StoreReleaseCommandsAsync` 与启动校验，差异记 Warning。
- `VerifyReleaseRepliesAsync`：**每轮开始前**重新过滤一次（等待期间可能掉线）；全部离线时以
  `clearedByOffline` 分支结束，不再误记"已收到全部确认"。

### 9.4 AgingCommandGate 离线兜底释放

- 新增配置 `AgingCommandGate:OfflineReleaseTimeoutMinutes`（默认 30，≤0 禁用）。
- `WaitAndReserveAsync` 改为 `Task.WhenAny(stateChangedTask, Task.Delay(timeout))`；
  超时且**设备已离线**才 `TryForceReleaseOfflineAsync` 强制开门；设备仍在线则继续等真实终态。
- 只在离线时释放的原因：老化可能持续数小时，按时间无条件释放会让新 `start_aging` 插队穿透。

### 9.5 配置项（本次新增）

```json
"GroupControl": {
  "StageTimeoutRequireHalf": false,
  "ExcludeOnAgingTerminal": true,
  "ExcludeOnDeviceOffline": true,
  "RaiseFaultOnExclusion": true
},
"AgingCommandGate": {
  "OfflineReleaseTimeoutMinutes": 30
}
```

### 9.6 验证

`dotnet build DeviceService.slnx --no-incremental` → **0 错误**，6 个警告均为改动前既有。

冒烟：
- 单架 3 台，只让 1 台报 TaskCP、1 台报 `agingStop` → 该台应立即从 participants 消失（不必等 10 分钟），
  剩余 2 台全部完成后放行；
- 让 1 台断电发遗嘱 → participants 立即减一 + `excluded` 记入 + `event:fault` 一条；
- 全部设备掉线 → 批次直接 finished，`CurrentRunSeq` 清空；
- 设备掉线后下 start_aging → 30 分钟后门被强制释放并打 Warning（设备在线则不会）。

### 9.7 仍未处理

- **总控（type=2）掉线**：`CPower` 仍会保留 pending 并每 1s 重试刷日志，直到该架重新有唯一在线总控。
  这是既有的"无总控/多总控保留 pending"设计，本次未改动。
- 剔除设备重新上线后本批次不回归：如需"回归"能力，需要新增 `ExcludedRejoinMode=Rejoin` 与重新入集的时机判定。
