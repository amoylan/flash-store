# flash-store 架构文档

> 最后更新：2026-08-27

flash-store 是一个基于 WAL（Write-Ahead Log）的 Go 内存状态存储库，设计面向交易等状态型服务。项目当前处于实验性 v0 阶段，以下内容描述设计目标和实现机制，不构成生产可用性或性能承诺。核心设计目标：

1. **WAL 驱动的主从复制**：Master 写 WAL，Slave 通过 replay WAL 重建状态
2. **Snapshot 持久化**：SnapshotSlave 定期创建全量快照，避免 WAL 无限增长
3. **Listener 通知**：通过 WalSent / WalReplayed 钩子把事件流暴露给业务侧，下游一致性补偿由业务侧自行构建
4. **角色解耦**：Master / SnapshotSlave / ReadOnlySlave 各自职责清晰，依赖最小化

本文按"自身数据存储 → 主从同步与通知 → 角色特殊性"的脉络循序渐进地介绍各组件，建议按顺序阅读。

## 目录

- [1. 整体架构](#1-整体架构)
- [2. 自身数据存储](#2-自身数据存储)
  - [2.1 Shard — 内存分片与读路径](#21-shard--内存分片与读路径)
  - [2.2 TTL TimeWheel — 过期数据清理](#22-ttl-timewheel--过期数据清理)
  - [2.3 Worker — 写路径与操作串行化](#23-worker--写路径与操作串行化)
  - [2.4 Tx / TxEvent — 事务入口](#24-tx--txevent--事务入口)
- [3. WAL 与 Snapshot — 持久化与主从同步](#3-wal-与-snapshot--持久化与主从同步)
  - [3.1 WAL 子系统](#31-wal-子系统)
  - [3.2 Replayer — 回放与追主](#32-replayer--回放与追主)
  - [3.3 Snapshot 子系统 — WAL 的 Compaction](#33-snapshot-子系统--wal-的-compaction)
- [4. Listener 与下游通知](#4-listener-与下游通知)
- [5. Replay 正确性保证](#5-replay-正确性保证)
  - [5.1 Snapshot 安全边界 — 快照任意时刻安全](#51-snapshot-安全边界--快照任意时刻安全)
  - [5.2 Listener 语义 — 裸通知钩子](#52-listener-语义--裸通知钩子)
- [6. SnapshotSlave 角色特点 与 schema 广播](#6-snapshotslave-角色特点-与-schema-广播)
- [7. 角色对比](#7-角色对比)
- [8. 已移除的机制（2026-08）](#8-已移除的机制2026-08)
- [9. 典型数据流](#9-典型数据流)
- [10. 配置参考](#10-配置参考)

---

## 1. 整体架构

```
                           ┌─────────────────────────────────────────────────┐
                           │                    Master                       │
                           │  ┌─────┐   ┌────────────┐   ┌───────────────┐   │
  Tx (业务写入) ──────────► │  │ Tx  │──►│ Worker×16  │──►│ WAL Writer    │   │
                           │  └─────┘   │(per-partition) │ (Kafka/File)  │   │
                           │            └────────────┘   └───────┬───────┘   │
                           │                                    │           │
                           │  Listener: WalSentListener (commit)│           │
                           │            WalReplayedListener (replay)        │
                           │  PublishSchemas (WalTypeSchemaConfig)          │
                           │                                    │           │
                           └────────────────────────────────────┼───────────┘
                                                                │
                                             WAL 远端存储 (Kafka)
                                                                │
                    ┌───────────────────────────────────────────┼─────────────────┐
                    │                                           │                 │
          ┌─────────▼──────────┐                     ┌─────────▼──────────┐       │
          │  SnapshotSlave     │                     │  ReadOnlySlave     │       │
          │  (pack-server)     │                     │  (query-server)    │       │
          │                    │                     │                    │       │
          │  ReplayForever     │                     │  ReplayForever     │       │
          │  + direct apply    │                     │  + ApplyToStore    │       │
          │  + no business Def │                     │  (Row + indexes    │       │
          │  + CreateSnapshot  │                     │   + business Def)  │       │
          │                    │                     │  + Read API        │       │
          └────────────────────┘                     └────────────────────┘       │
                    │                                                             │
                    ▼                                                             │
          Snapshot 存储 (S3/File) ◄────────────── Restore ─────────────────────────┘
```

Store 顶层是一个简单的容器，持有 `shards[64]` 和 `workers[16]`。路由规则：

- `identity % 64` → shard（决定数据落在哪个内存分片）
- `identity % 16` → worker（决定 WAL 落在哪个 Kafka partition）

一个 worker 可对应多个 shard，但一个 shard 只对应一个 worker。

---

## 2. 自身数据存储

先从最基础的 "Store 自己如何存数据" 讲起。这一层不涉及下游、不涉及主从——就是一个带 WAL 的内存 KV + 对象存储。

### 2.1 Shard — 内存分片与读路径

Shard 是内存中的数据分片。每个 Shard 存储：

- `tables map[Kind]*table`：按业务类型分表
  - 每个 table 存储 `objects map[string]object` + 多个 B-Tree `indexes`
  - `schema` 定义了哪些字段需要建索引
- `kv map[string]kValue`：简单 KV 存储

**两种存储模式**（由 shard.noBusinessDef 标志区分）：
- **常规模式**（Master / ReadOnlySlave）：注册了业务 Def[T]，按 schema 建索引，读 API 可回填 `*T`
- **无业务 Def 模式**（SnapshotSlave）：不 import 业务包、不注册 Def，不建索引、不做 Def-based 类型校验

注意：两种模式**都存解码后的 `schema.Row` 结构体**——Row 自描述，解码不需要业务 codec。"无业务 Def 模式"并不是"raw bytes passthrough"——WAL → shard → snapshot 路径上 Row 仍然完整解码/再编码，只是 shard 不建索引、读 API 上不支持 `*T` 回填（只能走 RowView，但现在 SnapshotSlave 没暴露读 API）。

**读 API 两大家族**：

- **Object 家族** — `GetObject` / `GetObjects` / `QueryObjectsByIdentity` / `QueryObjects`：返回业务结构体。读对象时会 **Clone**，避免调用方修改存储中的对象。
- **View 家族** — `GetView` / `GetViews` / `QueryViewsByIdentity` / `QueryViews`：返回 `registry.View`（按字段只读访问），**避免整对象 Clone**，适合只关心部分字段的场景，性能更好。

此外还有简单 KV：`GetKV` / `CheckKVExists`。

查询可以带 `IndexHint` + `Filter` 做范围过滤。

### 2.2 TTL TimeWheel — 过期数据清理

Object 和 KV 都可设置 TTL。过期清理由本地 `TimeWheel` 完成，**不写 WAL**（主从各自根据 WAL 里的 `Ttl` 字段各自注册时间轮任务）：

- 每圈 1 小时（3600 个 1 秒 slot），更长 TTL 走 rounds
- 安全校验：删除前确认当前 `expireAt` 与注册时一致，避免误删被更新过 TTL 的条目

把 TTL 放在 shard 层而不是单独服务，是因为过期数据的淘汰本身就是数据管理的一部分，逻辑上和 shard 内的 `objects` / `kv` 紧耦合。

### 2.3 Worker — 写路径与操作串行化

Worker 是"写路径"的载体。每个 worker 对应一个 WAL 分区（Kafka partition），全局固定 16 个。

**Worker 存在的根本原因**：**同一用户（identity）的操作必须先后有序**，否则下游和 Slave 重建的状态会出错。通过 `identity % 16` 把同一用户的所有写入路由到同一个 worker（同一个 Kafka partition），就能保证有序性。

**Worker 核心状态**：
- `seq`：WAL 业务序列号，单调递增，用于幂等去重（配合 `SeqDeduplicator`）
- `maxWalOffset`：已处理的最大 WAL offset
- `walWriter`：本分区的 WAL 写入器（Kafka / File / Noop）

**写流程**（`worker.Commit`）：
1. `appendWal` — 编码 + Writer.Append + 指数退避重试
2. `applyEvents` — 应用到本地 shard
3. 通知 `WalSentListener`

**写 WAL 的重试策略**：同一个 seq 无限重试（直到成功或 ctx 取消），配合接收端 `SeqDeduplicator` 幂等去重。这样即便 Writer.Append 返回错误但消息实际已写入 Kafka，也不会在 replay 时产生重复数据。

### 2.4 Tx / TxEvent — 事务入口

业务写入入口，仅 Master 使用。

- `Tx` 包含多个 `TxEvent`，每个 Event 包含多个 `Cmd` / `KvCmd`
- 所有 Event 必须落在同一 worker（即 `identity % 16` 相同），由 `pinWorker` 保证
- `Commit` 时一次性将所有 Event 编码为一条 WAL，写入 → apply 到 shard
- 各 Event 可有不同 identity（即不同 shard，但同一 worker）

```go
tx := store.BeginTx(ctx)
ev := tx.BeginEvent("OrderPlaced", bizData)
store.SetObject(ev, identity, codecID, key, obj, ttl)
store.SetKV(ev, identity, kvKey, value, ttl)
tx.Commit()
```

---

## 3. WAL 与 Snapshot — 持久化与主从同步

Worker 写入产生的 WAL 是整个系统的核心。它既是持久化机制，也是主从同步的数据通道。

### 3.1 WAL 子系统

```
写路径: Tx.Commit → worker.activeUpdate → appendWal → Writer.Append → Kafka
读路径: Reader.Seek → Reader.Next → walReplayer.Decode → replayApplier.Apply → store
```

**WAL 类型**（由 Header 中的 WalType 决定）：

| WalType | 用途 | 写入者 |
|---------|------|--------|
| `WalTypeBusiness` | 业务数据变更（Events） | Master 业务写入 |
| `WalTypeSchemaConfig` | schema 全量广播（store 级配置，仅 partition 0 单通道） | Master 启动后 `PublishSchemas` |

该广播类型的用途在 [第 6 章](#6-snapshotslave-角色特点-与-schema-广播) 详细展开。

> `WalType` 1/2/3 保留为 **reserved**：曾分别用于 ack 批量确认（AckEvents）、AckTargets 名称广播、SnapshotAckTargets 全局配置同步，机制均已移除（2026-08）。**禁止复用**——历史 WAL 里可能仍有该值，复用会造成旧数据被新语义误解析。

**存储后端**：通过 Source 抽象，支持 Kafka / File / Noop 三种实现。
**编解码**：WAL 和 Snapshot 均支持 JSON / Proto 两种格式。

### 3.2 Replayer — 回放与追主

每个 worker 对应一个 replayer，从 WAL 远端读取记录并 apply。

**Replay 模式**：
- `ReplayToLatest`：追到 WAL 末尾即退出（Master 启动恢复用）
- `ReplayForever`：持续跟随直到 ctx 取消（Slave 持续同步用）

**Apply 链**（由内到外）：
1. `applyEvents` → shard（若 `ApplyToStore == true`）
2. Listener（若配置）：触发 `OnWalReplayed`
3. Controller（若配置）：包装最外层（如加锁、暂停）

### 3.3 Snapshot 子系统 — WAL 的 Compaction

WAL 会无限增长。Snapshot 扮演 "Compaction" 的角色：把当前状态完整快照一次，下次启动就可以从 snapshot 恢复，只需要 replay snapshot 之后的 WAL。

**创建**（仅 SnapshotSlave）：
1. 前置检查：replay 已停止
2. 写 Header（JSON）：包含 format、shard/worker 数量、schemas
3. 并发写 Shard segments + Worker segments
4. 原子提交（`Finish`）
5. 更新 `lastSnapshotWalOffsets`

**恢复**（`RestoreAndStartReplay`，Master 和 Slave 均可）：
1. 读 Header，选择 codec
2. 并发恢复 Shard（objects + KV + TTL）和 Worker（seq、SeqDedup）
3. 恢复 `lastSnapshotWalOffsets`（用于可观测性指标）
4. 基于各 worker 的 `WalOffset + 1` 作为 replay 起点
5. 如果 snapshot 不可用（`ErrNoSnapshot`），静默降级为从头 replay

**存储后端**：S3 / File / Noop。

---

## 4. Listener 与下游通知

到这里，Store 已经能自己把数据存好、持久化到 WAL、通过 replay 主从同步。交易系统中 Store 还承担另一个职责：**把业务事件通知给下游系统（如撮合、归档、推送）**。该职责通过两个 Listener 接口以"裸通知钩子"形式暴露，一致性补偿由业务侧自行构建。

| 接口 | 触发时机 | 用途 |
|------|----------|------|
| `WalSentListener.OnWalSent` | Master `worker.Commit` 成功后 | 正向写入通知（如构造并推送撮合消息） |
| `WalReplayedListener.OnWalReplayed` | Replay 期间每条业务 WAL apply 成功后 | 重建/补发消息（如 Push 重启后重发遗漏消息） |

两者都是**裸通知钩子**：`WalRecord` 只含 `Partition / Events / Offset`——没有 ack 相关字段，也不做任何去重 / 补发判定。每条业务 WAL 恰好触发一次（seq 层面的重复由 `SeqDeduplicator` 去重）。业务侧自行决定是否重复发送、如何补偿，常见基线是消息键 = partition + offset 的下游幂等；更完整的 outbox 设计见 [todo/outbox-design.md](todo/outbox-design.md)（TODO）。

> 2026-08 之前曾存在 AckTarget / AckManager / AckBatcher / AckRouting / claim 状态机等一套完整的 ack 机制（见 [第 8 章](#8-已移除的机制2026-08)），现已整体移除。

---

## 5. Replay 正确性保证

### 5.1 Snapshot 安全边界 — 快照任意时刻安全

早期设计用 **AckGate + SnapshotAckTargets** 门控 SnapshotSlave 的 apply：业务 WAL 先 Buffer，等所有 `snapshotAckTargets` 都 ack 到该 offset 后才 flush 进 store，保证进入 snapshot 的数据都是 "ack 完备" 的。该机制已于 2026-08 移除（连同整个 ack 系统）。

现在的语义：SnapshotSlave 的 replay **直接 apply 业务 WAL**，快照**任意时刻安全**——snapshot 可能包含"尚未被下游消费"的数据，但这不影响正确性：snapshot 只用于状态重建（Header + Shards + Workers），恢复后从 `WalOffset + 1` 继续 replay，与下游是否已消费无关。下游补偿由业务侧基于 listener 自行构建（见 [§4](#4-listener-与下游通知)），store 内没有任何 ack 状态需要与 snapshot 对齐。

### 5.2 Listener 语义 — 裸通知钩子

Master / Slave 的 replay 对每条业务 WAL apply 成功后触发一次 `OnWalReplayed`，**无去重、无水位、无补发判定**。因此"哪些消息需要补发"完全由业务侧判定：例如在独立 replay（`ReplayForever` + Listener + `ApplyToStore: false`）里自行维护已消费断点、跳过已处理的消息；跨进程重启的重复投递靠下游幂等消化。

---

## 6. SnapshotSlave 角色特点 与 schema 广播

### 6.1 SnapshotSlave 角色特点

希望有一个专门的 **SnapshotSlave** 角色（部署在独立的 pack-server）持续运行、定期打 snapshot。这个角色的关键特性：

1. **不依赖 master 所在业务 package** — 不能 import 业务代码、不注册 `Def[T]`
2. **持续运行不用重启** — Master 重启时 SnapshotSlave 不能跟着重启
3. **能应对业务侧变化** — Master 升级后可能新增 / 删除 Event 类型、调整 schema

SnapshotSlave 不注册业务 schema，但它打 snapshot 时要把各 Kind 的 schema 描述写进 `Header.Schemas`（恢复时用于 compat 校验、loud-fail 拒绝不兼容演进）。于是 WAL 引入 **schema 广播**，让 SnapshotSlave 在运行过程中能"听到" Master 当前的 schema：

### 6.2 Schema 广播 WAL

| WalType | 广播内容 | 触发时机 | 写入 worker |
|---------|----------|----------|-------------|
| `WalTypeSchemaConfig` | 当前注册的全部 Kind schema（整集合替换） | Master 每次 `afterReplayStarted`（`PublishSchemas`） | **仅 partition 0**（store 级配置，单通道传递） |

Slave replay 到该 WAL 时 `UpsertSchema` 更新本地缓存；Master 写完 WAL 后自己也 Upsert（把进程内缓存同步到广播内容）。恢复时 `restoreFromSnapshot` 用 `header.Schemas` 逐条加载历史 schema。

---

## 7. 角色对比

三个角色的职责和依赖关系总结：

| 维度 | RoleMaster | RoleSnapshotSlave | RoleReadOnlySlave |
|------|-----------|-------------------|-------------------|
| 部署位置 | 业务进程内嵌（如撮合） | 独立 pack-server | 独立 query-server |
| 写 WAL | ✅ 唯一写入端 | ❌ | ❌ |
| 读 API | ✅（业务读本地内存即走读 API） | ❌ | ✅ |
| 数据存储 | 解码 Row + 建索引 | 解码 Row、不建索引 | 解码 Row + 建索引 |
| 注册业务 Def[T] | ✅ | ❌（`noBusinessDef=true`） | ✅ |
| 创建 Snapshot | ❌ | ✅ | ❌ |
| schema 广播 | ✅（`PublishSchemas`） | ❌ | ❌ |
| Replay 模式 | ReplayToLatest | ReplayForever | ReplayForever |
| Listener | ✅（WalSent + WalReplayed 均可） | ✅（可选） | ✅（可选） |

---

## 8. 已移除的机制（2026-08）

ack 系统已于 2026-08 整体移除。曾经的组件与职责：

| 已移除 | 说明 |
|--------|------|
| AckTarget / AckManager / AckState | 下游抽象与水位追踪 |
| AckBatcher + ack-only WAL | 批量写 ack WAL（`WalType 1` reserved） |
| AckRouting + fillAckTargets + `Event.AckTargets` | 写 WAL 时为事件标注下游 |
| AckTargets 广播（`WalType 2` reserved） | 向 Slave 同步全量 target 集合 |
| autoTrivialAck / AckAlignTargets / allAckTargets | replay 时推进无关 target 水位 |
| claim 状态机（TryClaim / ReleaseClaim / ClaimTTL） | callback 与 replay 的发送权互斥 |
| AckStates / AllAckTargets 快照段 | WorkerSnapshot 中的 ack 持久化 |
| OffsetDeterminingAckTargets | secondary replay 起始断点 |
| ResendGate | Master replay 补发去重 |
| AckBatchConfig / ack·claim 指标 | 配置与可观测性 |

未来的 outbox 设计见 [todo/outbox-design.md](todo/outbox-design.md)（TODO）。

---

## 9. 典型数据流

### 9.1 正向写入（Master）

```
BeginTx → BeginEvent → SetObject/SetKV/Delete → Commit
  → pinWorker (identity % 16)
  → appendWal (encode + Writer.Append + 指数退避重试)
  → applyEvents (路由到 shard)
  → OnWalSent (通知 listener)
```

### 9.2 Master 启动恢复

```
RestoreAndStartReplay
  → restoreFromSnapshot (Header + Shards + Workers 并发)
  → startReplay (per-worker goroutine, 起点 = WalOffset + 1)
  → WaitReplay (ReplayToLatest 追完)
  → PublishSchemas
  → 业务就绪
（业务侧如需重放补偿，自行再启动 ReplayForever + Listener 的 replay，见 §4）
```

### 9.3 SnapshotSlave 周期工作

```
loop:
  StartReplay (ReplayForever, ApplyToStore)
    → 业务 WAL 直接 apply（快照任意时刻安全）
  ... 运行一段时间 ...
  StopReplay
  CreateSnapshot
    → 写 Header + Shards + Workers (并发)
    → 更新 lastSnapshotWalOffsets
  重新 StartReplay
```

---

## 10. 配置参考

### Store Config

| 字段 | 说明 |
|------|------|
| `Role` | `master` / `snapshot_slave` / `readonly_slave` |
| `Wal.Writer` | WAL 写入配置（Source: kafka/file/noop, Format: json/proto） |
| `Wal.Reader` | WAL 读取配置 |
| `Snapshot` | Snapshot 读写配置（Source: s3/file/noop, Format: json/proto） |

### 常量

| 名称 | 值 | 说明 |
|------|-----|------|
| `numShards` | 64 | 内存分片数，`identity % 64` 路由 |
| `numWorkers` | 16 | WAL 分区数，`identity % 16` 路由到 worker/Kafka partition |
