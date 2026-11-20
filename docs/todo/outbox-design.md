# Outbox 设计：业务侧消息发件箱

> 状态：设计定稿（未实现，TODO）· 2026-08
> 术语：废弃 ack / record 命名，统一使用 **outbox**。ack 系统已整体移除。
> 本文是唯一的前向一致性设计文档。

## 1. 定位

- **store 核心零改动**：outbox 是业务侧模式。outbox 表 = 业务自己注册的一个普通 Kind，复制 / 快照 / 查询 / TTL 全走既有机制。
- store 只负责三件事：**WAL 持久化 + 复制 + snapshot**；下游一致性由业务侧基于 listener 钩子 + outbox 表自行构建。
- 可选的 `store/outbox` 辅助包（字段模板 + 扫描 helper，Kind 由业务注册、helper 按 Kind 参数化）——业务不用它也能自己搭。

## 2. 核心语义

### 2.1 两种关联，只保留一种

| 关联 | 是否需要 | 说明 |
|---|---|---|
| **事务关联**（outbox 行与业务事件同 Tx） | **必须** | outbox 的全部意义：状态变更 + 消息意图原子写，解决双写问题 |
| **定位关联**（relay 用 WAL 查行 / 重放时按行判重发） | **不需要** | 经典 outbox 的 relay 只扫表、不碰 WAL |

### 2.2 发布即删（经典 outbox 语义）

- relay 发布成功 → 删行。outbox 只解决"写路径可靠性"，不跟踪投递结果。
- 补偿模型：下游丢失 → WAL 重放（`OnWalReplayed`）重建消息 + **下游幂等**（消息键 = partition + offset 或业务键 + 版本）。
- 崩溃窗口：行已在表 → relay 续扫补发；已发布未删（发布后崩溃）→ 重发 → 下游幂等消化。

### 2.3 顺序契约（本方案的成立前提）

自增 id 是**分配序**，不是 WAL 提交序（并发提交的 Tx 两者可不一致）。本方案要求业务遵守：

- **同用户（同 kind,key）的写串行**（per-user lock，已是 store 的既有并发契约）——同用户消息的分配序 == 逻辑序 == WAL 序，immediate 路径（`OnWalSent`，WAL 序）与 relay 路径（id 序）**天然一致**；
- **跨用户顺序无要求**——两种路径对跨用户消息都不提供顺序保证（WAL 序本身对并发提交也是调度序）。

若未来某下游要求**严格 per-partition WAL 序（跨用户也要求）**，需改用 derived 字段方案（附录 A）。

## 3. 表结构（单表）

```go
// 业务侧注册（每服务一张）
type Outbox struct {
    Identity uint64 `store:"identity"`      // 路由 identity（查询结果不带 identity，删除要靠它）
    Target   string `store:"target,index"`  // 下游业务方（必须索引：查询 API 对未索引 hint 返回空结果）
    ID       int64  `store:"id,index"`      // 全局自增 id；对象 key = strconv(id)
    Payload  []byte `store:"type=custom"`   // JSON：构建消息所需的扩展（diff / 上下文 / 路由信息）
}
```

要点：

- **ID 由业务侧 `atomic.Int64` 分配**（Master 进程内全局唯一）。重启续接：restore + replay 完成后（replay 会把 WAL 里的 outbox 行连同 id 重放出来）查一次 `max(id)` 初始化计数器，再开始服务。id 间隙无害（cursor 扫描容忍）。
- key = `strconv(id)`：全局唯一 → 跨 partition / shard 无 key 碰撞（key 唯一性约束是 per-(shard, kind)）。
- **Payload 走 Custom 字段（JSON bytes）**：store 无 any/map 类型，Custom 是其原生泛化机制；代价是 payload 内部字段不可索引——需要按 payload 内容查询时走 filter（全表扫）或升级为分表强类型（见 §6）。
- struct 嵌入不支持（registry 只扫顶层字段）：多表复用字段靠复制模板，非嵌入。

## 4. relay 扫描模型

- **每 target 一个 cursor**：`QueryObjects(hints: [target == X, id > cursor], 批内按 id 升序排序)` → 构建消息 → 发布 → 删除。
- 两个 index hint 取交集（target 等值 + id 范围），均为索引字段；结果顺序由 API 不保证，**批内内存排序**。
- 删除：`BeginTx + DeleteObject(ev, row.Identity, kind, strconv(row.ID)) + Commit`（删除本身写 WAL，slave 同步）。
- 新 target 接入 = 新字符串值 + 新 cursor，**零注册成本**。

## 5. 写路径

```
业务 Tx：
  1. （已持有 per-user lock）
  2. id := outboxID.Add(1)
  3. SetObject(outboxKind, businessIdentity, strconv(id), Outbox{Identity, Target, ID: id, Payload})
     —— 与业务状态变更在同一个 Tx 内，同一 WAL 记录
```

## 6. 升级路径：分表（per-target Kind）

单表是默认形态。当某 target 需要：

- payload 强类型（字段级类型安全 + payload 内部字段可索引）
- 独立 schema 演进 / TTL / 保留策略

可为其单独注册 Kind。两形态不互斥：单表承载普通 target，强类型 target 独立建表。

## 7. TODO：store 原生自增 id 字段

当前系统**不支持通过 struct tag 定义自增 id 类型**，ID 由业务侧计数器承担（§3）。原生化需要解决两个问题，列为 TODO：

1. **WAL 确定性**：store 在 apply 时分配 id 的话，id 必须在 WAL payload 里可还原（slave replay 要得到与 master 相同的 id）——要么 apply 时回填 payload 并依赖 derived 字段机制，要么分配提前；
2. **重启续接**：序列要随 snapshot 持久化（或从表内 max 恢复）。

可行的落地顺序：先做通用 **derived 字段**（Def 声明字段来源 = `wal_seq` / `wal_offset` / `wal_partition`，apply 时填充，master 与 slave 路径一致、快照自然携带）——它是自增 id 的前提机制之一，也顺带覆盖附录 A 的严格顺序需求。

## 附录 A：derived 字段方案（严格 WAL 序的备选）

若顺序契约要求严格 per-partition WAL 序：

- Def 增加 derived 字段标注：`Seq int64` 字段来源 = WAL seq（或 offset），apply 时从 WAL meta 填充；
- 排序键 = (partition, seq)，relay 每 partition 一个 cursor；
- 填充序 == WAL 序，immediate 与 relay 两条路径对**任意消息**顺序一致；
- 代价：核心新增通用 derived 字段机制（几十行，非 outbox 专用）。
