# 架构探索笔记：写路径演进与一致性方案

> 状态：探索笔记 · 2026-08 · **均未实现**
> 本目录（`docs/todo/`）存放未来方向的探索记录与设计，当前以开源最小可用版本为先。

## 1. 当前模型的约束（讨论的起点）

- **WAL-first**（durable-before-apply）：先 appendWal（kafka acks=all）等 ack，再 apply 内存。
  - 换来的：单副本崩溃精确恢复（WAL 即真相），单一查询视图，无崩溃窗口
  - 代价：**时延地板 = kafka RTT**（同 AZ 1-3ms）；吞吐靠并发摊销 ACK 等待
- **写路径**：业务 goroutine 直接同步提交（无界并发），攒批依赖 franz-go 内部窗口。
  - bench（v3 mock，per-partition 管道模型）实测：并发分散到 16 partition 后每 partition
    攒批小（W16_Batch1ms ≈ W1_Batch1ms）——**批大小与每 partition 并发稀薄此消彼长**
- **顺序契约**：有序性只在单 partition 内保证；同 (kind,key) 并发写由业务串行化
  （per-user lock）。已知 tradeoff：同 batch 内并发提交的唤醒顺序由调度器决定，
  Master apply 序可能与 Slave replay 序不一致，收敛依赖业务契约。
- **下游一致性**：store 内无 ack / 水位 / 去重机制；listener 是裸通知钩子，
  补偿由业务侧构建（见 [outbox-design.md](outbox-design.md)）。

## 2. 探索过的路径

### 路径 A：现状不动（毫秒级够用的前提）

业务 per-user lock + 直接提交。优点：简单、已工作。缺点：批小、Master/Slave 收敛
依赖契约、时延地板 RTT。

### 路径 B：user-lock + 单写者 group commit（毫秒档的改进）

- 写路径改造：业务 goroutine（持 per-user lock）compute + build Tx → enqueue →
  per-partition 单写者攒批 appendWal → acks → apply → 唤醒（**锁跨 enqueue→apply**）
- 关键结论：**依赖链作用域 per-user（锁限制）、持久化链作用域 per-batch（跨用户批写），
  两者不重叠 ⇒ 单一视图共存，无悖论**；WAL-first 语义原封不动
- 收益：批由队列积压保证（与并发稀薄无关）；Master apply 序 == kafka 序 == Slave
  replay 序（结构性消除收敛问题）；compute 并行保留
- 代价：Commit 多一跳队列（µs 级）+ 背压策略 + 单写者故障模型

### 路径 C：锁自由消费者（LMAX 形态，亚毫秒档）

- 业务线程 publish → per-partition ring buffer → 单写者 consumer 批量 drain →
  串行 compute+apply → 批量 WAL → publish 结果
- 关键结论：**依赖链作用域全局（单写者串行一切）⇒ 与 WAL-first 批量持久化互斥**。
  要攒批只能**内存先行**（先逐条 apply 再批量发 WAL），而内存先行在单一查询视图下
  引入崩溃窗口（[最后 durable WAL, 内存最新] 区间丢失）。三选一：
  1. **dual view**（working / committed 双视图，见 §3）
  2. **同步 standby**（见 §4）
  3. 接受崩溃窗口（交易场景不可接受）
- 吞吐：每 partition = 1/compute_time（2-5µs → 200-500K ops/s），16 partition 3-8M
- 同账户一致性：单线程结构性保证，零锁（前提：per-session 顺序 publish）

### 路径 D：本地 journal（亚毫秒的姊妹路线）

commit gate 从 kafka 挪到本地 journal（group commit fsync，p50 50-200µs），
kafka 降级为异步复制流。工程要点：WAL Header 需携带 `LocalOffset`（双 offset 空间
解耦，kafka offset 降级为传输游标）；重启续传 = 读 kafka 尾部 LocalOffset+1。
与路径 C 的同步 standby 形态同源（LMAX 原教旨）。

## 3. Dual view 设计要点（路径 C 的选择 1）

- **overlay（写集）**：批/Tx 内维护 `key → working 对象`。读 = 两级查找：
  overlay → shard（read-your-own-writes，同批内 e2 读到 e1 的 working 版本）
- **COW**：首次触碰拷贝对象；commit 时整体 swap 进 shard（复用现有 RWMutex，
  只在 swap 瞬间持写锁，compute 全程无锁）；外部读者走原读 API 永不见 working 版本
- **API 形态**：复用现有 Tx（一个 Tx 本就是 N 个 event 一条 WAL 记录 = 批的天然容器），
  新增 Tx 级读方法 `tx.GetObject` / `tx.GetKV`（overlay 感知）；外部读 API 不动
- **delete 需 tombstone**：overlay 存 `(ptr, deleted)`，批内删掉的 key 读返回不存在
- **v1 限制**：批内只支持按 key 读；索引查询在 overlay 下困难（索引树无 working 版本），
  批内禁止（交易 compute 几乎全是 key 查询，影响可接受）
- **失败模型**：WAL 失败 → panic → replay 重建（不需要 undo log——replay 就是 undo）
- **这是退化版 MVCC**：每 key 最多两版本、单写者、单一水位、无快照隔离、无 VACUUM
  （对照：PG xmin/xmax + 快照可见性；InnoDB 就地更新 + undo 链）

## 4. 同步 standby 要点（路径 C 的选择 2）

- 同步范围 = **同机架不同物理机**（RTT 预算 ~50-200µs），防单机故障（最常见）
- 机房级故障同步层防不住 ⇒ 分层：同步 in-AZ + 异步跨 AZ（kafka 现位）+ journal 落盘
- 节点数权衡：1 standby 够防单机故障；裸奔窗口（failover 后重建前）+ standby 自身
  挂时写入阻塞（2/2 全确认）→ 2 standby 走 2/3 quorum（Raft）任一挂不阻塞。
  多数交易所撮合：1 sync + 1 async 跨 AZ（连续双机故障概率低于整 AZ 故障）

## 5. 其他关键结论

- **多 partition 的收益维度**：单 partition kafka 容量上限（fsync/网络）、slave replay
  并行、下游并行、锁争用缓解。partition 数决策依据 = 单 partition 容量 × 需求，与
  并发数/批大小无关（直接提交模型下批增益与 partition 数此消彼长）
- **at-least-once 是定理底线**：无下游合作无法 exactly-once；消息键 =
  partition + offset（或业务键 + 版本）+ 下游幂等
- **durable-before-apply 的本质**：把 WAL 时延并入了计算依赖链。依赖链全局（单写者）
  时与批量持久化互斥；依赖链 per-user（锁）时共存
- **outbox / 一致性补偿**：与以上写路径正交——store 内无状态，业务侧表 +
  listener/replay 构建（[outbox-design.md](outbox-design.md)）

## 6. TODO 清单

- [ ] 路径 B：单写者 group commit（毫秒档，改动中等，收益确定）
- [ ] 路径 C：dual view（Tx overlay + Tx 读 API + COW swap，亚毫秒档前提之一）
- [ ] 路径 D：本地 journal + Header.LocalOffset（亚毫秒档，commit gate 下移）
- [ ] 同步 standby 协议（若走路径 C/D）
- [ ] store 原生自增 id 字段（struct tag；outbox 的前置 TODO，见 outbox-design §7）
- [ ] outbox 实现（业务侧或 store/outbox 辅助包）
