# flash-store 指标手册

> 状态：v1 · 2026-05-08
> 适用：`store/metrics/` 包及所有内置埋点位点

flash-store 是一个 library。指标体系由 [store/metrics](../store/metrics) 包统一管理，业务侧在 `NewStore(cfg, reg)` 第二参传入 `prometheus.Registerer` 后自动启用。本手册按模块组织 in-tree 全部 metric，作为 PromQL / Grafana 编写时的查询字典与外部接口承诺。

---

## 1. 设计原则

1. **Registerer 走 NewStore 参数**，不进 `Config`。`Config` 字段全部需由 Apollo 之类的配置中心下发，`prometheus.Registerer` 是运行期对象不在此列。`nil` 时 store 内部回退到 throwaway registry，库内仍照常 observe，热路径无判空。
2. **service / role 等进程级常量标签由业务侧 wrap 注入**：
   ```go
   reg := prometheus.WrapRegistererWith(
       prometheus.Labels{"service": "trade-service", "role": "master"},
       prometheus.DefaultRegisterer,
   )
   s, _ := store.NewStore(cfg, reg)
   ```
   flash-store 不在签名上接收这些维度——一次 wrap 全 metric 自动带上。
3. **标签基数受控**：所有 label 取值在编译期可枚举（`worker_id` ≤ numWorkers，`wal_type` 2 个，`op` 10 个，余类同）。**严禁**把 order_id / user_id / kind 名等业务主键写进 label。
4. **直方图同时启用 native histogram + classic buckets**。Prometheus ≥ 2.40 自动用 native（稀疏、自适应、无需选桶），老版本回落 classic。
5. **耗时单位统一 `_seconds`**——prometheus base unit 约定。Grafana panel 设 `unit=seconds` 后自动按数量级渲染（s / ms / µs）。`time.Duration.Seconds()` 零分配，精度无损。
6. **状态量走 `GaugeFunc`**——scrape 时回调读取活态，不在 worker.Commit 等热路径上 Inc/Dec/Set。
7. **稳定 label 值是外部接口**——`wal_type=business`、`result=ok` 之类的字符串是 PromQL 查询契约，**禁止改名**。新增是允许的、删除等同破坏，详见 §11。

---

## 2. 集成方式

### 业务侧最小集成

```go
reg := prometheus.DefaultRegisterer  // 或者 prometheus.NewRegistry()
s, err := store.NewStore(cfg, reg)
```

`/metrics` 端点用 promhttp 暴露即可：

```go
http.Handle("/metrics", promhttp.Handler())
```

### 多 Store 共享 Registerer

同进程多 Store（极少见，比如双角色测试 binary）必须用 `WrapRegistererWith` 加 const label 区分，否则会因指标重复注册报错：

```go
masterReg := prometheus.WrapRegistererWith(prometheus.Labels{"store": "master"}, reg)
slaveReg  := prometheus.WrapRegistererWith(prometheus.Labels{"store": "slave"}, reg)
```

### 子系统的 label 注入

`store/wal/source/kafka` 的 collector 不自带 `worker_id`，由 store/worker 端用 `metrics.WorkerLabels(id)` wrap：

```go
wrapped := prometheus.WrapRegistererWith(metrics.WorkerLabels(partition), reg)
kafkaWriter.RegisterMetrics(wrapped)
```

新增子系统遵循同样模式：子系统注册指标时**不**自带 per-worker label，由调用方 wrap 注入——保持 label 命名统一。

---

## 3. 命名与 label 约定

### 名字前缀

所有指标以 `flashstore_` 开头。子系统次级前缀：

| 前缀 | 范围 |
|------|------|
| `flashstore_wal_*` | WAL 写入（store 视角） |
| `flashstore_commit_*` / `flashstore_apply_*` / `flashstore_tx_*` | 业务 Commit / Apply / Tx 计数 |
| `flashstore_read_*` | Read API |
| `flashstore_listener_*` | Listener 回调耗时 |
| `flashstore_snapshot_*` | Snapshot 创建 / 恢复 |
| `flashstore_replay_*` | Replay 进度 / 状态 |
| `flashstore_last_snapshot_*` | Snapshot 历史 |

### 标准 label 名

集中在 [store/metrics/labels.go](../store/metrics/labels.go)，**禁止**在业务代码里写裸字符串。Counter / Histogram Vec 的标签命名通过 `metrics.label*` 常量保证一致。

| label | 含义 | 典型取值 |
|-------|------|----------|
| `worker_id` | WAL 分区编号（partition == worker_id） | `"0"..."15"` |
| `wal_type` | WAL 类型 | `business` / `schema_config` |
| `kind` | WAL 写错误分类 | `encode` / `transient` / `ctx_canceled` / `seq_lag_giveup` |
| `result` | 操作结果 | `ok` / `error`（snapshot restore 额外有 `no_snapshot`） |
| `phase` | Snapshot 创建阶段 | `header` / `shards_workers` / `finish` / `total` |
| `op` | Read API 方法名 | `get_object` / `get_objects` / `get_view` / `get_views` / `query_objects` / `query_objects_by_identity` / `query_views` / `query_views_by_identity` / `get_kv` / `check_kv_exists` |
| `path` | Listener 回调路径 | `wal_sent` / `wal_replayed` |

### Curry / 类型化方法

per-worker 热点指标在 `worker` 构造时通过 `metrics.Metrics.ForWorker(id)` 一次性 curry 为 `*metrics.WorkerMetrics`，挂在 `worker.metrics` 上。热路径直接 `w.metrics.ObserveCommit(d)`，零 map 查找。call site 只用类型化方法 + 类型化常量（`metrics.WALErrTransient` 等），写错 label 值 = 编译失败。

---

## 4. WAL 写入

来源：[store/worker.go](../store/worker.go) `appendWal`。

| 名称 | 类型 | label | 何时 |
|------|------|-------|------|
| `flashstore_wal_write_duration_seconds` | Histogram | `worker_id`, `wal_type` | `appendWal` 端到端耗时（含 Kafka ACK 等待 + 重试 backoff） |
| `flashstore_wal_write_bytes_total` | Counter | `worker_id`, `wal_type` | 单次 append 编码后字节，仅最终成功才累加 |
| `flashstore_wal_seq_total` | Counter | `worker_id`, `wal_type` | append 成功一次 +1 |
| `flashstore_wal_write_errors_total` | Counter | `worker_id`, `kind` | encode 失败 / 重试期瞬时错误 / ctx 取消 / seq lag 放弃 |
| `flashstore_wal_write_retries_total` | Counter | `worker_id` | 触发指数退避重试一次 +1（不计第一次写入） |

### 用法示例

```promql
# 业务 WAL P99
histogram_quantile(0.99, sum by (le) (
  rate(flashstore_wal_write_duration_seconds_bucket{wal_type="business"}[5m])
))

# 各类型 WAL 字节占比
sum by (wal_type) (rate(flashstore_wal_write_bytes_total[5m]))

# 重试率
sum(rate(flashstore_wal_write_retries_total[5m]))
  / sum(rate(flashstore_wal_seq_total[5m]))
```

---

## 5. Commit / Apply / Tx 业务写路径

来源：[store/worker.go](../store/worker.go) `Commit` + `activeUpdate`。

| 名称 | 类型 | label | 何时 |
|------|------|-------|------|
| `flashstore_commit_duration_seconds` | Histogram | `worker_id` | `worker.Commit` 端到端，含 wal_write + apply + 同步 listener 回调——业务可感知的完整延迟 |
| `flashstore_apply_duration_seconds` | Histogram | `worker_id` | 仅 `applyEvents`（写本地 shard），不含 WAL 与 listener |
| `flashstore_tx_total` | Counter | `worker_id`, `result` | Commit 结果计数 |
| `flashstore_tx_events_per_commit` | Histogram | — | 每个 Commit 携带的 event 数 |

`commit_duration` − `apply_duration` − `wal_write_duration` ≈ listener 耗时（也可直接看 §8 的 `listener_duration_seconds{path="wal_sent"}`，比减法准）。

---

## 6. Read API

来源：[store/api.go](../store/api.go)，统一通过 `observeRead` helper 埋点（reader 非 `*Store` 时跳过——测试 mock 无副作用）。

| 名称 | 类型 | label | 何时 |
|------|------|-------|------|
| `flashstore_read_duration_seconds` | Histogram | `op` | Read API 调用耗时（defer 计时） |
| `flashstore_read_total` | Counter | `op`, `result` | 调用计数 |
| `flashstore_read_result_size` | Histogram | `op` | 批量 / 查询 API 的返回行数，单对象 API（GetObject / GetView / GetKV / CheckKVExists）跳过 |

`op` 取值：`get_object` / `get_objects` / `get_view` / `get_views` / `query_objects` / `query_objects_by_identity` / `query_views` / `query_views_by_identity` / `get_kv` / `check_kv_exists`。

---

## 7. Snapshot

来源：[store/snapshot_manager.go](../store/snapshot_manager.go)。

| 名称 | 类型 | label | 何时 |
|------|------|-------|------|
| `flashstore_snapshot_create_duration_seconds` | Histogram | `phase` | `CreateSnapshot` 各阶段耗时；phase ∈ {`header`, `shards_workers`, `finish`, `total`} |
| `flashstore_snapshot_create_total` | Counter | `result` | CreateSnapshot 完成事件，result ∈ {`ok`, `error`} |
| `flashstore_snapshot_size_bytes` | Histogram | — | 单次 snapshot 写入字节合计（header + 全部 shard + 全部 worker segment） |
| `flashstore_snapshot_restore_duration_seconds` | Histogram | `result` | `restoreFromSnapshot` 总耗时；result ∈ {`ok`, `error`, `no_snapshot`} |

`no_snapshot` 走静默降级路径（首次启动无可用 snapshot），是正常事件，不要报警。

---

## 8. Listener 回调

来源：[store/worker.go](../store/worker.go) `Commit` + [store/replayer.go](../store/replayer.go)。

| 名称 | 类型 | label | 何时 |
|------|------|-------|------|
| `flashstore_listener_duration_seconds` | Histogram | `worker_id`, `path` | Listener 回调聚合耗时；path ∈ {`wal_sent`（master 正常写路径所有 WalSentListener 总和）, `wal_replayed`（replay 期间单条 OnWalReplayed，immediate 路径）} |

无 listener name —— 目前只看"listener 是否拖累 commit/replay"的整体影响。需要细分到具体 listener 时再扩 `listener` 子 label。

---

## 9. Replay 进度 / 状态

来源：[store/metrics_state.go](../store/metrics_state.go) 的 `storeStateProvider`，通过 GaugeFunc 在 scrape 时读取活态。

| 名称 | 类型 | label | 含义 |
|------|------|-------|------|
| `flashstore_replay_state` | GaugeFunc | `worker_id` | 0=idle / 1=running / 2=errored；store 级状态对所有 worker 同值 |
| `flashstore_last_snapshot_wal_offset` | GaugeFunc | `worker_id` | 上次 snapshot 对应的 WAL offset，无历史 = -1 |

---

## 10. Kafka 后端指标边界

Kafka 后端当前不直接导出 franz-go 客户端内部的 batch、retry 或网络耗时指标。
`flashstore_wal_*` 只描述 store 调用 WAL writer 时可观察到的结果。若业务需要 broker
或客户端内部状态，应通过 Kafka 平台监控或在后续版本中增加有明确稳定语义的 collector。

---

## 11. 维护守则

### 不能动的

1. **已发布的 metric 名 / label 名 / label 取值**是 PromQL / 告警 / dashboard 的稳定接口。改名等价于打断历史时序——所有依赖该名字的查询都需要重写。**只能新增、严禁改名或删除**。如必须移除，先标 deprecated，跨一两个版本观察查询命中，再删。
2. **`wal.WalType.String()` 返回值**：作为 `wal_type` label 的稳定取值，扩枚举常量时只能追加，不可重排（注释里也写了）。
3. **`metrics.WALErrorKind` / `SnapshotPhase` / `SnapshotResult` / `TxResult` / `ReadOp` / `ListenerPath`** 这些枚举字符串值——同上，追加可以，改名不行。

### 必须做的

新增指标 / 新增枚举值时同步：

1. 在 [store/metrics/metrics.go](../store/metrics/metrics.go) 声明 `*Vec`（私有），写好 label 列表。
2. 暴露类型化方法（`ObserveXxx` / `IncXxx`），禁止外部直接调 `WithLabelValues`。
3. 若是 per-worker 热路径指标，加入 `WorkerMetrics` 字段并在 `Metrics.ForWorker` 内 curry 一次。状态量走 `StateProvider` + `RegisterStateGauges`。
4. 标签值用 `metrics.label*` 常量；新枚举类型定在 metrics 包并写清"已发布 = 不可改名"。
5. 同步更新本文档（§4–§10 任一章节）。
6. 如果指标随 cardinality 增长（label 取值无界），必须证明上界——拒绝任何会引入业务主键的 label。

---

## 12. 参考

- [架构总览](architecture.md)
- [API guide](api.md)
- 源码：[store/metrics/](../store/metrics)
