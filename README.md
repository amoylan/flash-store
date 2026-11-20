# flash-store

[![CI](https://github.com/amoylan/flash-store/actions/workflows/ci.yml/badge.svg)](https://github.com/amoylan/flash-store/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go)](https://go.dev/)
[![Status: Experimental](https://img.shields.io/badge/status-experimental-orange.svg)](#项目状态)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

flash-store 是一个基于 WAL（Write-Ahead Log）的 Go 内存状态存储库。它提供事务写入、
内存索引、WAL replay、snapshot 和事件 listener，可作为交易、账务等状态型服务的存储组件。
它是嵌入业务进程的 library，不是独立数据库，也不提供自动选主、故障转移或集群编排。

## 项目状态

> [!WARNING]
> flash-store 目前处于实验性 v0 阶段。公共 API、配置结构和持久化格式可能在后续版本中变化。
> 核心内存路径有单元测试和 race detector 检查，但 Kafka、S3 等后端尚无公开的端到端测试套件。
> 在生产环境使用前，请基于自身数据规模、故障模型和恢复流程完成验证。

## 定位与适用场景

**它是什么**：一个嵌入业务进程的内存状态存储库。单条写路径为 append WAL（Kafka 或
本地文件）→ 更新内存索引 → 同步通知 listener，读路径直接访问内存。实际持久性取决于
WAL 后端及其部署配置。

**适合的场景**：

- 交易系统的账户 / 订单 / 持仓状态（单 key 读写为主，需要通过 WAL 恢复状态）
- 采用单写者模型，通过 WAL 构建只读副本或快照副本的内存状态
- 事件驱动架构中"状态 + 事件流"一体化（业务事件随 WAL 流出，见 Listener）

**不适合的场景**：

- 通用关系型数据库的替代品（无 SQL、无 ad-hoc 聚合）
- 跨实体事务（见 [限制](#限制)）
- 大数据量冷存储（纯内存，TTL 驱逐辅助）

## 核心特性

- **WAL 驱动的主从复制** — Master 写 WAL，Slave 通过 replay 重建状态；写路径先 durable 后 apply
- **Snapshot 持久化** — SnapshotSlave 定期创建全量快照，restore + 增量 replay 快速恢复
- **角色解耦** — Master / SnapshotSlave / ReadOnlySlave 职责清晰，Slave 可无业务代码注册
- **自描述 Schema** — Row 自带字段类型标签，schema 演进有 compat 检查兜底（加/删/rename 字段安全）
- **Listener 通知钩子** — 业务事件随 WAL 流出；下游一致性补偿（去重 / 补发）由业务侧构建

## 快速开始

要求 **Go 1.25+**。依赖全部来自公共 Go proxy。

安装当前版本：

```bash
go get github.com/amoylan/flash-store/store@latest
```

构建和运行本仓库：

```bash
# 构建核心库
go -C store build ./...

# 跑单元测试（纯内存 mock，无外部依赖）
go -C store test ./...

# 可运行示例（注册 Kind → 事务写 → 索引查询 → Listener）
go -C store/example run ./demo
```

## 使用示例

核心流程如下，完整可运行代码见 [store/example](store/example)：

```go
// 1. 注册业务对象（struct tag 即 schema）
type Order struct {
    AccountID int64           `store:"account_id,num=1"`
    Status    int32           `store:"status,num=2,index"`
    Price     decimal.Decimal `store:"price,num=3,index,prec=8"`
}

orderKind, err := registry.NewKind("example", "order")
if err != nil {
    panic(err)
}
if _, err := registry.RegisterStruct[Order](orderKind); err != nil {
    panic(err)
}

// 2. 创建本地示例 Store。生产环境应配置 Kafka 或 file WAL。
s, err := store.NewStore(config.DefaultConfigForTest(), nil)
if err != nil {
    panic(err)
}
defer s.Close()

ctx := context.Background()
identity := uint64(1001)
order := &Order{
    AccountID: int64(identity),
    Status:    1,
    Price:     decimal.NewFromInt(50000),
}

// 3. 事务写（先落 WAL 再 apply 内存）
err = s.Transaction(ctx, func(tx *store.Tx) error {
    ev := tx.BeginEvent("order.place", nil)
    return store.SetObject(ev, identity, orderKind, "order-123", order, 0)
})
if err != nil {
    panic(err)
}

// 4. 读（纯内存）
got, err := store.GetObject[Order](s, identity, orderKind, "order-123")
if err != nil {
    panic(err)
}
fmt.Printf("order: %+v\n", got)
```

## 版本与兼容性

项目尚未发布稳定版本。`v0` 阶段可能调整公共 API、配置结构，以及 WAL 和 snapshot 格式；
升级前请阅读对应版本的变更说明并验证已有数据。Go module 位于 `store/` 子目录，因此发布标签
使用 `store/vX.Y.Z` 形式。

## 当前验证范围

CI 对核心模块执行格式检查、依赖一致性检查、构建、`go vet`、单元测试和 race detector。
当前仓库不包含 Kafka、S3 后端的端到端测试，也不提供公开的性能基线。相关能力在生产使用前
需要结合真实基础设施完成故障恢复、重放一致性和负载测试。

## 限制

设计取舍明确，使用前请确认以下限制符合你的场景：

- **无跨 partition 事务**：一个 Tx 内所有写必须落在同一 partition（`identity % 16` 相同）。
  跨用户 / 跨实体的原子写不支持——按用户设计 identity 时这不是问题。
- **全局非严格有序**：有序性只在单 partition 内保证（per-partition WAL 序）。
  跨 partition 无全局顺序；同 partition 内并发提交的相对顺序由调度决定，
  **同一 (kind, key) 的并发写必须由业务侧串行化**（标准做法是 per-user lock）。
- **单写者模型**：Master 单写；Slave 只读 / 只打快照。无多写者协调。
- **无内建高可用控制面**：不负责选主、故障转移、成员管理或部署编排。
- **无内建下游一致性补偿**：store 不跟踪下游消费进度（无 ack / 水位 / 去重机制）。
  消息同步的去重与补发由业务侧基于 Listener + replay 自行构建
  （设计参考 [docs/todo/outbox-design.md](docs/todo/outbox-design.md)，未实现）。
- **内存存储**：数据容量以内存为界；TTL 驱逐辅助清理过期数据。
- **查询 API 无分页 / 游标**：`QueryObjects` 一次性物化结果，顺序不保证；
  大批量扫描需自行分批 + 排序。
- **numWorkers / numShards 固定为 16 / 64**（编译期常量，见 store/worker.go / store/shard.go）。

## 仓库结构

仓库当前包含一个 Go module，根目录通过 `go.work` 引用 `store/`：

| 目录 | 说明 |
|------|------|
| `store/` | 核心存储引擎库（本仓库主体，业务方 import 的模块） |
| `store/example/` | 可运行示例（Kind 注册 / 事务写 / 索引查询 / Listener） |
| `docs/` | 设计文档与指南 |
| `Makefile` | 本地检查和 protobuf 生成入口 |

## 文档

**上手**：

- **[架构总览](docs/architecture.md)** — 整体架构、各组件职责、角色对比、数据流
- **[API 指南](docs/api.md)** — 业务代码如何读写 / 注册 Kind / 消费 Listener
- **[Schema 操作指南](docs/schema-guide.md)** — 如何加字段、删字段、rename、回滚；cookbook 风格

**深入**：

- [类型系统参考](docs/type-system.md) — Row / FieldValue / wire format / 演进规则与设计权衡
- [Outbox 设计（TODO）](docs/todo/outbox-design.md) — 业务侧消息发件箱（推荐形态，未实现）
- [Metrics 手册](docs/metrics.md) — Prometheus 指标全集与命名约定

## 参与贡献与安全问题

提交改动前请阅读 [CONTRIBUTING.md](CONTRIBUTING.md)。安全漏洞请按
[SECURITY.md](SECURITY.md) 中的方式私下报告，不要提交公开 Issue。

## License

[MIT](LICENSE)
