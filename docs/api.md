# flash-store API 使用指南

> 最后更新：2026-08-27

本文档讲**业务代码**如何使用 flash-store 的 API：注册对象类型、读写数据、过滤查询、消费 WAL 事件。架构与内部机制见 [architecture.md](architecture.md)；Row / tag / schema 演进规则见 [type-system.md](type-system.md)。

## 目录

- [1. 快速上手](#1-快速上手)
- [2. 注册业务对象类型](#2-注册业务对象类型)
  - [2.1 tag-driven：RegisterStruct](#21-tag-drivenregisterstruct)
  - [2.2 Builder：For + Int32/Int64/Decimal...](#22-builderfor--int32int64decimal)
  - [2.3 删除字段：推荐声明 `RetiredField`](#23-删除字段推荐声明-retiredfield)
  - [2.4 选哪个？](#24-选哪个)
- [3. 写路径](#3-写路径)
  - [3.1 Tx / TxEvent](#31-tx--txevent)
  - [3.2 SetObject / DeleteObject](#32-setobject--deleteobject)
  - [3.3 SetKV / DeleteKV](#33-setkv--deletekv)
- [4. 读路径](#4-读路径)
  - [4.1 GetObject / GetObjects](#41-getobject--getobjects)
  - [4.2 GetView — 不重建 struct 的只读视图](#42-getview--不重建-struct-的只读视图)
  - [4.3 QueryObjects / QueryViews + IndexHint + Filter](#43-queryobjects--queryviews--indexhint--filter)
  - [4.4 KV 读 API](#44-kv-读-api)
- [5. Listener：消费 WAL 事件](#5-listener消费-wal-事件)
  - [5.1 两种 Listener](#51-两种-listener)
  - [5.2 从 wal.Cmd 解回业务对象](#52-从-walcmd-解回业务对象)
  - [5.3 一致性补偿：业务侧基于钩子自行构建](#53-一致性补偿业务侧基于钩子自行构建)
- [6. 启动与停机](#6-启动与停机)
  - [6.1 Master 启动](#61-master-启动)
  - [6.2 SnapshotSlave 启动](#62-snapshotslave-启动)
  - [6.3 ReadOnlySlave 启动](#63-readonlyslave-启动)
- [7. 角色能做什么](#7-角色能做什么)
- [8. 常见错误与含义](#8-常见错误与含义)

---

## 1. 快速上手

一段代码串起所有概念：

```go
// 1. 定义业务 struct，用 `store:"..."` tag 声明 schema
//    num=<int> 是字段稳定身份，必填（详见 type-system.md §3.2）
type Order struct {
    OrderID  string          `store:"order_id,num=1"`
    Symbol   string          `store:"symbol,num=2,index"`
    Status   OrderStatus     `store:"status,num=3,index,type=int32"`  // 自定义枚举必须显式 type=
    Price    decimal.Decimal `store:"price,num=4,index,prec=8"`
    Qty      decimal.Decimal `store:"qty,num=5,prec=8"`
    Updated  time.Time       `store:"updated,num=6"`
}

// 2. init() 里注册：Kind + RegisterStruct（版本由 store 启动期自动推导，业务无需传）
var orderKind registry.Kind
func init() {
    var err error
    orderKind, err = registry.NewKind("future", "order")
    if err != nil { panic(err) }
    if _, err := registry.RegisterStruct[Order](orderKind); err != nil { panic(err) }
}

// 3. 启动 Store（业务进程即 Master）
s, _ := store.NewStore(config.Master(...))
if _, err := s.RestoreAndStartReplay(ctx, "", store.ReplayOptions{
    Mode: wal.ReplayToLatest, ApplyToStore: true,
}); err != nil { return err }

// 4. 写
err := s.Transaction(ctx, func(tx *store.Tx) error {
    ev := tx.BeginEvent("PlaceOrder", bizData)
    return store.SetObject(ev, userID, orderKind, order.OrderID, order, 0)
})

// 5. 读
got, err := store.GetObject[Order](s, userID, orderKind, orderID)

// 6. 查询（按索引字段）
orders, err := store.QueryObjectsByIdentity[Order](s, userID, orderKind,
    []store.IndexHint{{Name: "status", Ranges: index.Ranges{
        index.NewEqualRange(schema.NewInt32(1))}}},
    func(v *registry.RowView) bool { return true },  // 可选过滤器
)
```

---

## 2. 注册业务对象类型

flash-store 需要在启动前**先注册每种 Kind 的 schema**——这决定：
- Row 的字段布局
- 哪些字段进索引
- 字段 Default（业务必须稳定，不允许改）
- Go struct `*T` ↔ 存储 Row 的双向转换

### 2.1 tag-driven：RegisterStruct

**推荐的方式**。业务在 struct 字段上加 `store:"..."` tag，调一次 `registry.RegisterStruct[T]` 搞定。

```go
type Order struct {
    OrderID  string          `store:"order_id,num=1"`
    Symbol   string          `store:"symbol,num=2,index"`
    Status   OrderStatus     `store:"status,num=3,index,type=int32"`
    Price    decimal.Decimal `store:"price,num=4,index,prec=8"`
    Extra    OrderExtra      `store:"extra,num=5"`           // 自动识别为 Custom（见下）
    Internal string          `store:"-"`                     // 跳过
    private  int                                             // 无 tag 默认跳过
}

def, err := registry.RegisterStruct[Order](orderKind)
```

tag 格式 `store:"<name>,num=<int>[,<opt>[=<val>]...]"`。`num=<int>` 是字段**稳定身份**，必填——详见 [type-system.md §3](type-system.md#3-tag-grammar)。

**Custom 字段**（复杂对象，如嵌套 struct）只要在 `*F` 上实现 `schema.CustomValuer` / `CustomScanner` 就能自动识别，不用显式 `custom` tag：

```go
type OrderExtra struct { Fee decimal.Decimal; Tags []string }
func (e *OrderExtra) Value() ([]byte, error) { return json.Marshal(e) }
func (e *OrderExtra) Scan(b []byte) error    { return json.Unmarshal(b, e) }
```

### 2.2 Builder：For + Int32/Int64/Decimal...

显式 API，用于 tag 不够表达的场景（计算字段、某些字段需要自定义 getter、同一 struct 字段要映射到不同 schema 位置等）。

每个字段方法第一个参数是 `num int32`——和 tag 的 `num=<int>` 同义，字段稳定身份，必填。

```go
def := registry.For[Order](orderKind).
    String(1, "order_id", func(o *Order) *string { return &o.OrderID }).
    String(2, "symbol",   func(o *Order) *string { return &o.Symbol }, schema.Indexed()).
    Int32(3, "status",    func(o *Order) *int32  { return &o.Status }, schema.Indexed()).
    Decimal(4, "price",   func(o *Order) *decimal.Decimal { return &o.Price },
        schema.Indexed(), schema.DecimalPrec(8)).
    Build()
if err := registry.RegisterDef(def); err != nil { panic(err) }
```

Custom 字段用 package-level 泛型函数（Go 方法不能带自己的类型参数）：

```go
b := registry.For[Order](orderKind).
    String(1, "order_id", func(o *Order) *string { return &o.OrderID })
b = registry.CustomField[Order, OrderExtra](b, 5, "extra",
    func(o *Order) *OrderExtra { return &o.Extra })
def := b.Build()
```

### 2.3 删除字段：推荐声明 `RetiredField`

Num 一旦分配就永久占用（不能再给别的字段用）。删除字段**推荐**（非强制）在注册时声明历史退休记录——同包内有人加新字段撞已 retired 的 Num 时，包 init 立刻 panic（不等部署）：

```go
// 假设曾经有 Num=3 region (string, default="")，v5 移除
func init() {
    orderKind, _ = registry.NewKind("future", "order")
    _, err := registry.RegisterStruct[Order](orderKind,
        schema.RetiredField{Num: 3, Name: "region",
            Type: schema.FieldTypeString, Default: schema.NewString("")},
    )
    if err != nil { panic(err) }
}
```

Builder 链式等价：

```go
def := registry.For[Order](kind).
    Int64(1, "order_id", ...).
    Int32(2, "status", ...).
    Retire(schema.RetiredField{Num: 3, Name: "region",
        Type: schema.FieldTypeString, Default: schema.NewString("")}).
    Decimal(4, "price", ...).
    Build()
```

忘写 `RetiredField` 不会让启动失败：store 端 tombstone 由"alive 缩水"diff 自动累积，跨进程 / 跨重启的 Num 复用拦截照常生效。仅丢失"同包内 init 期撞 Num 立刻 panic"这层防御。详见 [type-system.md §3.2](type-system.md#32-num字段身份的稳定锚)。

### 2.4 选哪个？

默认用 **RegisterStruct**。短、直观、编译期类型安全，enum 类型自然支持。

只在以下情况用 Builder：
- 字段名和 schema 位置需要解耦（同一 struct 两个字段映射到 schema 一个位置、或反之）
- 需要完全自定义 getter 逻辑（比如从两个字段合成一个 schema 字段）
- 想明确控制初始化顺序（tag 按 struct 字段顺序，Builder 可任意）

---

## 3. 写路径

只有 **Master** 可以写。写的唯一入口是 `Tx` → `TxEvent` → `SetObject/SetKV/DeleteObject/DeleteKV`。

### 3.1 Tx / TxEvent

```go
tx := s.BeginTx(ctx)
defer tx.Rollback()           // 可选：未 Commit 前 ctx 中断走 rollback

ev1 := tx.BeginEvent("PlaceOrder", bizDataForPlaceOrder)  // 一个 Tx 可有多个 Event
// ... 操作 ev1 ...

ev2 := tx.BeginEvent("UpdateAccount", bizDataForAccount)
// ... 操作 ev2 ...

if err := tx.Commit(); err != nil {
    return err
}
```

**关键约束**：一个 `Tx` 里所有 Event 的 identity 必须**落在同一 worker**，即 `identity % 16` 要相同。否则 Commit 报错。同一 Event 内的 identity 必须完全一致；跨 Event 只要 worker 对齐、shard 可以不同。

`bizData` 是业务自定义的 `wal.BizData` 接口（实现 `Bytes() []byte` + `Version() uint8`），供 Listener 侧重建消息用。

### 3.2 SetObject / DeleteObject

```go
// 写对象
err := store.SetObject(ev, identity, orderKind, key, orderPtr, ttl)
//                                    ^^^        ^^^          ^^^
//                                    Kind       业务 struct  time.Duration(0)=不过期

// 删除对象
err := store.DeleteObject(ev, identity, orderKind, key)
```

`orderPtr` 必须是 `*Order`，且 `Order` 注册时的 `T` 要匹配。存储内部调 `Def[Order].Extract(orderPtr)` 提取成 Row，业务对象之后随意修改不影响存储——Row 是值拷贝形态。

### 3.3 SetKV / DeleteKV

简单的 `map[string][]byte`，不走 schema / 索引，单纯字节存储：

```go
err := store.SetKV(ev, identity, "flag:maintenance", []byte("1"), 30*time.Minute)
err := store.DeleteKV(ev, identity, "flag:maintenance")
```

业务场景：用户级别的简单标记位、计数器、金额等"和 Kind 无关"的小块数据。

---

## 4. 读路径

Master 和 ReadOnlySlave 都可以读。SnapshotSlave 不支持读 API（它不 import 业务包、不注册 Def[T]，所以没法做 `*T` 回填——将来若有需求可以基于 KindDescriptor 暴露 RowView 查询，但目前未开启）。

### 4.1 GetObject / GetObjects

```go
order, err := store.GetObject[Order](s, identity, orderKind, key)
// → 失败: ErrKeyNotFound / ErrTypeNotMatch / ErrKindNotRegistered

orders, err := store.GetObjects[Order](s, identity, orderKind,
    []string{"ord-1", "ord-2", "ord-3"})
```

每次调用内部走 `new(Order)` + `Def.Fill(row, &order)`——返回的是**全新对象**，业务修改不影响存储。

### 4.2 GetView — 不重建 struct 的只读视图

高频读、只关心少数几个字段（撮合消息构造、推送过滤等）时用 View，避免 Fill 整个 struct：

```go
view, err := store.GetView[Order](s, identity, orderKind, key)
price, err := view.GetResult("price").Decimal()
status, err := view.GetResult("status").Int32()
```

`view` 是 `*registry.RowView`——直接指向存储中的 Row（拷贝副本），按字段名访问。零 struct 分配；单字段访问走 `RowView.GetFieldValue` → `TypedDef.FieldByName` 拿 FieldDescriptor（含 Num + Default）→ `Row.GetByNum(num)` + typed unbox。

### 4.3 QueryObjects / QueryViews + IndexHint + Filter

按索引字段范围 + 可选二次 filter 查询：

```go
orders, err := store.QueryObjectsByIdentity[Order](s, identity, orderKind,
    []store.IndexHint{
        {Name: "status", Ranges: index.Ranges{
            index.NewEqualRange(schema.NewInt32(1)),                          // status == 1
        }},
        {Name: "symbol", Ranges: index.Ranges{
            index.NewEqualRange(schema.NewString("BTC_USDT")),                // symbol == "BTC_USDT"
        }},
    },
    // 二次过滤器：只在索引没完全覆盖条件时用
    func(v *registry.RowView) bool {
        price, _ := v.GetResult("price").Decimal()
        return price.GreaterThan(decimal.NewFromInt(30000))
    },
)
```

- `IndexHint.Name` 必须是 schema 里 Indexed 的字段；否则查询 fall back 到全表扫（若有多个 hint 都没索引则返回空）
- 多个 IndexHint 的结果做**交集**
- Filter 接受 `*registry.RowView`（不是业务 struct），避免 Fill。可以为 nil
- 查全 shard 用 `QueryObjects`（无 identity 参数）；按用户查用 `QueryObjectsByIdentity`

`QueryViews` / `QueryViewsByIdentity` 同理，但返回 `[]*registry.RowView`——完全跳过 struct 重建。

### 4.4 KV 读 API

```go
value, err := store.GetKV(s, identity, "flag:maintenance")  // []byte, ErrKeyNotFound
exists := store.CheckKVExists(s, identity, "flag:maintenance")
```

---

## 5. Listener：消费 WAL 事件

业务侧常见需求：每条业务写入后做 side effect（构造撮合消息推给下游、归档、推送）。flash-store 通过 Listener 接口让业务接入 WAL 事件流。

### 5.1 两种 Listener

| 接口 | 触发时机 | 用途 |
|------|----------|------|
| `store.WalSentListener.OnWalSent` | Master 的 `worker.Commit` 写完 WAL 后 | 正向写入的业务处理（如发撮合消息） |
| `store.WalReplayedListener.OnWalReplayed` | Replay 期间每条业务 WAL apply 成功后 | 重建/补发消息（如 Push 重启后按 WAL 重发） |

二者结构基本一致（收到 `store.WalRecord`），区别只在触发时机。实现侧：

```go
type Push struct{}

func (p *Push) OnWalSent(ctx context.Context, rec store.WalRecord) {
    for _, ev := range rec.Events {
        for _, cmd := range ev.Cmds {
            p.handle(ctx, ev, cmd, rec.Offset, rec.Partition)
        }
    }
}

// 注册：
s.AddWalSentListener(&Push{})
```

Listener 须**异步**处理——长时间阻塞会拖住 WAL 写入路径。典型做法是往 channel 里塞后异步消费。

两个接口都是**裸通知钩子**：`WalRecord` 只含 `Partition / Events / Offset`，没有 ack 相关字段，也不做任何去重 / 补发判定（2026-08 起 ack 系统已整体移除）。每条业务 WAL 恰好触发一次，重复发送与否由业务侧自行判定。

### 5.2 从 wal.Cmd 解回业务对象

`wal.Cmd` 里只有 `Kind`、`Row`（schema.Row 自描述字节形态）、`Op`、`Key`、`Ttl`——**没有** `*Order` 这种业务 struct。业务需要自己解开：

```go
for _, cmd := range ev.Cmds {
    if cmd.IsDelete() {
        push.SendTombstone(cmd.Key)
        continue
    }
    // cmd.IsSet(): 解回业务 struct
    var order Order
    if err := store.DecodeCmd(cmd, &order); err != nil {
        log.Error("decode cmd", err); continue
    }
    push.SendOrder(&order)
}
```

或者只读若干字段：

```go
view, err := store.DecodeCmdAsView(cmd)       // OpDelete 返回 ErrCmdNotSetOp
price, _ := view.GetResult("price").Decimal()
```

两个 helper 都在 `store` 包下（见 [listener_decode.go](../store/listener_decode.go)）。遇到 `OpDelete` 返回 `errors.ErrCmdNotSetOp` 哨兵——业务按 `cmd.Key` 走 delete 分支。

### 5.3 一致性补偿：业务侧基于钩子自行构建

store 不再提供 ack / 水位 / 补发 API（`OnAck` / `AckNextUnackedOffset` / `RegisterAckRouting` / `TryClaim` / `ReleaseClaim` 等均已随 2026-08 的 ack 系统移除）。下游一致性补偿由业务侧基于两个钩子自行构建，典型形态：

- **首发**：`WalSentListener` callback 在 commit 路径立即发送（低时延）；
- **兜底**：独立启动一条 `ReplayForever` + Listener + `ApplyToStore: false` 的 replay，从起点重放并重发遗漏消息——是否跳过已处理的消息由业务侧自持断点判定；
- **去重基线**：重复消息靠下游幂等（消息键 = partition + offset）消化；seq 层面的重复由 `SeqDeduplicator` 保证不会 apply 两次。

更完整的 outbox 设计见 [todo/outbox-design.md](todo/outbox-design.md)（TODO）。

---

## 6. 启动与停机

### 6.1 Master 启动

```go
s, err := store.NewStore(storeConfig /* Role: Master, Wal: kafka/file */)
if err != nil { return err }
defer s.Close()

// 注册 Listener（如果有）——正向写路径的 WalSentListener
_ = s.AddWalSentListener(&Push{})

// 恢复 + 追主：ReplayToLatest 追平到 HEAD
if _, err := s.RestoreAndStartReplay(ctx, "", store.ReplayOptions{
    Mode:         wal.ReplayToLatest,
    ApplyToStore: true,
}); err != nil { return err }

// 补发兜底（可选，业务自行构建）：独立 ReplayForever + Listener + ApplyToStore:false，
// 重放并重发遗漏消息；去重 / 断点由业务侧自持（见 §5.3）
_, err = s.StartReplay(ctx, store.ReplayOptions{
    Mode:         wal.ReplayForever,
    ApplyToStore: false,
    Listener:     &Push{},
})
if err != nil { return err }

// RestoreAndStartReplay 里已自动 PublishSchemas
// 现在业务可以接流量写入了
```

Master 挂 `WalReplayedListener` 的 replay 不做任何去重——补偿逻辑（跳过已处理、幂等）由业务侧实现，见 [architecture.md §4](architecture.md#4-listener-与下游通知)。

### 6.2 SnapshotSlave 启动

部署在独立 pack-server；业务代码**不依赖任何 Kind 注册**（SnapshotSlave 不 import 业务包，shard 内部 `noBusinessDef=true`，不建索引）。Row 自描述，所以不注册 Def 也能正确解码 / 再编码。

```go
s, _ := store.NewStore(slaveConfig /* Role: SnapshotSlave */)

for {
    // 持续 replay，业务 WAL 直接 apply（快照任意时刻安全）
    _ = s.StartReplay(ctx, store.ReplayOptions{
        Mode: wal.ReplayForever, ApplyToStore: true,
    })

    time.Sleep(snapshotInterval)

    s.StopReplay()
    version, err := s.CreateSnapshot(ctx)
    // ... log version, 下一轮 ...
}
```

### 6.3 ReadOnlySlave 启动

给 query-server 用。需要业务 struct 注册（和 Master 一样）。

```go
s, _ := store.NewStore(readerConfig /* Role: ReadOnlySlave */)

_, err := s.RestoreAndStartReplay(ctx, "", store.ReplayOptions{
    Mode: wal.ReplayForever, ApplyToStore: true,
})
if err != nil { return err }

// 现在可以用 GetObject / QueryObjects 查
```

ReadOnlySlave 不创建 snapshot——纯粹就是一个跟随 Master 的可查询内存库。

---

## 7. 角色能做什么

| 操作 | Master | SnapshotSlave | ReadOnlySlave |
|---|---|---|---|
| `registry.RegisterStruct` | ✅ 必须 | ❌ 不能 | ✅ 必须 |
| `store.SetObject / DeleteObject / SetKV / DeleteKV` | ✅ | ❌ | ❌ |
| `store.GetObject / GetView / QueryObjects / QueryViews / GetKV` | ✅ | ❌ | ✅ |
| `store.AddWalSentListener` | ✅ | 无意义 | 无意义 |
| Replay 期 `WalReplayedListener` | ✅ | ✅（少见） | 可选 |
| `store.CreateSnapshot` | ❌ | ✅ | ❌ |
| `store.RestoreAndStartReplay` | ✅ | ✅ | ✅ |

---

## 8. 常见错误与含义

| 错误 | 含义 | 怎么办 |
|---|---|---|
| `errors.ErrKindNotRegistered` | 调用 `GetObject[T]` / `SetObject` 的 `kind` 还没 `RegisterStruct` / `RegisterDef` | 检查 init() 顺序，确认 `registry.RegisterStruct[T](kind, ...)` 已调用 |
| `errors.ErrKeyNotFound` | key 不存在（或 TTL 过期被清除） | 业务正常分支 |
| `errors.ErrTypeNotMatch` | 存储里这条 key 的 Kind 和请求的 Kind 不一致（跨 Kind 碰撞） | 业务 bug，检查 Kind/key 命名空间 |
| `errors.ErrCmdNotSetOp` | `DecodeCmd` / `DecodeCmdAsView` 在 OpDelete 上被调用 | 外层 switch `cmd.IsSet() / cmd.IsDelete()` 分支 |
| `schema.ErrTypeMismatch` | `FieldValue.Int32() / String() / Decimal()` 访问时类型不匹配 | 代码 bug——检查 RowView 里字段访问顺序、或字段类型写错 |
| `schema.ErrIntOverflow` | `FieldValue.Int32()` 读取 Int64 存储值且超出 int32 范围 | Schema 从 int64 改成 int32 导致——要么恢复成 int64，要么改成加新字段 |
| 启动时 `snapshot: schema compat: field %q type changed` | snapshot 里存的字段类型和代码现在注册的不一致 | schema 演进违规（见 [type-system.md §5](type-system.md#5-演进规则)），改回兼容演进 |
| 启动时 `snapshot: schema compat: field %q default changed` | 改了老字段的 Default | Default 是承诺，不允许改；加新字段替代 |

---

## 参考

- [architecture.md](architecture.md) — 整体架构、存储/主从/Listener 通知
- [type-system.md](type-system.md) — Row 机制、tag 语法、schema 演进规则（本文多处引用）
- [todo/outbox-design.md](todo/outbox-design.md) — 业务侧消息发件箱设计（TODO）
