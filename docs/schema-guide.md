# Schema 操作指南

> 最后更新：2026-04-24

本文是**操作手册**：如何定义 Kind、加字段、删字段、rename、升级、回滚。想了解内部机制 / 设计权衡见 [type-system.md](type-system.md)；业务读写 API 用法见 [api.md](api.md)。

## 目录

- [1. 定义一个新的 Kind](#1-定义一个新的-kind)
- [2. 字段演进速查](#2-字段演进速查)
- [3. 加字段](#3-加字段)
- [4. 删字段](#4-删字段)
- [5. 改字段名 / 调整顺序](#5-改字段名--调整顺序)
- [6. 复活已删字段](#6-复活已删字段)
- [7. 代码回滚](#7-代码回滚)
- [8. 常见错误排查](#8-常见错误排查)

---

## 1. 定义一个新的 Kind

在业务 struct 上加 `store:"..."` tag，启动期调 `registry.RegisterStruct[T](kind)`：

```go
type Order struct {
    OrderID string          `store:"order_id,num=1"`
    Symbol  string          `store:"symbol,num=2,index"`
    Status  OrderStatus     `store:"status,num=3,index,type=int32"`
    Price   decimal.Decimal `store:"price,num=4,index,prec=8"`
    Qty     decimal.Decimal `store:"qty,num=5,prec=8"`
    Updated time.Time       `store:"updated,num=6"`
}

var orderKind registry.Kind
func init() {
    orderKind, _ = registry.NewKind("future", "order")
    if _, err := registry.RegisterStruct[Order](orderKind); err != nil {
        panic(err)
    }
}
```

**必填项**：
- tag 第一个位置参数是 schema 里的字段名（snake_case 惯例）
- `num=<int>` 每个字段的稳定身份，全局唯一且 > 0

**常见 tag 选项**（完整表见 [type-system.md §3.3](type-system.md#33-完整选项表)）：

| 选项 | 说明 |
|---|---|
| `index` | 字段进索引，`IndexHint` 查询用 |
| `prec=<N>` | Decimal 精度（Truncate 位数） |
| `default=<literal>` | 默认值；老 Row 缺该字段时 Fill 补位 |
| `type=<name>` | 自定义命名 scalar 类型（如 `type OrderStatus int32`）必须写 |
| `-` | 跳过字段 |

**关于 `num=<int>` 的编号惯例**：业务自定，不强制 1-based，可以跳号。一旦分配给某字段就**永久占用**（包括被删除后），不能给不同语义的字段复用——这是身份契约。

---

## 2. 字段演进速查

| 演进类型 | 需要什么 | 注意 |
|---|---|---|
| **加字段** | 分配新 Num + 声明 `default` | 老数据读出来走 Default |
| **删字段** | 删 struct 字段 + tag；推荐加 `schema.RetiredField{...}` 声明 | 见 [§4](#4-删字段) |
| **rename 字段** | 改 tag 里的 name，Num 不变 | 用到旧名字的代码（`RowView.Get("old_name")`）要同步改 |
| **调整 struct 字段顺序** | 随便调 | Num 是身份锚，顺序无意义 |
| **改 Decimal 精度 / String 长度** | 改 `prec=` / `max=` | 新写入按新约束，老数据不变 |
| **改 Indexed 标志** | 加/去 `index` 选项 | restore 时按当前 schema 重建索引 |
| **改字段 Default** | ❌ 禁止 | 回溯影响历史 Row，见 [§8 默认值变更](#8-常见错误排查) |
| **改同一 Num 的 Type** | ❌ 禁止 | 老 Row 和新 schema 类型错配 |

---

## 3. 加字段

```go
// v1
type Order struct {
    OrderID string `store:"order_id,num=1"`
    Status  int32  `store:"status,num=2,type=int32"`
}

// v2: 加 note 字段
type Order struct {
    OrderID string `store:"order_id,num=1"`
    Status  int32  `store:"status,num=2,type=int32"`
    Note    string `store:"note,num=3,default=n/a"` // 新 Num，带 default
}
```

**要点**：
- 分配一个从未使用过的 Num
- 对 scalar 字段，一般应该带 `default=`（不给也能跑，但老 Row 读出来会是 Go 零值）
- Custom 字段不支持 tag 里设 default；Scanner 要能处理 `nil` / 空 bytes（见 [type-system.md §4.3](type-system.md#43-新增-custom-字段业务-scanner-需处理-nil--空-bytes)）

---

## 4. 删字段

### 推荐做法

```go
// v1
type Order struct {
    OrderID string `store:"order_id,num=1"`
    Region  string `store:"region,num=3"`  // 想删这个
    Status  int32  `store:"status,num=4,type=int32"`
}

// v2: 删 Region
type Order struct {
    OrderID string `store:"order_id,num=1"`
    Status  int32  `store:"status,num=4,type=int32"`
    // Num=3 曾是 region (string, default="")，v2 移除
}

func init() {
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
    Retire(schema.RetiredField{Num: 3, Name: "region",
        Type: schema.FieldTypeString, Default: schema.NewString("")}).
    Int32(4, "status", ...).
    Build()
```

### 为什么推荐加 `Retired{}` 声明

**不是必须**——不加也能启动。加了的价值，**仅限同包内**：

1. **review 能看见**：diff 里 `-Region 字段 + +Retired{Num:3}` 对称，reviewer 一眼看明白"Num=3 永久占用"
2. **新字段撞 Num 即刻 panic**：同包内有人未来加字段写 `num=3` 会在**包 init 阶段**就 panic，不用等 master 启动
3. **同 Num 多次 Retire 内容不一致会被抓**：Builder.Retire 注册期校验幂等性

`RetiredField` **不参与跨进程的 schema 状态**——broadcast 不带它，snapshot Header 也不读它。Store 端的 tombstone 由"alive 缩水"diff 自动累积维护，与源码声明无关。

### 如果删字段不加声明会怎样

- 启动正常，store 自动把 Num=3 迁到内部 tombstone
- 语义保护仍然生效：以后有人想把 Num=3 作为不同 Type/Default 的字段加回来 → 启动 loud-fail
- 只是少了同包内 init 期的撞 Num 防御（推到 master 启动时由 store 端 tombstone 接管报错）

---

## 5. 改字段名 / 调整顺序

Num 是身份锚，**改名 / 改顺序都不需要特殊操作**：

```go
// v1
type Order struct {
    OrderID   string `store:"order_id,num=1"`
    Price     decimal.Decimal `store:"price,num=2,index,prec=8"`
}

// v2: rename + 换声明顺序——完全兼容
type Order struct {
    Price decimal.Decimal `store:"price,num=2,index,prec=8"`
    ID    string          `store:"id,num=1"`  // 原 order_id
}
```

**但业务代码要同步**：

```go
// 读路径里用了旧名字——需要改
view.GetResult("order_id")  // → view.GetResult("id")
```

索引在启动 restore 时按当前 schema 重建，不需要手动迁移。

---

## 6. 复活已删字段

**同 Type/Default 复活 = 直接加回来，store 自动从 tombstone 搬回 alive**：

```go
// v3 又把 region 加回来
type Order struct {
    OrderID string `store:"order_id,num=1"`
    Region  string `store:"region,num=3"`  // 恢复 Num=3，同 string，同 default=""
    Status  int32  `store:"status,num=4,type=int32"`
}

// 如果 v2 曾经加过 Retired{Num:3}，v3 里要删掉这个声明
// （否则 Builder.addField 会 panic: num=3 is retired）
func init() {
    // 不再传 RetiredField{Num:3, ...}
    _, err := registry.RegisterStruct[Order](orderKind)
    ...
}
```

**不同 Type/Default 复活 = 禁止**。启动期 `checkSchemasCompat` 拒绝：

```
kind future.order: num=3 was dropped with Type=string, now reintroduced as Type=int32
(Num reuse with different type forbidden; pick a new num)
```

业务想用 Num=3 做别的语义字段 → **分配新 Num**。

---

## 7. 代码回滚

### 场景

v2 加了字段 C，部署后发现 bug，回滚到 v1 代码。v1 不知道 C 存在。

### 启动行为

**静默通过**。master 启动期 compat 检查不会拦。`fieldsRegistry.upsert` 自动把消失的 alive（C）迁到 tombstone。不需要改 v1 代码、不需要加 `Retired{}`。

### 数据后果

| 记录类型 | v1 运行期间 | roll forward 回 v4 后 |
|---|---|---|
| v2 写过但 v1 **未更新** | C 值保留在 shard/snapshot 里，v1 Fill 丢弃（业务侧看不到） | v4 Fill 能读回 C 值 ✅ |
| v1 期间**被更新过** | v1 Extract 不写 Num=C，新 Row 覆盖旧 Row | Num=C 不在 Row 里，Fill 走 Default ❌ |

**结论**：回滚期间 C 字段等于"暂停"——未碰的记录无损，碰过的记录丢失 C 值。业务方通常能接受这种代价（毕竟 rollback 就是"临时退回"）。

### Roll forward

- **v4 保留 C**（同 Type/Default）：直接定义字段，store 自动从 tombstone 搬回 alive，见 [§6](#6-复活已删字段)
- **v4 永久废弃 C**：代码里不定义 C，可加 `Retired{Num:C}`（推荐）也可不加——store 的 tombstone 已经记住了 C 的 Type/Default，保护不会丢失

---

## 8. 常见错误排查

| 错误信息 | 原因 | 修复 |
|---|---|---|
| `field "xxx" missing required \`num=<int>\`` | tag 缺 num 选项 | 加 `num=<唯一 int>` |
| `field type xxx is a defined type ... add \`type=xxx\`` | 自定义命名 scalar（如 `type Status int32`）没写 `type=` | tag 里加 `type=int32`（或对应类型） |
| `duplicate num=N (fields "x" and "y")` | 同一 struct 两字段用同一 Num | 修正 Num 分配 |
| `num=N is retired` | 新字段撞到 `Retired{}` 声明的 Num | 换一个未使用过的 Num |
| `num=N was dropped with Type=X, now reintroduced as Type=Y` | 试图用历史 Num 做不同语义字段 | 换一个新 Num |
| `field "x" (pos N) default changed` | 改了某字段的 default | 不能改——加新字段用新 default 代替 |
| `num=N retired twice with conflicting metadata` | 同包 Builder 里两次 `Retire` 同 Num 但 Type/Default/Name 不一致 | 统一两次声明的 metadata |
| `ErrIntOverflow` at startup | schema 从 int64 改成 int32，但历史数据里有超出 int32 范围的值 | 恢复 int64，或加新字段迁移 |
| `schema drift on index: field "x" stored as Y, schema declares Z` | 代码里偷偷改了字段 Type 但没迁移 | 禁止的操作——走"加新字段"路径 |

---

## 附：关键契约一句话

1. **每个字段必须有 `num=<int>`**，且全局唯一、> 0、永久占用
2. **改 Type / 改 Default = 禁止**；要改就加新字段（新 Num）
3. **删字段推荐写 `Retired{}`**，不写也能启动；写了仅有同包内 init 期撞 Num 防御 + review 可读性，跨进程 Num 复用拦截由 store 端 tombstone 兜底
4. **回滚透明**——老代码部署后 store 自动接受，不需要改代码

想知道"为什么这样设计"、"自描述 Row 格式长啥样"、"compat 检查具体走哪几步"——继续读 [type-system.md](type-system.md)。
