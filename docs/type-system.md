# flash-store 类型系统

> 最后更新：2026-04-24

本文讲 flash-store 的**数据类型层**：业务 struct 如何映射到存储，Row / FieldValue 内部机制，`store:"..."` tag 语法，schema 演进允许什么、禁止什么、为什么这样设计。

> **新人建议**：如果只是想快速上手"怎么定义 Kind / 加字段 / 删字段 / rollback 会发生什么"——先读 [schema-guide.md](schema-guide.md)（cookbook 风格，约 200 行）。本文偏参考 / 设计说明（~650 行），信息密度高、含设计权衡和边界案例。

实战 API 用法见 [api.md](api.md)，整体架构见 [architecture.md](architecture.md)。

## 目录

- [1. 总览：三层模型](#1-总览三层模型)
- [2. Row / FieldValue 机制](#2-row--fieldvalue-机制)
  - [2.1 Row 的 num-addressed 结构](#21-row-的-num-addressed-结构)
  - [2.2 FieldValue tagged union](#22-fieldvalue-tagged-union)
  - [2.3 支持的 FieldType](#23-支持的-fieldtype)
  - [2.4 Row wire format v2](#24-row-wire-format-v2)
- [3. tag grammar](#3-tag-grammar)
  - [3.1 语法](#31-语法)
  - [3.2 Num：字段身份的稳定锚](#32-num字段身份的稳定锚)
  - [3.3 完整选项表](#33-完整选项表)
  - [3.4 Go 类型推断规则](#34-go-类型推断规则)
  - [3.5 自定义命名 scalar 类型](#35-自定义命名-scalar-类型)
  - [3.6 Builder 与 tag 的等价关系](#36-builder-与-tag-的等价关系)
- [4. Custom 字段](#4-custom-字段)
  - [4.1 CustomValuer / CustomScanner](#41-customvaluer--customscanner)
  - [4.2 为什么不需要 Clone](#42-为什么不需要-clone)
  - [4.3 新增 Custom 字段：业务 Scanner 需处理 nil / 空 bytes](#43-新增-custom-字段业务-scanner-需处理-nil--空-bytes)
- [5. 演进规则](#5-演进规则)
  - [5.1 允许的演进](#51-允许的演进)
  - [5.2 禁止的演进](#52-禁止的演进)
  - [5.3 Default 为什么不能改](#53-default-为什么不能改)
  - [5.4 双道 compat 检查](#54-双道-compat-检查)
  - [5.5 跨版本 Num 复用](#55-跨版本-num-复用)
  - [5.6 代码回滚：alive shrinkage 防御](#56-代码回滚alive-shrinkage-防御)
- [6. Schema 的缓存与传播](#6-schema-的缓存与传播)
  - [6.1 Master：代码即真相](#61-master代码即真相)
  - [6.2 Schema 缓存与传播](#62-schema-缓存与传播)
- [7. 边界情况与防御](#7-边界情况与防御)

---

## 1. 总览：三层模型

flash-store 的数据有三个存在层次：

```
  业务 struct（代码里的 *Order）
         │
         │  Extract                         Fill
         │  （Def[T].Extract）              （Def[T].Fill）
         ▼                                     ▲
  schema.Row（num-addressed 字段集合）    ────┘
         │
         │  EncodeRow（自描述二进制 v2）
         ▼
  WAL / snapshot 字节流
```

- **Def[T]**：`registry.Def[T]` 持有 `schema.FieldDescriptor` 列表 + `Binder[T]`（getter/setter 闭包）+ `byNum` 映射，是"*T ↔ Row"的桥。每个 Kind 注册一份。
- **Row**：内存里每条对象数据。num-addressed 的 `[]Field`，无序，每个 Field 带 `Num` 作为稳定身份。
- **Row bytes**：自描述字节序列，WAL / snapshot 都按这种形态在网络 / 磁盘上流转。

关键设计特性：
- **Num 是字段身份**：Go struct 字段顺序、schema 声明顺序、wire 编码顺序三者解耦；业务可以随便重排声明、rename、drop、reorder，只要 Num 保持稳定就是向后兼容演进
- **Row 自描述**：每个 Field 带 Num + typeTag，slave（不注册业务 struct）也能正确解码、透传
- **Default 只在代码里**：不存 Row，Fill 时按当前 Def 的 spec 补；Default **不允许改**（见 [§5.3](#53-default-为什么不能改)）
- **Extract 值拷贝**：Row 是独立副本，业务对象修改不影响存储

---

## 2. Row / FieldValue 机制

### 2.1 Row 的 num-addressed 结构

```go
// schema/row.go
type Row struct {
    Version uint32    // 写入时 Def[T].Version；Fill 按 writer 版本查 schema 做映射
    Fields  []Field   // 无序，按 Num 寻址
}

type Field struct {
    Num   int32       // 稳定身份；由业务通过 `num=<int>` tag 显式分配
    Value FieldValue  // tagged-union 值
}
```

`Version` 不参与兼容性判断（Def 的演进兼容性由 [CheckSchemaCompat](#54-双道-compat-检查) 判），但它记录了写入时刻的 schema 版本号，Fill 按 writer 版本查 store 多版本缓存做 Num→reader pos 映射。

**Fields 无序**：Extract 产出的顺序沿 Def 声明顺序，Decode 产出的顺序沿 wire 编码顺序，调用方一律应按 Num 访问——`Row.GetByNum(num)` 线性扫描；热路径 Fill 走 `Def.byNum` 预建表（O(1)）。

### 2.2 FieldValue tagged union

FieldValue **不**是 `any` — 而是 tagged union，每种 Go 类型有独立内联槽位：

```go
// schema/field.go
type FieldValue struct {
    fieldType FieldType

    i64 int64            // Int32 / Int64 / Timestamp（UnixNano；0 = 零值）
    s   string           // String 专用
    d   decimal.Decimal  // Decimal 专用；{*big.Int, int32}，~16 字节
    c   []byte           // Custom 专用；Valuer 产出的字节
}
```

**为什么不用 `any`**：scalar 装箱成 interface 每次都堆分配。Tagged union 把所有类型内联到一个 ~72 字节结构体，按 `fieldType` 分派。Int32/Int64/Decimal/Timestamp 的热写路径**零分配**。

**Timestamp 语义**：`time.Time` 原本 24 字节带 monotonic 位 + Location，Row 里归一化为 UnixNano（UTC，丢 monotonic）。`time.Time{}` 零值在 i64 中以 `0` 表示——本系统中 unix epoch (1970-01-01) 不是合法业务时间，"未设置" 与 "epoch" 折叠到同一值是安全的，且 `FieldValue` 的 Go 零值天然就是 zero time。`Time()` 读到 `i64 == 0` 时返回 `time.Time{}`（IsZero=true）。**注意**：显式构造的 `time.Unix(0, 0)` 经 Row 往返后会被读回为 `time.Time{}`——业务侧若要表达"已设置但等于 epoch"应使用 `time.Unix(0, 1)` 之类避开该值（实际不会发生）。

读访问走 typed methods：

```go
v.Int32() (int32, error)    // 也接受 FieldTypeInt64 stored（范围内；越界 ErrIntOverflow）
v.Int64() (int64, error)    // 也接受 FieldTypeInt32 stored（widening，无损）
v.String() (string, error)
v.Decimal() (decimal.Decimal, error)
v.Time() (time.Time, error)
v.CustomBytes() ([]byte, error)  // 返回 Valuer 产出的 raw bytes
```

### 2.3 支持的 FieldType

| FieldType | Go 类型 | Indexable | 备注 |
|---|---|---|---|
| `FieldTypeInt32` | `int32` 及 `~int32` 自定义类型 | ✅ | iota 枚举常用（须 `type=int32` tag） |
| `FieldTypeInt64` | `int64` / `int` 及 `~int64` / `~int` | ✅ | int 按平台宽度分派 |
| `FieldTypeString` | `string` 及 `~string` | ✅ | |
| `FieldTypeDecimal` | `decimal.Decimal` | ✅ | `decimal.NewFromString` 精度保留 |
| `FieldTypeTimestamp` | `time.Time` | ✅ | 归一化为 UTC UnixNano |
| `FieldTypeCustom` | 任意实现 Valuer/Scanner 的类型 | ❌ | 参见 [§4](#4-custom-字段) |
| `FieldTypeUndefined` | — | ❌ | 零值哨兵，表示"Fill 应走 Default" |

### 2.4 Row wire format v2

`schema.EncodeRow(row)` 产出的字节流，所有 slave 和 snapshot 都用同一种格式：

```
  Row     = [format_ver: 1 byte = 0x02]
            [schema_ver: uvarint32]
            [nFields: uvarint32]
            field * nFields

  Field   = [num: uvarint32]
            [typeTag: 1 byte]
            [payload]

  typeTag 按 FieldType iota 顺序（0=Undefined, 1=Int32, ...）：
    Undefined  → 无 payload
    Int32      → svarint (zigzag int32)
    Int64      → svarint (zigzag int64)
    String     → [len: uvarint32] [utf8 bytes]
    Decimal    → [len: uvarint32] [decimal 规范字符串 bytes]
    Timestamp  → svarint (unixNano; 0 = 零值)
    Custom     → [len: uvarint32] [Valuer 产出的 bytes]
```

`[format_ver]` 前缀让 Decode 能拒掉老 v1 wire（无 format byte、按位置对齐）；当前仅有 `0x02` 一种合法值，不兼容老格式。

**自描述 = slave 友好 + Num 驱动**：每个 Field 带 Num + typeTag，slave 不注册业务 struct 也能原样解码 Row 并透传到 snapshot；接收端完全按 Num 识别字段，不依赖外部 schema 对齐位置。

在 WAL / snapshot 里：
- **proto 格式**：`wal.CmdPb.row bytes` / `snapshot.ObjectEntryPb.row bytes` 直接存这份字节
- **JSON 格式**：作为 base64 字符串嵌入（无 Row JSON 格式——自描述已经够紧凑，单一编码简化维护）

---

## 3. tag grammar

### 3.1 语法

```
store:"<name>,num=<int>[,<opt>[=<val>]...]"
```

- 无 `store` tag 或 `store:"-"` → 跳过该字段
- `<name>` 是 schema 中的字段名（**必填**）——和 Go 字段名可以不同，snake_case 是 flash-store 的约定
- `num=<int>` 是字段稳定身份（**必填**，`int32` 范围且 > 0）

### 3.2 Num：字段身份的稳定锚

Num 是字段的唯一不可变身份。它解耦：

- Go struct 字段**声明顺序** ⟂ Num（任意调整声明顺序不影响 wire）
- **字段名**（Name）⟂ Num（rename 只改可读标签，Num 不变即兼容）
- **wire 位置** ⟂ Num（Row.Fields 无序）

**分配策略**：

- 新字段挑一个**从未使用过**的 int；通常业务按历史递增（1, 2, 3, ...），也可跳号（5, 7, 12）
- 删除字段后其 Num 不可复用给不同语义的新字段——compat check 跨历史版本强制这一点（见 [§5.5](#55-跨版本-num-复用)）
- Num 写进 wire 和 FieldDescriptor；不会出现在业务读路径的错误信息里（用户看的是 Name）

**示例**：重构 struct 字段顺序仍然兼容

```go
// v1
type Order struct {
    OrderID string `store:"order_id,num=1"`
    Price   decimal.Decimal `store:"price,num=2,index,prec=8"`
}

// v2：把字段顺序调换 + rename OrderID，完全兼容 v1 存下的 Row
type Order struct {
    Price decimal.Decimal `store:"price,num=2,index,prec=8"`
    ID    string          `store:"id,num=1"` // rename: order_id → id，Num=1 不变
}
```

**管理 Num + 删除字段**：Num 一旦分配不能再给别的字段用（即使删掉）。store 对这个约束的强制分两层：

1. **语义保护（强制）**：启动期 `checkSchemasCompat` 对 cache tombstone 做对账——新 alive 字段的 Num 如果撞上历史 tombstone 里不同 Type/Default 的条目 → loud-fail。这是**数据正确性**底线，任何时候都生效。tombstone 由 Store 在运行期累积维护（"alive 缩水 → tombstone"的 diff 规则 + snapshot Header restore 的全量载入），不依赖源码声明
2. **代码可读性 / 同包防撞（推荐，非强制）**：业务在 `RegisterStruct` / Builder 里显式写 `schema.RetiredField{...}` 声明历史退休 Num。加了的好处：
   - reviewer 在 diff 里能看见"-Status 字段 + +Retired{Num:2}" 对称对账
   - 同包内若有人加新字段撞已退休 Num，Builder 注册期（包 init）立刻 panic——不等部署

`RetiredField` 不参与 `KindDescriptor` 的导出，也不参与跨进程 / 跨重启的状态传递。忘写不会让 (1) 失效——只是丢掉 (2) 的"包 init 期撞 Num 立刻 panic"这层防御；跨重启的 Num 复用拦截依然由 (1) 兜底。

```go
type Order struct {
    OrderID   string          `store:"order_id,num=1"`
    Price     decimal.Decimal `store:"price,num=2,index,prec=8"`
    // Num=3 曾是 "region" (string, default=""), v5 移除
    Status    int32           `store:"status,num=4,type=int32"`
    CreatedAt time.Time       `store:"created_at,num=5"`
}

func init() {
    orderKind, _ = registry.NewKind("future", "order")
    _, err := registry.RegisterStruct[Order](orderKind,
        schema.RetiredField{Num: 3, Name: "region",
            Type: schema.FieldTypeString, Default: schema.NewString("")},
    )
    if err != nil { panic(err) }
}
```

如果同包 PR 里有人不知情地加了 `num=3` 的新字段，包 init 直接 panic，不等部署。如果业务删字段时忘了加 `RetiredField{}` 声明，启动不会失败——`checkSchemasCompat` 仍会通过 store 端累积的 tombstone 拦截"复用 Num=3 当不同语义字段"，只是少了同包内 init 期那道捷径。

Builder API 等价写法：

```go
def := registry.For[Order](kind).
    Int64(1, "order_id", ...).
    Decimal(2, "price", ..., schema.Indexed(), schema.DecimalPrec(8)).
    Retire(schema.RetiredField{Num: 3, Name: "region",
        Type: schema.FieldTypeString, Default: schema.NewString("")}).
    Int32(4, "status", ...).
    Timestamp(5, "created_at", ...).
    Build()
```

**为什么要求 `Name`**：仅诊断用（错误信息、review diff 可读性）——schema 身份只认 Num，改名字不影响 compat。

**为什么要求 `Type` + `Default`**：Builder 在同包内 `Retire(rf)` 多次声明同 Num 时用它做幂等校验（"两次声明的 Type/Default 必须一致"，否则视作业务笔误立即 panic）。Store 端 tombstone 的语义锚由运行期累积自洽维护，不读取这些字段。

### 3.3 完整选项表

| 选项 | 适用 FieldType | 必填？ | 说明 |
|---|---|---|---|
| `num=<int>` | 所有 | ✅ | 字段稳定身份（>0），[§3.2](#32-num字段身份的稳定锚) |
| `index` | 所有可 index 类型 | 可选 | 该字段进索引，供 `IndexHint` 查询 |
| `type=<name>` | — | 可选 | 显式指定 FieldType；允许 `int32/int64/string/decimal/timestamp/custom`（`custom` 等价于下面的 `custom` 速记） |
| `custom` | — | 可选 | 强制 Custom 类型（`type=custom` 的速记） |
| `prec=<N>` | Decimal | 可选 | Extract 时按 N 位小数 Truncate，落盘的 Row 规范化；Def 演进期间精度可改 |
| `max=<N>` | String | 可选 | 最大字符长度；**当前仅作声明性元数据**，store 不主动截断 |
| `default=<literal>` | 所有 scalar（不含 Custom） | 可选 | 字段的默认值——schema 演进、业务对象缺字段时 Fill 用；**不允许改**（见 [§5.3](#53-default-为什么不能改)） |

`default` 字面量的解析：
- `Int32/Int64`：十进制整数（`default=1`）
- `String`：字面量（`default=open`；空值可写 `default=`）
- `Decimal`：十进制小数字符串（`default=0.1`）
- `Timestamp`：RFC3339Nano，或字面 `zero` 代表 `time.Time{}`
- `Custom`：**不支持通过 tag 设 Default**。默认值一律为 `[]byte(nil)`；新增 Custom 字段时业务 `Scan` 必须能处理 nil / 空 bytes（见 [§4.3](#43-新增-custom-字段业务-scanner-需处理-nil--空-bytes)）

### 3.4 Go 类型推断规则

`resolveFieldType` 的决策顺序：

1. `tag.type=...` 显式声明 → 直接用
2. `decimal.Decimal` → Decimal；`time.Time` → Timestamp
3. Go 预声明基本类型：`int32` / `int64` / `int` / `string` → 对应 FieldType
4. 其他 struct/字面类型：`*F` 若实现 `CustomValuer + CustomScanner` 接口 → Custom
5. 以上都不匹配 → 注册期报错，提示加 `type=` 或添加 Valuer/Scanner

### 3.5 自定义命名 scalar 类型

`type OrderStatus int32` 这类**自定义命名 scalar** 被强制要求 `type=...`，否则注册期 loud-fail：

```go
type OrderStatus int32
type Order struct {
    Status OrderStatus `store:"status,num=1,index,type=int32"`  // ✅ 必须显式
    // Status OrderStatus `store:"status,num=1,index"`          // ❌ 注册期报错
}
```

**为什么强制**：防止业务日后把 underlying 类型改了（例如 `type OrderStatus int32` → `type OrderStatus int64`）：
- 只靠自动推断，schema 的 FieldType 会跟着从 Int32 变成 Int64，wire 格式静默变化
- 强制 `type=int32` 之后，若 Go 代码改成 `~int64` 但 tag 还是 `type=int32`，注册期就会因"宽度不匹配"报错，强制业务明确意图

内置基本类型（`int32`/`int64`/`int`/`string`/`decimal.Decimal`/`time.Time`）不强制 `type=`——它们的类型是 Go 语言锚定的，不会被业务"悄悄改"。

### 3.6 Builder 与 tag 的等价关系

两条注册路径——`registry.RegisterStruct[T](kind)`（tag-driven）和 `registry.For[T](kind).Int32(...)...`（Builder 链）——表达能力**完全等价**，用哪条主要看业务喜好。对应关系：

| 概念 | tag 写法 | Builder 写法 |
|---|---|---|
| 字段身份 | `num=<int>` 选项 | 方法第一个参数 `num int32` |
| 字段名 | tag 第一个位置参数 | 方法第二个参数 `name schema.FieldName` |
| 字段类型 | Go 类型 + `type=<name>` 按需覆盖 | 方法名（`Int32` / `Int64` / `String` / `Decimal` / `Timestamp` / `CustomField`）|
| Indexed | `index` | `schema.Indexed()` |
| Decimal 精度 | `prec=<N>` | `schema.DecimalPrec(N)` |
| String 最大长度 | `max=<N>` | `schema.StringMax(N)` |
| 默认值 | `default=<literal>`（按 FieldType 字面量解析） | `schema.Default(v any)`（接受 Go 值，如 `decimal.NewFromInt(5)`、`time.Time{}`） |
| 跳过字段 | `store:"-"` 或无 tag | 不调相应 Builder 方法 |
| Custom 字段 | `*F` 实现 Valuer/Scanner 自动识别；或 `custom` / `type=custom` | `registry.CustomField[T, F](b, num, name, getter)` |
| 声明已删字段 | `RegisterStruct[T](kind, schema.RetiredField{...}, ...)` 可变参数 | `Builder.Retire(schema.RetiredField{...})` 链式方法 |

唯一的能力差距：**tag 无法给 Custom 字段设 Default**（因为 tag 只能写字面量，Custom Default 是 `[]byte`）——需要 Default 的 Custom 字段只能走 Builder 配合 `schema.Default([]byte{...})`。大多数场景业务应在 Scanner 里处理 nil/空 bytes（见 [§4.3](#43-新增-custom-字段业务-scanner-需处理-nil--空-bytes)），比自定义 bytes default 更干净。

Builder 里字段常数隐藏在方法参数中，对 code review 更友好；tag 里字段声明紧贴 struct 字段，对业务方日常改动更方便。两条路径 Def 产物完全一致，shard/index/snapshot 行为无差别。

---

## 4. Custom 字段

### 4.1 CustomValuer / CustomScanner

对 Valuer/Scanner 的约定对齐 `database/sql` 的 `driver.Valuer` + `sql.Scanner`，只不过统一到 `[]byte`：

```go
// schema/spec.go
type CustomValuer interface { Value() ([]byte, error) }
type CustomScanner interface { Scan(data []byte) error }
```

实现必须挂在 `*F` 上（Scan 要写入自身，需要指针接收者）：

```go
type OrderExtra struct {
    Fee  decimal.Decimal `json:"fee"`
    Tags []string        `json:"tags"`
}

func (e *OrderExtra) Value() ([]byte, error) { return json.Marshal(e) }
func (e *OrderExtra) Scan(b []byte) error    {
    if len(b) == 0 { *e = OrderExtra{}; return nil }
    return json.Unmarshal(b, e)
}
```

序列化可以用 JSON、protobuf、MessagePack、gob...随便。对 store 来说 Custom 字段就是不透明字节。

### 4.2 为什么不需要 Clone

Row 里 Custom 字段**只存 Valuer 产出的 bytes**，不持有业务对象引用：

- Extract 时调 `pf.Value()` 立刻序列化成 bytes → Row 存这份字节
- Fill 时从 bytes 调 `pf.Scan()` 解回新建的 Go 对象
- 业务之后怎么改自己原来的对象都不影响存储——bytes 不可变、Scan 每次造新对象

这和 SQL 驱动的语义完全一致。业务**不需要**实现 Clone 方法。缺点是 Get 时要走一次 Scan（反序列化）；Custom 字段通常是低频访问字段，这点开销可接受。

### 4.3 新增 Custom 字段：业务 Scanner 需处理 nil / 空 bytes

Custom 字段**不支持通过 tag 设 Default**（tag 字面量无法表达 `[]byte`），也就是说：

- tag-driven 注册的 Custom 字段，Default 一律是 `[]byte(nil)`（`ZeroDefault(FieldTypeCustom)` 的产物）
- Schema 演进新增 Custom 字段时，历史 Row 没有这个 Num；Fill 走 Default → `Scan(nil)` 调到业务
- 即使走 Builder API 显式 `schema.Default([]byte{...})`，也只是把 default payload 换成固定 bytes，并不等价于"有意义的对象默认值"

**业务 Scanner 必须能正确处理 `nil` / `len==0` 的输入**：

```go
func (e *OrderExtra) Scan(b []byte) error {
    if len(b) == 0 {
        *e = OrderExtra{} // 清零，表示"没有数据"
        return nil
    }
    return json.Unmarshal(b, e)
}
```

不这样写的话，新字段落地那一刻、替换到 reader 的所有历史 Row 都会让 Fill 抛 unmarshal error——整个 Kind 的读路径集体崩。

写法建议：
- **推荐**：Scanner 里把 nil / empty 映射为 Go 零值对象（上例的 `*e = OrderExtra{}`），业务代码读 Extra 时按"业务零值"理解
- 若业务**真的需要**区分"没有"和"空对象"，加一个辅助字段（如 `has_extra bool`，作为独立 Num 存在 schema 里）——而不是试图在 Custom payload 里用一种特殊字节串表示"存在但空"

大多数场景走推荐写法，比维护带 default bytes 的 Custom 字段干净得多。

---

## 5. 演进规则

Schema 在**master 重启**时才能变化（Go 代码就是真相源，不能热更）。每次 master 启动都走 `restoreFromSnapshot` → `checkSchemasCompat`（对比 snapshot 里记录的历史 schema 和当前代码的新 schema）。compat 检查会 loud-fail 不合法的演进。

### 5.1 允许的演进

Num-身份设计下，常见演进全部兼容：

| 演进类型 | Num 语义 | 原因 / 操作 |
|---|---|---|
| **新增字段** | 分配新 Num | 老 Row 里没这个 Num，Fill 用 Default；打开 index 的新字段同理 |
| **删除字段** | Num 在 alive 里消失、自动进 tombstone | 历史 Row 里该 Num 的值被 Fill 丢弃。**推荐：删 struct 字段 + 同包加 `schema.RetiredField{...}`** 让同包内撞 Num 的新字段在 Builder 注册期（包 init）立刻 panic；非必需（不加也能正常启动），跨进程的 Num 复用拦截由 store 端 tombstone 兜底。见 [§3.2](#32-num字段身份的稳定锚) |
| **复活已删字段** | 从 tombstone 搬回 alive，自动 | 只要新声明的 Num/Type/Default 与历史完全一致 → 下次 UpsertSchema 自动消解 tombstone 项；不同 Type/Default 会拒绝 |
| **rename 字段** | Num 不变，Name 变 | 索引按 Name 建表，重新按新名建即可；`RowView.Get("old_name")` 要改用新名 |
| **reorder 字段**（声明顺序 / schema 位置） | Num 不变 | Row.Fields 无序，位置无语义——重排不影响 wire |
| **改 DecimalPrecision / StringMaxLen** | Num 不变 | 只影响 Extract 时的 normalize（DecimalPrec truncate）和新写入的约束；已入库的 Row 不变 |
| **改 Indexed 标志** | Num 不变 | 索引在 restore 时按当前 schema 重建；打开 index → 历史 Row 按 Default/实际值 enrol；关闭 index → 不再增删 |
| **改 Version**（自动） | 业务不管 | Version 由 store 启动期自动推导；结构等价沿用，演进递增 |

### 5.2 禁止的演进

在 `CheckSchemaCompat` 里 loud-fail，master 启动 restore 失败：

| 违反 | 为什么 |
|---|---|
| **同 Num 改 Type**（int32→int64 / int32→string / decimal→int64 …） | 老 Row 里该 Num 存的是旧类型，新 schema 读出来会类型错配→索引腐败 / Fill 失败 |
| **同 Num 改 Default** | Default 不存 Row，改了会让历史 Row 跨重启漂移（详见 [§5.3](#53-default-为什么不能改)） |
| **同一 schema 内 Num 重复** | 注册期 bug（Builder 已经在 `addField` 里拦截） |
| **跨版本复用被删除的 Num** | Num 是永久身份，删过的 Num 不能作为不同语义的字段重新出现（详见 [§5.5](#55-跨版本-num-复用)） |

如果业务"确实需要"这些改动，**正确做法是加新字段**（分配新 Num）：

```go
// ❌ 想把 status 从 int32 改成 string
type Order struct { Status string `store:"status,num=2,index"` }  // compat 拒绝

// ✅ 加新字段，老 status 保留但标记为废弃
type Order struct {
    Status    int32  `store:"status,num=2,index,type=int32"`        // 保留
    StatusNew string `store:"status_new,num=10,index,default="`      // 新字段 + 新 Num
}
// 业务代码读新字段，后续数据迁移逻辑（批量重写）另做
```

### 5.3 Default 为什么不能改

设计里最容易反直觉的一条。场景示例：

```
t=0  Master v1，schema = [id, status]，写 Order{id=1, status=1}
     → Row 带 Num=1,2 两个 Field（2 fields）
t=1  SnapshotSlave 打 snapshot，Row 是 2 fields
t=2  Master 升级到 v2，加字段 note (Num=3)，default="a"
t=3  Listener 处理历史 Order，Fill 得到 note="a"（取自当前 Def.Default）
     → 下游收到 note="a"
t=4  Master 再次重启，代码里 default 改成 "b"
t=5  Master restore snapshot（t=1 的，2 fields）+ replay WAL
t=6  Listener 重放同一条历史 Order，Fill 得到 note="b"
     → 下游**对同一条 record** 看到 note 从 "a" 变成 "b" ⚠
```

根因：**Default 不存 Row**，每次 Fill / `indexedFieldValue` 都从当前 Def 取。改 Default 会**回溯到所有历史 Row**，下游跨重启失去一致性。

解决：把 Default 当作"承诺"。`FieldDescriptor` 里持久化 Default，`CheckSchemaCompat` 对同 Num 字段的 Default 做严格等值比较。

业务想改 Default 背后的语义？加新字段。比改 Default 显式得多，也不会让下游遭殃。

### 5.4 双道 compat 检查

```
  Master 启动:
    restoreFromSnapshot
      → header.Schemas （snapshot 里记的**全部历史版本**）
      → afterReplayStarted: checkSchemasCompat(registry.AllDescriptors())
      → 遍历每个 Kind 的历史版本，逐一 CheckSchemaCompat(old, current)
      → 不过则 abort startup ← 第一道防线

  Slave 运行中:
    replay 读到 WalTypeSchemaConfig
      → master 广播的新版本 schema
      → validateSchemaConfig: 对本地历史缓存**每个版本**都 CheckSchemaCompat
      → 不过则 panic ← 第二道防线
```

两道都调用同一个 `schema.CheckSchemaCompat`，同一套规则，**都遍历全部历史版本**而不是只比最新一跳。

**为什么两道**：
- snapshot 上的检查覆盖 "master 自己重启升级" 场景
- broadcast 上的检查覆盖 "master 运行期把 broadcast 内容改坏" 场景（比如调错了部署顺序）+ slave 没有代码 Def 可对比时唯一的防线

### 5.5 跨版本 Num 复用

Num 是**永久身份**。考虑下面场景：

```
v1:  fields = { Num=1 id(int64), Num=5 flag(int32) }
v2:  fields = { Num=1 id(int64) }                      // 删除 Num=5，允许
v3:  fields = { Num=1 id(int64), Num=5 label(string) } // 把 Num=5 重新用作 string
```

单纯看相邻 `(v2, v3)`：v2 里没 Num=5，v3 里 Num=5 是新字段 → 看起来像"新加字段"，简单 compat 会允许。

但这是错的：**历史 Row 里可能有 v1 时期写的 Num=5 int32 值**（snapshot 存着、或者 WAL 里还有）。用 v3 的 Def 去 Fill 这样的 Row，会在读 Num=5 时遇到 `int32` 存、`string` 读——ErrTypeMismatch，索引也会 drift。

**解决：tombstone**。store 对每个 Kind 维护 `alive + tombstone` 两个字段集合：

- `alive`：当前 latest schema 里存在的字段（Num → FieldDescriptor）
- `tombstone`：曾经存在、现已被删除的字段——Num 及最后一次的 Type/Default

字段从 alive 退出时直接进 tombstone；compat 检查对比新 schema 的每个 Num：
- 在 `alive` 里 → 同 Num 不能改 Type/Default
- 在 `tombstone` 里 → 若要复活 Num，Type/Default 必须完全一致（允许"同语义复活"，禁止"改语义复用"）

上例 v3 的 Num=5 string 对比 tombstone 里 Num=5 int32 → **Type 不匹配，拒绝**。

Tombstone 的持久化：snapshot Header 的 `KindDescriptor.DroppedFields` 字段携带 tombstone，restore 时重建。Master 广播只发 alive，slave / master 各自维护 tombstone 增量。详见 [§6.2](#62-schema-缓存与传播)。

### 5.6 代码回滚：行为与数据后果

时间线：v1 代码上线 schema={A,B}，v2 上线加字段 C，v2 有 bug 想回滚到 v1。

**回滚 v1 启动时发生什么**：

- 启动时 store 缓存 `alive={A,B,C}`（从 v2 时期 broadcast 累积）
- v1 代码的 Def 里只有 {A, B}，既没 C 也没 `Retire{C}`
- compat 检查放行——没有 Type 冲突、没有 Num 复用
- `UpsertSchema` 自动合并：`alive={A,B}`，C 悄悄迁到 `tombstone`
- PublishSchemas 把 {A,B} 发给 slaves；slaves 同样更新

**v1 运行期间数据影响**：

- 读：v1 的 Fill 只看 Num=A/B，Row 里带的 C 值（历史遗留）被静默忽略。业务层看不到 C
- 写：v1 的 Extract 不输出 Num=C —— 新 Row 里**不带** C
- **未被 v1 改过的记录**：shard / snapshot / WAL 里的 Row 仍完整保留 C 值
- **被 v1 更新过的记录**：新 Row 覆盖了旧 Row，Num=C 永久丢失

**Roll forward 回 v4（C 复活，同 Type/Default）**：

- compat 检查：新 `alive` 把 Num=C 加回来。C 在 tombstone 中找到同 Type/Default → **允许复活**
- `UpsertSchema` 自动把 Num=C 从 tombstone 移回 alive（无需手动操作）
- 读：v4 Fill 回填 C 字段；**未被 v1 改过的记录**恢复完整 C 值；**v1 改过的记录**里 Num=C 不存在，Fill 走 Default
- 写：v4 Extract 恢复输出 Num=C

**Roll forward 到 v4（C 永久废弃）**：

- v4 代码不带 C 字段；加不加 `Retire{C}` 都行：
  - 加了：源码里明示 Num=C 永久占用；Builder 里任何撞 C 的新字段 init-time panic
  - 不加：store 内部 tombstone 里仍有 C（snapshot header 持久化），防 Num 复用的保护不变
- 无论哪种，v4 启动正常

**关于 version 数字的"跳号"**：cache `latestVersion` 是 v2 的 2，v1 启动 resolveSchemaVersions 算出 3；再 roll forward 到 v4 算出 4。Version 只做诊断不参与 compat，数字跳号不影响正确性。

**总结**：

| 问题 | 答案 |
|---|---|
| 回滚会被阻止吗？ | 不会。compat 只拦截"语义不一致"（Type/Default 改变），"代码不知情"不算 |
| 回滚会丢数据吗？ | 只丢"回滚期间被 v1 更新"的记录的 C 字段；其他记录完整保留 |
| 复活已删字段要改什么？ | 代码里加回字段（同 Num/Type/Default）——store 自动从 tombstone 移回 alive |
| `Retire{}` 声明何时必要？ | 不必要；推荐写上作为源码可读性 + Builder 撞 Num 时 init-time 检测 |

---

## 6. Schema 的缓存与传播

### 6.1 Master：代码即真相

Master 进程里业务代码调 `registry.RegisterStruct[T](kind)`：
- 每个 Kind 的 schema 定义在代码里（tag 或 Builder）
- 启动期 `afterReplayStarted` 里走 `resolveSchemaVersions`：
  - 没有历史缓存（全新 Kind）→ `Def.Version = 1`
  - 有历史 alive 且当前代码结构等价 → 沿用 `latestVersion`
  - 有历史 alive 且当前代码结构变了（兼容演进）→ `latestVersion + 1`
- 然后跑 `checkSchemasCompat`（对比 alive + tombstone）
- 最后 `PublishSchemas` 写 WAL + `UpsertSchema` 更新自己的 store 缓存

Master 永远把代码注册当作真相源；tombstone 由 store 缓存自动管理（按最新广播里消失的 Num 自动归档）。

### 6.2 Schema 缓存与传播

`Store.schemasByKind map[Kind]*fieldsRegistry` 是每个 Kind 一个登记簿：

```go
type fieldsRegistry struct {
    alive         map[int32]FieldDescriptor  // 当前在用
    tombstone     map[int32]FieldDescriptor  // 曾在用、现已删除
    latestVersion uint32
}
```

**SnapshotSlave 不注册业务 struct**（不 import 业务代码）。它对 schema 的认知只能来自 master：

```
Master.PublishSchemas（只发 alive）
  → WAL: WalTypeSchemaConfig, payload = []KindDescriptor (DroppedFields 恒空)

Slave.replayApplier.Apply(WalTypeSchemaConfig)
  → validateSchemaConfig（vs 本地 alive + tombstone）
  → for each desc: store.UpsertSchema(desc)
      · desc.Fields 里的 Num → 写入 alive
      · 先前 alive 里有、desc 里没有的 Num → 自动迁到 tombstone
```

**Tombstone 由两条途径累积**，都不依赖业务源码声明 `RetiredField`：

- **运行期 diff**：每次 `UpsertSchema` 把"老 alive 里在新 alive 中消失的 Num"自动归 tombstone。这条对**已有 prior alive 的进程**（slave 持续在线、master 重启）总是有效——后续 broadcast 的 alive 缩水会被自动捕获
- **Snapshot Header 全量载入**：`Header.Schemas` 由 `KnownSchemas()` 导出，含 Store 当前 `alive + tombstone`。restore 端 `UpsertSchema(desc)` 会把 `desc.DroppedFields` 完整并入 tombstone——让 fresh 节点（无 prior 状态）也能拿到完整 Num 历史

注意：`schema.KindDescriptor.DroppedFields` 字段同时被两条传播链路使用，但**只有 snapshot Header 这条非空**——broadcast 路径下 `DescriptorOf` 不再读取业务的 `RetiredField` 声明。`RetiredField` 退化为纯源码层工具（同包 init 期撞 Num 防御 + diff 可读性），不参与 Store 的状态搬运。

**为什么 alive+tombstone 比"存每版本 KindDescriptor"更好**：
- Fill 热路径不读它，只 compat 用——对 compat 来说 Num 的历史"最后一次 Type/Default"就够，不需要每个中间版本
- 空间从 Σ(V·F) 降到 Σ(F + D)，长期运行 + 多次演进的老 store 尤其明显
- Compat 检查从 O(V·F) 降到 O(F + D)

Snapshot Header 始终以 JSON 存储（不走 Codec），`FieldValue` 有自定义 `MarshalJSON/UnmarshalJSON`，`FieldType` 作为可读字符串（`"int32"` 等）而非 iota 数字——未来 FieldType 重编号不破坏历史 snapshot。

---

## 7. 边界情况与防御

| 场景 | 行为 | 位置 |
|---|---|---|
| Row 里缺某个 Num（老版本 Row） | Fill 对该 Num 在 reader 端对应位置用 Default；shard 索引同理——对称的增删索引 | [shard.go](../store/shard.go) 的 `indexedFieldValue` + [def.go](../store/registry/def.go) 的 `Fill` |
| Row 里某 Num 的 FieldType 和当前 schema 声明的不同（代码改错未迁移） | `indexedFieldValue` 立即 panic，消息含字段名、Num、两种类型 | 同上 |
| Int64 存储值读成 int32，但超出范围 | `FieldValue.Int32()` 返回 `ErrIntOverflow`，Fill loud-fail | [schema/field.go](../store/schema/field.go) |
| Int32 存储值读成 int64 | widening 无损，成功 | 同上 |
| Custom 字段带 `indexed` tag | `Builder.Build()` 注册期 panic | [registry/def.go](../store/registry/def.go) |
| Custom 字段 Valuer 返回 error | Extract 时吞掉 error，写入零长度 payload（避免整条 WAL 崩）；业务可通过观察 Row bytes 长度排查 | [registry/def.go](../store/registry/def.go) `CustomField` |
| 自定义命名 scalar 类型（如 `type Status int32`）无 `type=` tag | 注册期报错，错误信息提示加 `type=int32` | [registry/register_struct.go](../store/registry/register_struct.go) |
| 缺 `num=<int>` tag | 注册期报错，错误信息提示 "missing required \`num=<int>\`" | [registry/struct_tag.go](../store/registry/struct_tag.go) |
| `num=0` / 负数 / 重复 num | 注册期报错 | 同上 + [def.go](../store/registry/def.go) `addField` |
| 跨版本复用被删除的 Num（不同 Type/Default） | `checkSchemasCompat` 对比 tombstone 时 loud-fail | [snapshot_manager.go](../store/snapshot_manager.go) |
| 同包内新字段 Num 撞到 `Retired{}` 声明 | `Builder.addField` 注册期 panic，错误信息含 num + 原 Name + 原 Type | [registry/def.go](../store/registry/def.go) |
| 删除字段但没加 `Retired{}` 声明 | 允许——store 自动把消失的 alive 迁到 tombstone；`Retired{}` 仅为同包内 init 期撞 Num 的可选防御 | [snapshot_manager.go](../store/snapshot_manager.go) |
| 同 `Retired{}` 声明在同包 Builder 里两次内容不一致 | `Builder.Retire` 注册期 panic（笔误防御） | [registry/def.go](../store/registry/def.go) |
| 代码回滚（老代码不知道后加的字段） | 允许通过；回滚期间未被更新的记录保留字段值，roll forward 可恢复；被更新过的记录丢失字段数据，见 [§5.6](#56-代码回滚行为与数据后果) | 同上 |
| wire 遇到老 v1 format（无 `0x02` 前缀） | `DecodeRow` 返回 "unsupported format byte" 错误 | [schema/row_codec.go](../store/schema/row_codec.go) |
| snapshot.Header.Schemas 为空（全新 store） | 跳过 compat 检查（新 Kind 允许） | [snapshot_manager.go](../store/snapshot_manager.go) |
| snapshot 里有 Kind 但当前进程未注册 | 日志 warn，不 fail（业务可能下线某个 Kind） | 同上 |

---

## 参考

- [api.md](api.md) — 业务 API 使用（SetObject / GetObject / Listener 等）
- [architecture.md](architecture.md) — 整体架构、存储 / 主从 / 下游一致性
