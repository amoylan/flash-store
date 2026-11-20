package schema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/shopspring/decimal"
)

// FieldName 字段名称。schema / registry / wire 统一用它。
type FieldName string

type FieldType int

const (
	FieldTypeUndefined FieldType = iota
	FieldTypeInt32
	FieldTypeInt64
	FieldTypeString
	FieldTypeDecimal
	FieldTypeTimestamp

	FieldTypeCustom // 自定义类型 不可用于索引
)

// IsIndexable 判断是否可以用于索引
func (t FieldType) IsIndexable() bool {
	return t > FieldTypeUndefined && t < FieldTypeCustom
}

// String 返回 FieldType 的可读名。用于日志、错误信息、JSON 持久化；
// 与 registry 的 `type=<name>` tag 识别名一致。
func (t FieldType) String() string {
	switch t {
	case FieldTypeUndefined:
		return "undefined"
	case FieldTypeInt32:
		return "int32"
	case FieldTypeInt64:
		return "int64"
	case FieldTypeString:
		return "string"
	case FieldTypeDecimal:
		return "decimal"
	case FieldTypeTimestamp:
		return "timestamp"
	case FieldTypeCustom:
		return "custom"
	}
	return "unknown"
}

// ParseFieldTypeName 是 String 的逆操作；供 JSON UnmarshalJSON 和 tag 解析共用。
func ParseFieldTypeName(s string) (FieldType, error) {
	switch s {
	case "int32":
		return FieldTypeInt32, nil
	case "int64":
		return FieldTypeInt64, nil
	case "string":
		return FieldTypeString, nil
	case "decimal":
		return FieldTypeDecimal, nil
	case "timestamp":
		return FieldTypeTimestamp, nil
	case "custom":
		return FieldTypeCustom, nil
	case "undefined", "":
		return FieldTypeUndefined, nil
	}
	return FieldTypeUndefined, errors.New("unknown FieldType name: " + s)
}

// MarshalJSON / UnmarshalJSON 使 FieldType 在 JSON 里以字符串形式出现——
// 比起数字 iota 值更人类可读、且不受未来 iota 重新排序影响（向后兼容）。
func (t FieldType) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.String() + `"`), nil
}

func (t *FieldType) UnmarshalJSON(data []byte) error {
	if len(data) < 2 || data[0] != '"' || data[len(data)-1] != '"' {
		return errors.New("FieldType JSON: expected quoted string, got " + string(data))
	}
	ft, err := ParseFieldTypeName(string(data[1 : len(data)-1]))
	if err != nil {
		return err
	}
	*t = ft
	return nil
}

var ErrTypeMismatch = errors.New("type mismatch")

// ErrIntOverflow 当 Int32 读取一个存储为 Int64、但值超出 int32 范围的 FieldValue 时返回。
// 触发场景：snapshot 里存的是 Int64 → tag 改成 int32 后 Fill；
//
//	若所有值都在 int32 范围内，恢复仍然无损；否则本错误让 Fill loud-fail，
//	避免静默丢高位导致数据损坏。
var ErrIntOverflow = errors.New("int value out of range")

// FieldValue 以 tagged union 形式内联所有支持的字段值——每种类型走独立字段槽，
// 不走 `any`，省掉 scalar 转接口时的堆分配（Int32/Int64/Timestamp/Decimal 的
// 常见写路径由此从"每字段一次 iface box 分配"降为零分配）。
//
// 约 72 字节/结构：fieldType(8) + i64(8) + string(16) + decimal(16) + []byte(24)。
// 不同类型共享同一个结构体但只用其中一组字段——按 fieldType 分派。
//
// 语义微调：Timestamp 以 UnixNano 存于 i64，`time.Time.Location()` 和 monotonic
// 时钟在 in-memory 路径也被归一化（之前只在 WAL/snapshot 往返时丢）。`time.Time{}`
// 在 i64 中以 0 表示——本系统中 unix epoch (1970-01-01) 不是合法业务时间，
// 因此把"未设置"和"epoch"折叠到同一个值是安全的；同时让 FieldValue 的 Go
// 零值天然表达 zero time，不需要构造器介入。
//
// 读路径通过 Int32/Int64/String/Decimal/Time/CustomBytes 方法按 fieldType 分派，
// 类型不匹配返回 ErrTypeMismatch。
type FieldValue struct {
	fieldType FieldType

	// Int32 / Int64 / Timestamp（Timestamp 以 UnixNano 存；0 表示 zero time）。
	i64 int64
	// String 字段专用。
	s string
	// Decimal 字段专用；decimal.Decimal 本身是 {*big.Int, int32}，内联能省一次 iface box。
	d decimal.Decimal
	// Custom 字段专用；存 CustomValuer 产出的 bytes，业务读取时再走 Scan 复原。
	c []byte
}

func (v FieldValue) Type() FieldType {
	return v.fieldType
}

func (v FieldValue) Int32() (int32, error) {
	switch v.fieldType {
	case FieldTypeInt32:
		return int32(v.i64), nil
	case FieldTypeInt64:
		// Narrowing 路径：用于 schema 演进时 Row 里存的是 Int64 但当前 tag 已改成 int32。
		// 范围内直接截断转换；超出则返回错误而非静默丢高位——避免数据损坏静默发生。
		if v.i64 > math.MaxInt32 || v.i64 < math.MinInt32 {
			return 0, fmt.Errorf("%w: int64 value %d out of int32 range", ErrIntOverflow, v.i64)
		}
		return int32(v.i64), nil
	}
	return 0, ErrTypeMismatch
}

func (v FieldValue) Int64() (int64, error) {
	switch v.fieldType {
	case FieldTypeInt32, FieldTypeInt64:
		return v.i64, nil
	}
	return 0, ErrTypeMismatch
}

func (v FieldValue) String() (string, error) {
	if v.fieldType != FieldTypeString {
		return "", ErrTypeMismatch
	}
	return v.s, nil
}

func (v FieldValue) Decimal() (decimal.Decimal, error) {
	if v.fieldType != FieldTypeDecimal {
		return decimal.Decimal{}, ErrTypeMismatch
	}
	return v.d, nil
}

func (v FieldValue) Time() (time.Time, error) {
	if v.fieldType != FieldTypeTimestamp {
		return time.Time{}, ErrTypeMismatch
	}
	if v.i64 == 0 {
		return time.Time{}, nil
	}
	return time.Unix(0, v.i64).UTC(), nil
}

// CustomValue 保留 any 返回类型以保持旧 API 形态；实际承载的始终是 []byte
// （或 nil）。新代码更适合直接用 CustomBytes，省掉一次 iface 包装。
func (v FieldValue) CustomValue() (any, error) {
	if v.fieldType != FieldTypeCustom {
		return nil, ErrTypeMismatch
	}
	if v.c == nil {
		return nil, nil
	}
	return v.c, nil
}

// CustomBytes 是 CustomValue 的无 any 版本，避免 []byte → any 的 iface 箱。
// 热路径（row_codec EncodeRow、CustomField Set 解码）优先用它。
func (v FieldValue) CustomBytes() ([]byte, error) {
	if v.fieldType != FieldTypeCustom {
		return nil, ErrTypeMismatch
	}
	return v.c, nil
}

func (v FieldValue) Less(ft FieldType, than FieldValue) bool {
	switch ft {
	case FieldTypeInt32, FieldTypeInt64, FieldTypeTimestamp:
		return v.i64 < than.i64
	case FieldTypeString:
		return v.s < than.s
	case FieldTypeDecimal:
		return v.d.LessThan(than.d)
	}
	return false
}

// Equal 返回 v 与 other 是否类型 + 值都相等。对所有类型都给出正确答案：
//   - Int32/Int64/Timestamp：直接比 i64
//   - String：比较字符串
//   - Decimal：decimal.Decimal.Equal（数值等价，而非字面量相同，例如 "1.0" == "1"）
//   - Custom：bytes.Equal
//   - Undefined：双方都 Undefined 则相等
//
// 注意与 OrderedEqual 的分工：OrderedEqual 靠 Less 比较器推导，Custom 上会误判；
// 本方法是自包含的强类型相等，schema compat / Default 比较等"严格"判定用它。
func (v FieldValue) Equal(other FieldValue) bool {
	if v.fieldType != other.fieldType {
		return false
	}
	switch v.fieldType {
	case FieldTypeUndefined:
		return true
	case FieldTypeInt32, FieldTypeInt64, FieldTypeTimestamp:
		return v.i64 == other.i64
	case FieldTypeString:
		return v.s == other.s
	case FieldTypeDecimal:
		return v.d.Equal(other.d)
	case FieldTypeCustom:
		return bytes.Equal(v.c, other.c)
	}
	return false
}

// OrderedEqual 通过 Less 比较器推导相等：!(v<than) && !(than<v)。
// 这是索引 B-tree 内部基于 Less 的等价判定，对 Custom 会误判（Custom 的 Less 总返回 false）。
// 索引不会把 Custom 放进树（FieldTypeCustom.IsIndexable()==false），所以这条不影响正确性。
// 为 schema-compat / Default 比较等场景，用 Equal 而不是 OrderedEqual。
func (v FieldValue) OrderedEqual(ft FieldType, than FieldValue) bool {
	less := v.Less(ft, than)
	greater := than.Less(ft, v)
	return !less && !greater
}

// MarshalJSON / UnmarshalJSON 让 FieldValue 可以直接进 JSON wire（schema
// 持久化、snapshot Header、schema 广播 payload 都走这里）。
// 格式：{"t":"<typeName>","v":<typed value>}；Undefined 省略 v。
// 数值类型用原生 JSON number；Decimal 用字符串（保精度）；Timestamp 用 RFC3339Nano；
// Custom 用 []byte（JSON 默认 base64）。
func (v FieldValue) MarshalJSON() ([]byte, error) {
	type wire struct {
		T FieldType       `json:"t"`
		V json.RawMessage `json:"v,omitempty"`
	}
	w := wire{T: v.fieldType}
	switch v.fieldType {
	case FieldTypeUndefined:
		// no payload
	case FieldTypeInt32:
		b, err := json.Marshal(int32(v.i64))
		if err != nil {
			return nil, err
		}
		w.V = b
	case FieldTypeInt64:
		b, err := json.Marshal(v.i64)
		if err != nil {
			return nil, err
		}
		w.V = b
	case FieldTypeString:
		b, err := json.Marshal(v.s)
		if err != nil {
			return nil, err
		}
		w.V = b
	case FieldTypeDecimal:
		b, err := json.Marshal(v.d.String())
		if err != nil {
			return nil, err
		}
		w.V = b
	case FieldTypeTimestamp:
		var s string
		if v.i64 == 0 {
			s = ""
		} else {
			s = time.Unix(0, v.i64).UTC().Format(time.RFC3339Nano)
		}
		b, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		w.V = b
	case FieldTypeCustom:
		b, err := json.Marshal(v.c)
		if err != nil {
			return nil, err
		}
		w.V = b
	default:
		return nil, fmt.Errorf("FieldValue MarshalJSON: unknown fieldType %v", v.fieldType)
	}
	return json.Marshal(w)
}

func (v *FieldValue) UnmarshalJSON(data []byte) error {
	var w struct {
		T FieldType       `json:"t"`
		V json.RawMessage `json:"v,omitempty"`
	}
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	switch w.T {
	case FieldTypeUndefined:
		*v = FieldValue{}
		return nil
	case FieldTypeInt32:
		var n int32
		if err := json.Unmarshal(w.V, &n); err != nil {
			return err
		}
		*v = NewInt32(n)
		return nil
	case FieldTypeInt64:
		var n int64
		if err := json.Unmarshal(w.V, &n); err != nil {
			return err
		}
		*v = NewInt64(n)
		return nil
	case FieldTypeString:
		var s string
		if err := json.Unmarshal(w.V, &s); err != nil {
			return err
		}
		*v = NewString(s)
		return nil
	case FieldTypeDecimal:
		var s string
		if err := json.Unmarshal(w.V, &s); err != nil {
			return err
		}
		// 同 row codec 的同款防御：拦截超长 payload + 解析后异常 exp，避免
		// shopspring/decimal 把形如 "0E400000001" 的输入解成 exp=4亿 的对象后续 OOM。
		if len(s) > maxDecimalStringLen {
			return fmt.Errorf("decimal payload length %d exceeds max %d", len(s), maxDecimalStringLen)
		}
		d, err := decimal.NewFromString(s)
		if err != nil {
			return fmt.Errorf("decimal parse %q: %w", s, err)
		}
		if e := d.Exponent(); e > maxDecimalAbsExp || e < -maxDecimalAbsExp {
			return fmt.Errorf("decimal exponent %d out of [-%d, %d]", e, maxDecimalAbsExp, maxDecimalAbsExp)
		}
		*v = NewDecimal(d)
		return nil
	case FieldTypeTimestamp:
		var s string
		if err := json.Unmarshal(w.V, &s); err != nil {
			return err
		}
		if s == "" {
			*v = ZeroTimestamp()
			return nil
		}
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return fmt.Errorf("timestamp parse %q: %w", s, err)
		}
		*v = NewTimestamp(t)
		return nil
	case FieldTypeCustom:
		var b []byte
		if len(w.V) > 0 {
			if err := json.Unmarshal(w.V, &b); err != nil {
				return err
			}
		}
		*v = NewCustomFieldValue(b)
		return nil
	}
	return fmt.Errorf("FieldValue UnmarshalJSON: unknown type %v", w.T)
}

// ============================================================================
// 构造函数——类型化版本位于热路径，NewFieldValue[T] 泛型形式仅作为便利 API；
// 泛型形式内部走 `any(v).(type)`，会引入一次 iface 分配，不建议热路径使用。
// ============================================================================

// NewInt32 构造 Int32 FieldValue，零分配。
func NewInt32(v int32) FieldValue {
	return FieldValue{fieldType: FieldTypeInt32, i64: int64(v)}
}

// NewInt64 构造 Int64 FieldValue，零分配。
func NewInt64(v int64) FieldValue {
	return FieldValue{fieldType: FieldTypeInt64, i64: v}
}

// NewString 构造 String FieldValue；string 头在结构体里内联，零分配。
func NewString(v string) FieldValue {
	return FieldValue{fieldType: FieldTypeString, s: v}
}

// NewDecimal 构造 Decimal FieldValue；decimal.Decimal 是 {*big.Int, int32}，
// 已经有自己的内部指针，但相比 any 箱能省一次 iface 分配。
func NewDecimal(v decimal.Decimal) FieldValue {
	return FieldValue{fieldType: FieldTypeDecimal, d: v}
}

// NewTimestamp 构造 Timestamp FieldValue。
// 内部以 UnixNano 存于 i64，zero time 用 0 表示；存储大小等同 Int64，
// 比内联 time.Time（24 字节）省 16 字节，Row 体积和拷贝成本都更小。
//
// 本系统中 unix epoch (1970-01-01 00:00:00 UTC) 不会作为合法业务时间出现，
// 因此 i64=0 同时承担 "未设置" 与 "epoch" 两种语义；显式构造的
// time.Unix(0, 0) 经过 Row 往返后会被读回为 time.Time{}（IsZero=true）。
//
// 想直接产一个 zero Timestamp FieldValue 时优先用 ZeroTimestamp()——
// 不必绕 time.Time{}，调用点意图也更清晰。
func NewTimestamp(v time.Time) FieldValue {
	if v.IsZero() {
		return FieldValue{fieldType: FieldTypeTimestamp, i64: 0}
	}
	return FieldValue{fieldType: FieldTypeTimestamp, i64: v.UnixNano()}
}

// ZeroTimestamp 返回 Timestamp 类型的零值 FieldValue（i64=0）。
// 这是系统约定的"未设置"timestamp 的规范构造形式，Time() 读回为 time.Time{}（IsZero）。
// 等价于 NewTimestamp(time.Time{})，但意图更明确，且不必绕 time.Time 中介。
func ZeroTimestamp() FieldValue {
	return FieldValue{fieldType: FieldTypeTimestamp, i64: 0}
}

// NewCustomFieldValue 构造 Custom FieldValue。内部统一保存 []byte。
func NewCustomFieldValue(v any) FieldValue {
	if v == nil {
		return FieldValue{fieldType: FieldTypeCustom}
	}
	if b, ok := v.([]byte); ok {
		return FieldValue{fieldType: FieldTypeCustom, c: b}
	}
	// 传入非 []byte 属于使用错误（Default 那边会先校验）；兜底为零长度 payload。
	return FieldValue{fieldType: FieldTypeCustom}
}

// FieldTypeSet 给 NewFieldValue[T] 的类型约束。
type FieldTypeSet interface {
	int | int32 | int64 | string | decimal.Decimal | time.Time
	comparable
}

// GetFieldType 按 T 推断 FieldType；注册期便利工具。
func GetFieldType[T FieldTypeSet]() FieldType {
	var zero T
	return typeOfValue(any(zero))
}

// NewFieldValue 泛型便利构造。注意：内部 `any(value).(type)` 会产生一次 iface 箱，
// 不要在热路径（per-field Extract）直接使用；scalarBinder 之类热点请用 NewInt32/NewInt64/... 等类型化构造。
func NewFieldValue[T FieldTypeSet](value T) FieldValue {
	switch t := any(value).(type) {
	case int:
		return NewInt64(int64(t))
	case int32:
		return NewInt32(t)
	case int64:
		return NewInt64(t)
	case decimal.Decimal:
		return NewDecimal(t)
	case time.Time:
		return NewTimestamp(t)
	case string:
		return NewString(t)
	}
	return FieldValue{}
}

func typeOfValue(v any) FieldType {
	switch v.(type) {
	case int, int64:
		return FieldTypeInt64
	case int32:
		return FieldTypeInt32
	case decimal.Decimal:
		return FieldTypeDecimal
	case time.Time:
		return FieldTypeTimestamp
	case string:
		return FieldTypeString
	}
	return FieldTypeUndefined
}
