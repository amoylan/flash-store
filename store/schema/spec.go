package schema

import (
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

// RetiredField 记录一个已从 schema 移除、但 Num 依然永久占用的字段。
//
// 业务在删除字段时**推荐**在 RegisterStruct / Builder 里显式声明相应的 RetiredField
// （通过 Retired{...} 参数或 Builder.Retire 方法）。这让 "Num 永久占用" 的契约
// 从代码层面就可见，并触发同包内的 Num 防撞检查：
//   - review 能看到 "-Status field + +Retired{Num:2}" 对称的 diff
//   - 同包其他人加新字段时若撞到已 retired 的 Num，**注册期**（包 init）立刻 panic
//
// 不强制：跨进程的 tombstone 由 Store 自己根据 broadcast / snapshot 的 alive 变化
// 累积维护（参见 fieldsRegistry.upsert），跟源码里有没有 RetiredField 声明无关。
// 忘加 RetiredField 仅丢失"包 init 期撞 Num 立刻 panic"这层保护，跨重启的 Num
// 复用拦截在 Store 端依然有效（vs alive + vs tombstone 检查）。
//
// Type / Default 是诊断与同包防撞所需，应填字段**最后一次存在时的 schema**——
// Builder.Retire 用它判定"声明同 Num 时内容是否一致"以拦重复声明的笔误。
//
// Name 只作诊断用（错误信息、日志）——schema 层面字段身份只认 Num。
type RetiredField struct {
	Num     int32
	Name    FieldName
	Type    FieldType
	Default FieldValue
}

// CustomValuer / CustomScanner 是业务自定义类型与 Row 之间的编解码桥。
// 语义对齐 database/sql 的 driver.Valuer + sql.Scanner，只不过面向 []byte：
//
//   - Value：把自身编码成 bytes，写入路径（Extract / WAL / snapshot）用。
//   - Scan：把 bytes 解回到自身，读取路径（Fill / replay / restore）用。
//
// Row 里的 Custom 字段在内存中**只存 Valuer 产出的 bytes**，不是 Go 对象引用。
// 这样做的好处：bytes 不可变，业务修改自己持有的对象不会污染存储/索引；
// 不需要再额外要求业务实现 Clone。代价是每次 Get 要走一次 Scan 重建对象。
//
// 典型实现（方法挂在 *F 上，允许 Scan 修改自身）：
//
//	type Extra struct { Fee decimal.Decimal; Tags []string }
//	func (e *Extra) Value() ([]byte, error)   { return json.Marshal(e) }
//	func (e *Extra) Scan(data []byte) error   { return json.Unmarshal(data, e) }
type CustomValuer interface {
	Value() ([]byte, error)
}

type CustomScanner interface {
	Scan(data []byte) error
}

// FieldOption 在字段注册期修改其 FieldDescriptor。
// 定向到特定类型的选项（DecimalPrec/StringMax 等）若类型不匹配会在注册期 panic。
type FieldOption func(*FieldDescriptor)

// Indexed 声明字段可索引。Custom 类型禁止标记 Indexed，由 Builder.Build 校验。
func Indexed() FieldOption {
	return func(s *FieldDescriptor) {
		s.Indexed = true
	}
}

// Default 指定默认值。v 的 Go 类型必须与字段 FieldType 匹配，否则注册期 panic。
// 用于：schema 演进后旧 Row 缺失该字段、partial Get 的源 struct 不带该字段、
// Extract 接收 nil 对象。
func Default(v any) FieldOption {
	return func(s *FieldDescriptor) {
		fv, err := coerceToFieldValue(s.Type, v)
		if err != nil {
			panic(fmt.Sprintf("schema.Default: field %q: %v", s.Name, err))
		}
		s.Default = fv
	}
}

// DecimalPrec 为 FieldTypeDecimal 字段设置截断精度。
// Extract 在写入期按此精度规范化，使存储里的 Decimal 始终符合约束。
func DecimalPrec(p int32) FieldOption {
	return func(s *FieldDescriptor) {
		if s.Type != FieldTypeDecimal {
			panic(fmt.Sprintf("schema.DecimalPrec: field %q is not FieldTypeDecimal", s.Name))
		}
		s.DecimalPrecision = p
	}
}

// StringMax 为 FieldTypeString 字段声明最大长度；当前仅作元数据，store 不主动截断。
func StringMax(n int) FieldOption {
	return func(s *FieldDescriptor) {
		if s.Type != FieldTypeString {
			panic(fmt.Sprintf("schema.StringMax: field %q is not FieldTypeString", s.Name))
		}
		s.StringMaxLen = n
	}
}

func coerceToFieldValue(t FieldType, v any) (FieldValue, error) {
	switch t {
	case FieldTypeInt32:
		switch x := v.(type) {
		case int32:
			return NewFieldValue(x), nil
		case int:
			return NewFieldValue(int32(x)), nil
		}
		return FieldValue{}, fmt.Errorf("want int32, got %T", v)
	case FieldTypeInt64:
		switch x := v.(type) {
		case int64:
			return NewFieldValue(x), nil
		case int:
			return NewFieldValue(int64(x)), nil
		case int32:
			return NewFieldValue(int64(x)), nil
		}
		return FieldValue{}, fmt.Errorf("want int64, got %T", v)
	case FieldTypeString:
		x, ok := v.(string)
		if !ok {
			return FieldValue{}, fmt.Errorf("want string, got %T", v)
		}
		return NewFieldValue(x), nil
	case FieldTypeDecimal:
		x, ok := v.(decimal.Decimal)
		if !ok {
			return FieldValue{}, fmt.Errorf("want decimal.Decimal, got %T", v)
		}
		return NewFieldValue(x), nil
	case FieldTypeTimestamp:
		x, ok := v.(time.Time)
		if !ok {
			return FieldValue{}, fmt.Errorf("want time.Time, got %T", v)
		}
		return NewFieldValue(x), nil
	case FieldTypeCustom:
		// Custom 字段在 Row 里只存 bytes（由 CustomValuer 编码）；
		// 因此 Default 必须是 []byte（或 nil，代表"零长度 payload"）。
		if v == nil {
			return NewCustomFieldValue([]byte(nil)), nil
		}
		b, ok := v.([]byte)
		if !ok {
			return FieldValue{}, fmt.Errorf("custom Default must be []byte, got %T", v)
		}
		return NewCustomFieldValue(b), nil
	}
	return FieldValue{}, fmt.Errorf("unsupported FieldType %v", t)
}

// ZeroDefault 返回给定 FieldType 的规范零值，Builder 在用户未显式指定 Default
// 时使用，保证 spec.Default.Type() 非 Undefined。
func ZeroDefault(t FieldType) FieldValue {
	switch t {
	case FieldTypeInt32:
		return NewFieldValue[int32](0)
	case FieldTypeInt64:
		return NewFieldValue[int64](0)
	case FieldTypeString:
		return NewFieldValue("")
	case FieldTypeDecimal:
		return NewFieldValue(decimal.Zero)
	case FieldTypeTimestamp:
		return ZeroTimestamp()
	case FieldTypeCustom:
		return NewCustomFieldValue([]byte(nil))
	}
	return FieldValue{}
}
