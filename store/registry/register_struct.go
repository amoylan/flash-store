package registry

import (
	"fmt"
	"reflect"
	"strconv"
	"time"
	"unsafe"

	"github.com/amoylan/flash-store/store/schema"
	"github.com/shopspring/decimal"
)

// RegisterStruct 是 tag-driven 的快速注册入口：业务在 struct 字段上加 `store:"..."`
// 标签声明即可注册一个 Def[T]，不再需要手写 Builder 链。
//
// 用法：
//
//	type Order struct {
//	    OrderID   string          `store:"order_id,num=1"`
//	    Status    OrderStatus     `store:"status,num=2,index,type=int32"`
//	    Price     decimal.Decimal `store:"price,num=4,index,prec=8"`
//	    // Num=3 曾作为 "region" (string, default="") 存在，v5 移除
//	    // ⇒ 推荐写下面的 Retired{} 声明，让同包内新字段撞 Num 时立刻 panic
//	}
//
//	def, err := registry.RegisterStruct[Order](orderKind,
//	    schema.RetiredField{Num: 3, Name: "region",
//	        Type: schema.FieldTypeString, Default: schema.NewString("")},
//	)
//
// retired 参数记录业务之前删除过的字段——Num 永久占用。**推荐**写但非强制：
// 写了能让 reviewer 在 diff 里看到"这个 Num 动过"，并让同包内加新字段撞已退休
// Num 时在注册期（包 init）立刻 panic。忘写的话只丢失这层 init 期防御；跨进程
// 的 Num 复用拦截由 Store 端的 tombstone（运行期累积）独立兜底。
//
// 原理：启动期 reflect 解析 tag 和字段类型，利用 reflect.StructField.Offset
// 构建 `unsafe.Pointer + offset` 形态的 accessor（零反射热路径）。Go 编译期
// 不会拒绝这里的 unsafe，但它和 reflect.Value 的字段读写在内存语义上等价。
//
// 与 Builder 的关系：Def 仍是存储唯一真相源；RegisterStruct 只是把"手写 Builder
// 链"的那段由 tag 自动推出来。二者可共存——边界场景（字段名需要跨 struct 共享、
// 计算字段、自定义 normalize 等）仍可用 Builder。
func RegisterStruct[T any](kind Kind, retired ...schema.RetiredField) (*Def[T], error) {
	def, err := BuildFromStruct[T](kind, retired...)
	if err != nil {
		return nil, err
	}
	if err := RegisterDef(def); err != nil {
		return nil, err
	}
	return def, nil
}

// BuildFromStruct 只构建 Def[T]，不注册。用于测试或需要进一步二次处理 Def 的场景。
func BuildFromStruct[T any](kind Kind, retired ...schema.RetiredField) (*Def[T], error) {
	var zero T
	t := reflect.TypeOf(zero)
	if t == nil {
		return nil, fmt.Errorf("registry.BuildFromStruct: T is nil interface")
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("registry.BuildFromStruct: T must be a struct, got %s", t.Kind())
	}
	b := For[T](kind)
	// 先注册 retired——这样后续 addField 能立即发现新字段 num 撞到已退休 num。
	// Retire 调用里的 panic（Num 冲突 / 重复声明内容不一致）在 reflect 扫字段
	// 之前就被捕获；不需要 recover 包装，让 panic 一路抛到业务 init()。
	for _, rf := range retired {
		b.Retire(rf)
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag, err := parseStoreTag(f.Tag.Get("store"))
		if err != nil {
			return nil, fmt.Errorf("field %s.%s: %w", t.Name(), f.Name, err)
		}
		if tag.Skip {
			continue
		}
		if !f.IsExported() {
			return nil, fmt.Errorf("field %s.%s: has store tag but is unexported", t.Name(), f.Name)
		}
		if err := addStructField(b, f, tag); err != nil {
			return nil, fmt.Errorf("field %s.%s: %w", t.Name(), f.Name, err)
		}
	}
	return b.Build(), nil
}

// ---- type detection ----

var (
	decimalReflectType = reflect.TypeOf(decimal.Decimal{})
	timeReflectType    = reflect.TypeOf(time.Time{})
	customValuerType   = reflect.TypeOf((*schema.CustomValuer)(nil)).Elem()
	customScannerType  = reflect.TypeOf((*schema.CustomScanner)(nil)).Elem()
)

// resolveFieldType 根据 Go 类型 + tag 决定 FieldType。
//
// 顺序：
//  1. tag 显式覆盖：type= 或 custom（后者是 type=custom 的速记）
//  2. 具名类型：decimal.Decimal / time.Time
//  3. 预声明基本类型：int32 / int64 / int / string（仅此四种允许零配置推断）
//  4. *T 实现 Valuer/Scanner → Custom
//  5. 报错
//
// 对 scalar kind 的**自定义命名类型**（`type OrderStatus int32` 之类）强制要求
// 业务在 tag 里显式写 `type=int32`——这样意图可审、不会因为改了枚举的 underlying
// 类型而静默改变存储 schema。想要自动推断的话，字段直接用预声明基本类型即可。
func resolveFieldType(t reflect.Type, tag storeTag) (schema.FieldType, error) {
	if tag.HasTypeOverride {
		return tag.TypeOverride, nil
	}
	if tag.Custom {
		return schema.FieldTypeCustom, nil
	}
	switch t {
	case decimalReflectType:
		return schema.FieldTypeDecimal, nil
	case timeReflectType:
		return schema.FieldTypeTimestamp, nil
	}
	switch t.Kind() {
	case reflect.Int32, reflect.Int64, reflect.Int, reflect.String:
		if !isPredeclaredBasic(t) {
			return schema.FieldTypeUndefined, fmt.Errorf(
				"field type %s is a defined type with underlying %s; add `type=%s` to tag to confirm intent (auto-inference only applies to predeclared basic types)",
				t, t.Kind(), suggestTypeTag(t.Kind()))
		}
		return kindToFieldType(t.Kind()), nil
	}
	// 最后尝试 Custom：*T 实现 Valuer/Scanner 即视作 Custom。
	ptr := reflect.PointerTo(t)
	if ptr.Implements(customValuerType) && ptr.Implements(customScannerType) {
		return schema.FieldTypeCustom, nil
	}
	return schema.FieldTypeUndefined, fmt.Errorf(
		"unsupported Go type %s; tag with type=... or custom + Valuer/Scanner methods on *%s, or use a supported type",
		t, t.Name())
}

// isPredeclaredBasic 判断 t 是否是 Go 预声明基本类型（int32 / int64 / int / string）本身，
// 而不是底层同 kind 的自定义命名类型。判据：PkgPath 为空（预声明类型无所属包）
// 且 Name 等于 Kind 的字符串形式。
func isPredeclaredBasic(t reflect.Type) bool {
	return t.PkgPath() == "" && t.Name() == t.Kind().String()
}

func kindToFieldType(k reflect.Kind) schema.FieldType {
	switch k {
	case reflect.Int32:
		return schema.FieldTypeInt32
	case reflect.Int64, reflect.Int:
		return schema.FieldTypeInt64
	case reflect.String:
		return schema.FieldTypeString
	}
	return schema.FieldTypeUndefined
}

func suggestTypeTag(k reflect.Kind) string {
	switch k {
	case reflect.Int32:
		return "int32"
	case reflect.Int64, reflect.Int:
		return "int64"
	case reflect.String:
		return "string"
	}
	return "..."
}

// ---- field registration ----

func addStructField[T any](b *Builder[T], f reflect.StructField, tag storeTag) error {
	ft, err := resolveFieldType(f.Type, tag)
	if err != nil {
		return err
	}

	// 校验选项与类型的合法组合。
	if tag.DecPrec != nil && ft != schema.FieldTypeDecimal {
		return fmt.Errorf("prec= only applies to Decimal fields")
	}
	if tag.StrMax != nil && ft != schema.FieldTypeString {
		return fmt.Errorf("max= only applies to String fields")
	}
	if tag.Indexed && ft == schema.FieldTypeCustom {
		return fmt.Errorf("custom field cannot be indexed")
	}

	spec := schema.FieldDescriptor{
		Name:             schema.FieldName(tag.Name),
		Type:             ft,
		Indexed:          tag.Indexed,
		DecimalPrecision: -1,
	}
	if tag.DecPrec != nil {
		spec.DecimalPrecision = *tag.DecPrec
	}
	if tag.StrMax != nil {
		spec.StringMaxLen = *tag.StrMax
	}
	if tag.HasDefault {
		def, err := parseTagDefault(ft, tag.Default)
		if err != nil {
			return err
		}
		spec.Default = def
	} else {
		spec.Default = schema.ZeroDefault(ft)
	}

	bnd, err := offsetBinder[T](ft, f.Type, f.Offset)
	if err != nil {
		return err
	}
	b.addField(tag.Num, spec, bnd)
	return nil
}

// offsetBinder 根据 FieldType + Go kind 构建 unsafe.Pointer + offset accessor。
//
// 关键：pointer cast 的目标类型必须和字段的**实际内存宽度**匹配。
// 例如 `type Status int` 的 Go kind 是 reflect.Int（32-bit 系统上 4 字节，64-bit
// 系统上 8 字节），此时指针必须转成 (*int)(ptr)、不能直接 (*int64)(ptr)——
// 后者会在 32-bit 系统上越界写入邻接字段。每个 FieldType 下再按 goType.Kind()
// 具体分派。
func offsetBinder[T any](ft schema.FieldType, goType reflect.Type, offset uintptr) (Binder[T], error) {
	switch ft {
	case schema.FieldTypeInt32:
		return int32OffsetBinder[T](goType, offset)
	case schema.FieldTypeInt64:
		return int64OffsetBinder[T](goType, offset)
	case schema.FieldTypeString:
		if goType.Kind() != reflect.String {
			return Binder[T]{}, fmt.Errorf("string field: Go kind %v not supported (need ~string)", goType.Kind())
		}
		return stringOffsetBinder[T](offset), nil
	case schema.FieldTypeDecimal:
		if goType != decimalReflectType {
			return Binder[T]{}, fmt.Errorf("decimal field: Go type must be decimal.Decimal, got %s", goType)
		}
		return decimalOffsetBinder[T](offset), nil
	case schema.FieldTypeTimestamp:
		if goType != timeReflectType {
			return Binder[T]{}, fmt.Errorf("timestamp field: Go type must be time.Time, got %s", goType)
		}
		return timestampOffsetBinder[T](offset), nil
	case schema.FieldTypeCustom:
		return customOffsetBinder[T](goType, offset)
	}
	return Binder[T]{}, fmt.Errorf("offsetBinder: unsupported FieldType %v", ft)
}

func int32OffsetBinder[T any](goType reflect.Type, offset uintptr) (Binder[T], error) {
	if goType.Kind() != reflect.Int32 {
		return Binder[T]{}, fmt.Errorf(
			"int32 field: Go kind %v not supported (need ~int32; if field is a wider integer, change declared type or use type=int64)",
			goType.Kind())
	}
	return Binder[T]{
		Get: func(o *T) schema.FieldValue {
			ptr := (*int32)(unsafe.Add(unsafe.Pointer(o), offset))
			return schema.NewInt32(*ptr)
		},
		Set: func(o *T, v *schema.FieldValue) error {
			x, err := v.Int32()
			if err != nil {
				return err
			}
			ptr := (*int32)(unsafe.Add(unsafe.Pointer(o), offset))
			*ptr = x
			return nil
		},
	}, nil
}

// int64OffsetBinder 同时支持 reflect.Int64 和 reflect.Int——后者覆盖形如
// `type SeqNo int` / 业务使用 iota 枚举的习惯场景，用 (*int)(ptr) 保证内存宽度正确。
func int64OffsetBinder[T any](goType reflect.Type, offset uintptr) (Binder[T], error) {
	switch goType.Kind() {
	case reflect.Int64:
		return Binder[T]{
			Get: func(o *T) schema.FieldValue {
				ptr := (*int64)(unsafe.Add(unsafe.Pointer(o), offset))
				return schema.NewInt64(*ptr)
			},
			Set: func(o *T, v *schema.FieldValue) error {
				x, err := v.Int64()
				if err != nil {
					return err
				}
				ptr := (*int64)(unsafe.Add(unsafe.Pointer(o), offset))
				*ptr = x
				return nil
			},
		}, nil
	case reflect.Int:
		return Binder[T]{
			Get: func(o *T) schema.FieldValue {
				ptr := (*int)(unsafe.Add(unsafe.Pointer(o), offset))
				return schema.NewInt64(int64(*ptr))
			},
			Set: func(o *T, v *schema.FieldValue) error {
				x, err := v.Int64()
				if err != nil {
					return err
				}
				ptr := (*int)(unsafe.Add(unsafe.Pointer(o), offset))
				*ptr = int(x)
				return nil
			},
		}, nil
	}
	return Binder[T]{}, fmt.Errorf(
		"int64 field: Go kind %v not supported (need ~int or ~int64)",
		goType.Kind())
}

func stringOffsetBinder[T any](offset uintptr) Binder[T] {
	return Binder[T]{
		Get: func(o *T) schema.FieldValue {
			ptr := (*string)(unsafe.Add(unsafe.Pointer(o), offset))
			return schema.NewString(*ptr)
		},
		Set: func(o *T, v *schema.FieldValue) error {
			s, err := v.String()
			if err != nil {
				return err
			}
			ptr := (*string)(unsafe.Add(unsafe.Pointer(o), offset))
			*ptr = s
			return nil
		},
	}
}

func decimalOffsetBinder[T any](offset uintptr) Binder[T] {
	return Binder[T]{
		Get: func(o *T) schema.FieldValue {
			ptr := (*decimal.Decimal)(unsafe.Add(unsafe.Pointer(o), offset))
			return schema.NewDecimal(*ptr)
		},
		Set: func(o *T, v *schema.FieldValue) error {
			d, err := v.Decimal()
			if err != nil {
				return err
			}
			ptr := (*decimal.Decimal)(unsafe.Add(unsafe.Pointer(o), offset))
			*ptr = d
			return nil
		},
	}
}

func timestampOffsetBinder[T any](offset uintptr) Binder[T] {
	return Binder[T]{
		Get: func(o *T) schema.FieldValue {
			ptr := (*time.Time)(unsafe.Add(unsafe.Pointer(o), offset))
			return schema.NewTimestamp(*ptr)
		},
		Set: func(o *T, v *schema.FieldValue) error {
			t, err := v.Time()
			if err != nil {
				return err
			}
			ptr := (*time.Time)(unsafe.Add(unsafe.Pointer(o), offset))
			*ptr = t
			return nil
		},
	}
}

// customOffsetBinder 走 reflect.NewAt 把指向字段偏移的 unsafe.Pointer 复原成
// `*F`，然后类型断言到 CustomValuer / CustomScanner 接口调用业务方法。
// reflect.NewAt 每次会 new 一个 reflect.Value（约 32B 分配）——Custom 字段通常
// 是低频字段，能接受；若要零分配可改 itab 手工拼接，但会绑死 runtime 内部。
// copyCustomBytes 给 CustomValuer.Value() 返回的 slice 做防御性拷贝。
// nil / 空切片直接返回原值（NewCustomFieldValue 内部对 nil/empty 也是无开销分支）。
func copyCustomBytes(b []byte) []byte {
	if len(b) == 0 {
		return b
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func customOffsetBinder[T any](goType reflect.Type, offset uintptr) (Binder[T], error) {
	ptrType := reflect.PointerTo(goType)
	if !ptrType.Implements(customValuerType) {
		return Binder[T]{}, fmt.Errorf("custom field: *%s does not implement schema.CustomValuer", goType)
	}
	if !ptrType.Implements(customScannerType) {
		return Binder[T]{}, fmt.Errorf("custom field: *%s does not implement schema.CustomScanner", goType)
	}
	return Binder[T]{
		Get: func(o *T) schema.FieldValue {
			addr := unsafe.Add(unsafe.Pointer(o), offset)
			v := reflect.NewAt(goType, addr).Interface()
			bytes, err := v.(schema.CustomValuer).Value()
			if err != nil {
				return schema.NewCustomFieldValue([]byte(nil))
			}
			// 防御性拷贝：CustomValuer.Value() 的契约不强制返回独立 slice。
			// 若用户实现返回内部字段的 alias，Commit 后业务修改原 *T 会"穿透"到存进
			// shard.objects 的 Row，造成内存视图随用户对象漂移。
			// 在这一处拷贝就够了——后续 row 的所有持有方（shard、snapshot 序列化、
			// listener 回调）都基于这份独立拷贝。
			return schema.NewCustomFieldValue(copyCustomBytes(bytes))
		},
		Set: func(o *T, v *schema.FieldValue) error {
			bytes, err := v.CustomBytes()
			if err != nil {
				return err
			}
			addr := unsafe.Add(unsafe.Pointer(o), offset)
			p := reflect.NewAt(goType, addr).Interface()
			return p.(schema.CustomScanner).Scan(bytes)
		},
	}, nil
}

// ---- default parsing ----

func parseTagDefault(ft schema.FieldType, s string) (schema.FieldValue, error) {
	switch ft {
	case schema.FieldTypeInt32:
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return schema.FieldValue{}, fmt.Errorf("default=%q not a valid int32: %w", s, err)
		}
		return schema.NewInt32(int32(n)), nil
	case schema.FieldTypeInt64:
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return schema.FieldValue{}, fmt.Errorf("default=%q not a valid int64: %w", s, err)
		}
		return schema.NewInt64(n), nil
	case schema.FieldTypeString:
		return schema.NewString(s), nil
	case schema.FieldTypeDecimal:
		d, err := decimal.NewFromString(s)
		if err != nil {
			return schema.FieldValue{}, fmt.Errorf("default=%q not a valid decimal: %w", s, err)
		}
		return schema.NewDecimal(d), nil
	case schema.FieldTypeTimestamp:
		if s == "" || s == "zero" {
			return schema.ZeroTimestamp(), nil
		}
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return schema.FieldValue{}, fmt.Errorf("default=%q not valid RFC3339Nano: %w", s, err)
		}
		return schema.NewTimestamp(t), nil
	}
	return schema.FieldValue{}, fmt.Errorf("default= not supported for type %v", ft)
}
