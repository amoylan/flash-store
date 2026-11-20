package registry

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/amoylan/flash-store/store/schema"
)

// storeTag 是 `store:"..."` 标签的解析结果。
//
// 支持形式：
//
//	`store:"-"`                               显式跳过
//	无 store 标签                             隐式跳过
//	`store:"name,num=<int>"`                  最小形态——name + num 均必填
//	`store:"name,num=<int>,opt1,opt2=val2"`   带选项
//
// num=<int> 是字段的稳定身份（FieldDescriptor.Num）：
//
//	`schema:"name,num=7"` 含义：字段 "name" 的身份是 7。Go struct 字段顺序、
//	schema 里声明顺序都无关紧要，wire 里只认 Num。Num 一旦分配就不能被其他
//	字段复用（删除后也不行），由 store 级别跨历史版本的 compat 检查兜底。
//
// 识别的选项：
//
//	num=<int>           字段稳定身份（int32，> 0）；**必填**
//	index               字段进入索引位列表
//	custom              强制按 Custom 类型处理（custom 的速记形式，等价 type=custom）
//	type=<name>         显式指定 FieldType：int32 / int64 / string / decimal / timestamp / custom
//	                    用于 Go 类型推断不唯一或需要明确声明的场景；底层 Go kind 还得和声明
//	                    的类型兼容（例：type=int32 要求字段 kind 为 int32），不然注册期报错。
//	prec=<int>          Decimal 字段的精度（Truncate 位数），等价 schema.DecimalPrec
//	max=<int>           String 字段的最大长度（当前仅作元数据）
//	default=<literal>   字段默认值；按 FieldType 解析：
//	                      Int32/Int64  十进制整数
//	                      String       字面量（空字符串可写 default=）
//	                      Decimal      十进制小数字符串
//	                      Timestamp    RFC3339Nano，或 "zero" 代表 time.Time{}
type storeTag struct {
	Skip            bool
	Name            string
	Num             int32 // 必填；解析后 > 0
	HasNum          bool
	Indexed         bool
	Custom          bool
	HasTypeOverride bool
	TypeOverride    schema.FieldType
	DecPrec         *int32
	StrMax          *int
	HasDefault      bool
	Default         string
}

func parseStoreTag(raw string) (storeTag, error) {
	if raw == "" || raw == "-" {
		return storeTag{Skip: true}, nil
	}
	parts := strings.Split(raw, ",")
	t := storeTag{Name: strings.TrimSpace(parts[0])}
	if t.Name == "" {
		return storeTag{}, fmt.Errorf("store tag: empty name (use `-` to skip)")
	}
	for _, opt := range parts[1:] {
		opt = strings.TrimSpace(opt)
		if opt == "" {
			continue
		}
		key, val, hasEq := strings.Cut(opt, "=")
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		switch key {
		case "num":
			if !hasEq || val == "" {
				return storeTag{}, fmt.Errorf("store tag: num requires a value")
			}
			n, err := strconv.ParseInt(val, 10, 32)
			if err != nil {
				return storeTag{}, fmt.Errorf("store tag: num %q: %w", val, err)
			}
			if n <= 0 {
				return storeTag{}, fmt.Errorf("store tag: num must be > 0 (got %d)", n)
			}
			t.Num = int32(n)
			t.HasNum = true
		case "index":
			t.Indexed = true
		case "custom":
			t.Custom = true
		case "type":
			if !hasEq {
				return storeTag{}, fmt.Errorf("store tag: type requires a value")
			}
			ft, err := parseFieldTypeName(val)
			if err != nil {
				return storeTag{}, fmt.Errorf("store tag: %w", err)
			}
			t.HasTypeOverride = true
			t.TypeOverride = ft
		case "prec":
			if !hasEq {
				return storeTag{}, fmt.Errorf("store tag: prec requires a value")
			}
			n, err := strconv.Atoi(val)
			if err != nil {
				return storeTag{}, fmt.Errorf("store tag: prec %q: %w", val, err)
			}
			p := int32(n)
			t.DecPrec = &p
		case "max":
			if !hasEq {
				return storeTag{}, fmt.Errorf("store tag: max requires a value")
			}
			n, err := strconv.Atoi(val)
			if err != nil {
				return storeTag{}, fmt.Errorf("store tag: max %q: %w", val, err)
			}
			t.StrMax = &n
		case "default":
			if !hasEq {
				return storeTag{}, fmt.Errorf("store tag: default requires a value")
			}
			t.HasDefault = true
			t.Default = val
		default:
			return storeTag{}, fmt.Errorf("store tag: unknown option %q", opt)
		}
	}
	if !t.HasNum {
		return storeTag{}, fmt.Errorf("store tag: field %q missing required `num=<int>` (Num is the field's stable identity; assign a unique int > 0)", t.Name)
	}
	return t, nil
}

func parseFieldTypeName(s string) (schema.FieldType, error) {
	switch s {
	case "int32":
		return schema.FieldTypeInt32, nil
	case "int64":
		return schema.FieldTypeInt64, nil
	case "string":
		return schema.FieldTypeString, nil
	case "decimal":
		return schema.FieldTypeDecimal, nil
	case "timestamp":
		return schema.FieldTypeTimestamp, nil
	case "custom":
		return schema.FieldTypeCustom, nil
	}
	return schema.FieldTypeUndefined, fmt.Errorf("unknown type name %q (want int32/int64/string/decimal/timestamp/custom)", s)
}
