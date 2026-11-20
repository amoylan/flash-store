package registry

import (
	"errors"
	"time"

	"github.com/amoylan/flash-store/store/schema"
	"github.com/shopspring/decimal"
)

// RowView 是只读视图：持有 TypedDef（字段布局）+ schema.Row（存储数据），
// 供 Query Filter / GetView 读路径按字段名取值，不重建业务 struct。
//
// 方法层面提供按名取 FieldValue，以及常见类型的快捷方法（GetInt32/GetInt64
// /GetString/GetDecimal/GetTime）。GetResult 返回带错误封装的 ViewResult，
// 和过去 registry.View 的 API 尽量保持一致以减少迁移成本。
type RowView struct {
	def TypedDef
	row schema.Row
}

// NewRowView 构造一个 RowView。def / row 均不能为 nil 空值，调用方自保证。
func NewRowView(def TypedDef, row schema.Row) *RowView {
	return &RowView{def: def, row: row}
}

var (
	ErrFieldNotFound = errors.New("field not found")
)

// GetFieldValue 按字段名取 FieldValue。字段不存在返回 ErrFieldNotFound。
//
// 解析链：
//   - TypedDef.FieldByName 找到 FieldDescriptor（Num + Default + Type）
//   - 以 Num 在 Row 中查值（Row 无序，Num 是身份）
//   - 若 Row 里没这个 Num（历史 writer schema 不包含 / 新加的字段）或显式 Undefined
//     → 回落到 spec.Default
//
// 关键：不能按 `row.Fields[pos]` 位置取值——wire 格式解出来的 Row 是无序的，
// 位置和 reader schema 声明顺序之间没有必然联系。
func (v *RowView) GetFieldValue(name schema.FieldName) (schema.FieldValue, error) {
	spec, ok := v.def.FieldByName(name)
	if !ok {
		return schema.FieldValue{}, ErrFieldNotFound
	}
	fv, ok := v.row.GetByNum(spec.Num)
	if !ok || fv.Type() == schema.FieldTypeUndefined {
		return spec.Default, nil
	}
	return fv, nil
}

// Version 返回 Row 对应的 schema 版本，便于多版本共存场景做判断。
func (v *RowView) Version() uint32 { return v.row.Version }

// ViewResult 是 GetResult 的返回值，把 FieldValue + err 一并带回。
type ViewResult struct {
	value schema.FieldValue
	Error error
}

func (r *ViewResult) Int32() (int32, error) {
	if r.Error != nil {
		return 0, r.Error
	}
	return r.value.Int32()
}

func (r *ViewResult) Int64() (int64, error) {
	if r.Error != nil {
		return 0, r.Error
	}
	return r.value.Int64()
}

func (r *ViewResult) String() (string, error) {
	if r.Error != nil {
		return "", r.Error
	}
	return r.value.String()
}

func (r *ViewResult) Decimal() (decimal.Decimal, error) {
	if r.Error != nil {
		return decimal.Decimal{}, r.Error
	}
	return r.value.Decimal()
}

func (r *ViewResult) Time() (time.Time, error) {
	if r.Error != nil {
		return time.Time{}, r.Error
	}
	return r.value.Time()
}

func (r *ViewResult) FieldValue() (schema.FieldValue, error) {
	if r.Error != nil {
		return schema.FieldValue{}, r.Error
	}
	return r.value, nil
}

// GetResult 按字段名取值，包装成 ViewResult。
func (v *RowView) GetResult(name schema.FieldName) *ViewResult {
	fv, err := v.GetFieldValue(name)
	return &ViewResult{value: fv, Error: err}
}
