package index

import (
	"errors"

	"github.com/amoylan/flash-store/store/schema"
)

type Comparator int

const (
	GreaterThan Comparator = iota
	LessThan
	GreaterThanOrEqualsTo
	LessThanOrEqualsTo
)

type Condition struct {
	Comparator Comparator
	Field      schema.FieldValue
}

// NewCondition 新建一个Condition
func NewCondition(cmp Comparator, field schema.FieldValue) *Condition {
	return &Condition{
		Comparator: cmp,
		Field:      field,
	}
}

func (c *Condition) Validate(t schema.FieldType) error {
	if c == nil {
		return nil
	}
	if c.Field.Type() != t {
		return errors.New("type mismatch")
	}
	return nil
}

// Match 当出现错误时，match返回false
// 注意：需要先调用 Validate 处理错误 Match函数只返回是否匹配
func (c *Condition) Match(field schema.FieldValue) bool {
	if c == nil {
		return true
	}
	less := field.Less(field.Type(), c.Field)
	greater := c.Field.Less(field.Type(), field)
	equals := !less && !greater // 不大不小就是相等
	switch c.Comparator {
	case GreaterThan:
		return greater
	case LessThan:
		return less
	case GreaterThanOrEqualsTo:
		return greater || equals
	case LessThanOrEqualsTo:
		return less || equals
	}
	return false
}

type Range struct {
	Left  *Condition
	Right *Condition
}

// NewEqualRange 新建一个单点的Range
func NewEqualRange(field schema.FieldValue) Range {
	return Range{
		Left:  NewCondition(GreaterThanOrEqualsTo, field),
		Right: NewCondition(LessThanOrEqualsTo, field),
	}
}

var ErrInvalidRange = errors.New("invalid range")

func (r Range) Validate(t schema.FieldType) error {
	if r.Left == nil && r.Right == nil {
		return ErrInvalidRange
	}
	if r.Left != nil {
		if !(r.Left.Comparator == GreaterThan ||
			r.Left.Comparator == GreaterThanOrEqualsTo) {
			return ErrInvalidRange
		}
		if err := r.Left.Validate(t); err != nil {
			return err
		}
	}
	if r.Right != nil {
		if !(r.Right.Comparator == LessThan ||
			r.Right.Comparator == LessThanOrEqualsTo) {
			return ErrInvalidRange
		}
		if err := r.Right.Validate(t); err != nil {
			return err
		}
	}
	return nil
}

type Ranges []Range

// Validate 校验Ranges是否合法，如果合法，返回合并后的Range列表
func (rs Ranges) Validate(ft schema.FieldType) (Ranges, error) {
	for _, r := range rs {
		// 校验每一个Range
		if err := r.Validate(ft); err != nil {
			return nil, err
		}
		// TODO: 校验Range之间是否有合理，是否有交集等
	}
	return rs, nil
}

type RangesMap[T comparable] map[T]Ranges
