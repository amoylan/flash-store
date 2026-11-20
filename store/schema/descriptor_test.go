package schema

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/shopspring/decimal"
)

func desc(kind string, version uint32, fields ...FieldDescriptor) KindDescriptor {
	return KindDescriptor{Kind: kind, Version: version, Fields: fields}
}

// f 构造测试用 FieldDescriptor。Num 必须显式传入——compat 基于 Num 身份比对，
// 同一 schema 内 Num 重复会立即报错。
func f(num int32, name string, t FieldType, opts ...func(*FieldDescriptor)) FieldDescriptor {
	fd := FieldDescriptor{Num: num, Name: FieldName(name), Type: t}
	for _, o := range opts {
		o(&fd)
	}
	return fd
}

func withIndex() func(*FieldDescriptor) { return func(fd *FieldDescriptor) { fd.Indexed = true } }
func withPrec(p int32) func(*FieldDescriptor) {
	return func(fd *FieldDescriptor) { fd.DecimalPrecision = p }
}
func withDefault(v FieldValue) func(*FieldDescriptor) {
	return func(fd *FieldDescriptor) { fd.Default = v }
}

func TestCheckSchemaCompat_Allowed(t *testing.T) {
	cases := []struct {
		name     string
		old, cur KindDescriptor
	}{
		{
			"identical",
			desc("a.b", 1, f(1, "id", FieldTypeInt64), f(2, "name", FieldTypeString)),
			desc("a.b", 1, f(1, "id", FieldTypeInt64), f(2, "name", FieldTypeString)),
		},
		{
			"append new field (new Num)",
			desc("a.b", 1, f(1, "id", FieldTypeInt64)),
			desc("a.b", 2, f(1, "id", FieldTypeInt64), f(2, "status", FieldTypeString)),
		},
		{
			"drop field (Num missing in cur)",
			desc("a.b", 1, f(1, "id", FieldTypeInt64), f(2, "status", FieldTypeString)),
			desc("a.b", 2, f(1, "id", FieldTypeInt64)),
		},
		{
			"rename field (same Num, different Name)",
			desc("a.b", 1, f(1, "id", FieldTypeInt64)),
			desc("a.b", 2, f(1, "ID", FieldTypeInt64)),
		},
		{
			"reorder fields (Num is identity, positions don't matter)",
			desc("a.b", 1, f(1, "a", FieldTypeInt64), f(2, "b", FieldTypeString)),
			desc("a.b", 1, f(2, "b", FieldTypeString), f(1, "a", FieldTypeInt64)),
		},
		{
			"toggle Indexed",
			desc("a.b", 1, f(1, "id", FieldTypeInt64), f(2, "status", FieldTypeString)),
			desc("a.b", 1, f(1, "id", FieldTypeInt64), f(2, "status", FieldTypeString, withIndex())),
		},
		{
			"change DecimalPrecision",
			desc("a.b", 1, f(1, "price", FieldTypeDecimal, withPrec(4))),
			desc("a.b", 1, f(1, "price", FieldTypeDecimal, withPrec(8))),
		},
		{
			"version bump",
			desc("a.b", 1, f(1, "id", FieldTypeInt64)),
			desc("a.b", 99, f(1, "id", FieldTypeInt64)),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := CheckSchemaCompat(c.old, c.cur); err != nil {
				t.Fatalf("expected compat, got: %v", err)
			}
		})
	}
}

func TestCheckSchemaCompat_Rejected(t *testing.T) {
	cases := []struct {
		name     string
		old, cur KindDescriptor
		errPart  string
	}{
		{
			"kind mismatch",
			desc("a.b", 1, f(1, "id", FieldTypeInt64)),
			desc("a.c", 1, f(1, "id", FieldTypeInt64)),
			"kind mismatch",
		},
		{
			"same Num, type changed int32 -> string",
			desc("a.b", 1, f(1, "status", FieldTypeInt32)),
			desc("a.b", 1, f(1, "status", FieldTypeString)),
			"type changed",
		},
		{
			"same Num, type changed int32 -> int64",
			desc("a.b", 1, f(1, "n", FieldTypeInt32)),
			desc("a.b", 1, f(1, "n", FieldTypeInt64)),
			"type changed",
		},
		{
			"same Num, type changed decimal -> int64",
			desc("a.b", 1, f(1, "p", FieldTypeDecimal)),
			desc("a.b", 1, f(1, "p", FieldTypeInt64)),
			"type changed",
		},
		{
			"duplicate Num in new schema",
			desc("a.b", 1, f(1, "a", FieldTypeInt64)),
			desc("a.b", 2, f(1, "a", FieldTypeInt64), f(1, "b", FieldTypeInt64)),
			"duplicate num",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := CheckSchemaCompat(c.old, c.cur)
			if err == nil {
				t.Fatalf("expected error containing %q", c.errPart)
			}
			if !strings.Contains(err.Error(), c.errPart) {
				t.Fatalf("error %q missing %q", err, c.errPart)
			}
		})
	}
}

// Default 相关：同 Num 字段 Default 改变必须拒绝。
func TestCheckSchemaCompat_DefaultChangeRejected(t *testing.T) {
	cases := []struct {
		name string
		old  FieldValue
		cur  FieldValue
	}{
		{"int32 0 → 1", NewInt32(0), NewInt32(1)},
		{"int64 100 → 200", NewInt64(100), NewInt64(200)},
		{"string empty → open", NewString(""), NewString("open")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			old := desc("a.b", 1, f(1, "x", c.old.Type(), withDefault(c.old)))
			cur := desc("a.b", 1, f(1, "x", c.cur.Type(), withDefault(c.cur)))
			err := CheckSchemaCompat(old, cur)
			if err == nil {
				t.Fatal("expected error for Default change")
			}
			if !strings.Contains(err.Error(), "default changed") {
				t.Fatalf("error should mention 'default changed': %v", err)
			}
		})
	}
}

func TestCheckSchemaCompat_SameDefaultAllowed(t *testing.T) {
	tenPointZero, _ := decimal.NewFromString("1.0")
	one, _ := decimal.NewFromString("1")
	cases := []struct {
		name     string
		old, cur KindDescriptor
	}{
		{
			"same int default",
			desc("a.b", 1, f(1, "x", FieldTypeInt32, withDefault(NewInt32(7)))),
			desc("a.b", 1, f(1, "x", FieldTypeInt32, withDefault(NewInt32(7)))),
		},
		{
			"decimal value-equal (different literal)",
			desc("a.b", 1, f(1, "x", FieldTypeDecimal, withDefault(NewDecimal(tenPointZero)))),
			desc("a.b", 1, f(1, "x", FieldTypeDecimal, withDefault(NewDecimal(one)))),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := CheckSchemaCompat(c.old, c.cur); err != nil {
				t.Fatalf("expected compat, got: %v", err)
			}
		})
	}
}

// JSON round-trip：FieldType 作为字符串存/取，未来 iota 重新编号不破坏历史 snapshot。
func TestKindDescriptor_JSON_RoundTrip(t *testing.T) {
	d := desc("future.order", 3,
		f(1, "id", FieldTypeInt64),
		f(2, "status", FieldTypeInt32, withIndex()),
		f(3, "price", FieldTypeDecimal, withIndex(), withPrec(8)),
		f(4, "created_at", FieldTypeTimestamp),
	)
	data, err := json.Marshal(&d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"int64"`) || !strings.Contains(string(data), `"decimal"`) {
		t.Fatalf("FieldType not serialized as string: %s", data)
	}
	var got KindDescriptor
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Kind != d.Kind || got.Version != d.Version || len(got.Fields) != len(d.Fields) {
		t.Fatalf("round-trip = %+v, want %+v", got, d)
	}
	for i := range d.Fields {
		if !fieldDescEqual(got.Fields[i], d.Fields[i]) {
			t.Fatalf("field[%d] = %+v, want %+v", i, got.Fields[i], d.Fields[i])
		}
	}
}

// fieldDescEqual 逐字段比较——FieldDescriptor 含 FieldValue（内有 []byte 切片），
// 不能用 == 比较。
func fieldDescEqual(a, b FieldDescriptor) bool {
	return a.Num == b.Num &&
		a.Name == b.Name &&
		a.Type == b.Type &&
		a.Indexed == b.Indexed &&
		a.DecimalPrecision == b.DecimalPrecision &&
		a.StringMaxLen == b.StringMaxLen &&
		a.Default.Equal(b.Default)
}
