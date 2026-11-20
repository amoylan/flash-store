package registry

import (
	"errors"
	"testing"

	"github.com/amoylan/flash-store/store/schema"
)

// 模拟 snapshot 存好 Row 之后 tag 改类型的两种演进方向。
// Row 本身自描述（每字段带 FieldType 标签），Fill 只靠 FieldValue.Int32/Int64 访问器
// 做转换——因此行为完全由这两个 accessor 的语义决定。

// 方向 1：snapshot 里存的 Int32 → tag 改成 int64 → 能否无损恢复？
func TestEvolve_Int32StoredThenInt64Tag(t *testing.T) {
	type oldRec struct {
		V int32 `store:"v,num=1"`
	}
	type newRec struct {
		V int64 `store:"v,num=1"`
	}
	oldKind, _ := NewKind("evo", "i32-to-i64-old")
	newKind, _ := NewKind("evo", "i32-to-i64-new")
	oldDef, _ := BuildFromStruct[oldRec](oldKind)
	newDef, _ := BuildFromStruct[newRec](newKind)

	// 模拟：snapshot 里有一个按 int32 schema 写下的 Row。
	stored := oldDef.Extract(&oldRec{V: 123456})
	bytes, _ := schema.EncodeRow(stored)

	// 用新 schema（tag 是 int64）Fill 这份 bytes。
	decoded, err := schema.DecodeRow(bytes)
	if err != nil {
		t.Fatalf("DecodeRow: %v", err)
	}
	out := &newRec{}
	if err := newDef.Fill(decoded, out); err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if out.V != 123456 {
		t.Fatalf("widening int32→int64: got %d, want 123456", out.V)
	}

	// 负数也要正确处理（sign extension）。
	stored2 := oldDef.Extract(&oldRec{V: -7})
	bytes2, _ := schema.EncodeRow(stored2)
	decoded2, _ := schema.DecodeRow(bytes2)
	out2 := &newRec{}
	_ = newDef.Fill(decoded2, out2)
	if out2.V != -7 {
		t.Fatalf("widening negative: got %d, want -7", out2.V)
	}
}

// 方向 2：snapshot 里存的 Int64 → tag 改成 int32。
// 分两种子情况：值在 int32 范围内（安全） vs 超出范围（**静默截断**）。
func TestEvolve_Int64StoredThenInt32Tag(t *testing.T) {
	type oldRec struct {
		V int64 `store:"v,num=1"`
	}
	type newRec struct {
		V int32 `store:"v,num=1"`
	}
	oldKind, _ := NewKind("evo", "i64-to-i32-old")
	newKind, _ := NewKind("evo", "i64-to-i32-new")
	oldDef, _ := BuildFromStruct[oldRec](oldKind)
	newDef, _ := BuildFromStruct[newRec](newKind)

	// 子情况 A：值 (42) 在 int32 范围内 —— 无损。
	storedSmall := oldDef.Extract(&oldRec{V: 42})
	bytesSmall, _ := schema.EncodeRow(storedSmall)
	decodedSmall, _ := schema.DecodeRow(bytesSmall)
	outSmall := &newRec{}
	if err := newDef.Fill(decodedSmall, outSmall); err != nil {
		t.Fatalf("Fill small value: %v", err)
	}
	if outSmall.V != 42 {
		t.Fatalf("in-range narrowing: got %d, want 42", outSmall.V)
	}

	// 子情况 B：值 (1<<40) 超出 int32 范围 —— Fill 应 loud-fail，不静默丢高位。
	const big int64 = 1 << 40 // 2^40，远大于 MaxInt32 (2^31-1)
	storedBig := oldDef.Extract(&oldRec{V: big})
	bytesBig, _ := schema.EncodeRow(storedBig)
	decodedBig, _ := schema.DecodeRow(bytesBig)
	outBig := &newRec{}
	err := newDef.Fill(decodedBig, outBig)
	if err == nil {
		t.Fatalf("narrowing int64(%d) → int32 should fail, got nil err; filled V=%d", big, outBig.V)
	}
	if !errors.Is(err, schema.ErrIntOverflow) {
		t.Fatalf("error should wrap ErrIntOverflow, got %v", err)
	}
	t.Logf("narrowing int64(%d) → int32 correctly failed: %v", big, err)
}
