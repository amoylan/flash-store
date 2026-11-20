package registry

import (
	"testing"

	"github.com/amoylan/flash-store/store/schema"
)

// 场景：master 运行中历史数据已经落盘（snapshot / WAL 里有一堆 v1 写的 Row），
// 下次重启升级到 v2 schema——多了一个字段、给了 non-zero default。
// 验证：v1 Row 经 DecodeRow + v2 Def.Fill 出来，新字段填的是 default，
// 不是 Go zero value。老字段照旧回来。
//
// Num 语义：v1 字段的 Num 必须在 v2 里保留不变；v2 新增字段分配全新 Num。

// ---- 字符串非零默认值 ----

type v1RecStr struct {
	ID int64 `store:"id,num=1"`
}

type v2RecStr struct {
	ID     int64  `store:"id,num=1"`
	Status string `store:"status,num=2,default=open"` // 新字段 + non-zero default
}

func TestEvolve_NewField_StringNonZeroDefault(t *testing.T) {
	kindV1, _ := NewKind("evo", "newfield-str-v1")
	kindV2, _ := NewKind("evo", "newfield-str-v2")
	v1Def, _ := BuildFromStruct[v1RecStr](kindV1)
	v2Def, _ := BuildFromStruct[v2RecStr](kindV2)

	// v1 时刻：写一条记录，EncodeRow → bytes 落盘（snapshot / WAL 都是这份形态）。
	rowV1 := v1Def.Extract(&v1RecStr{ID: 42})
	bytes, err := schema.EncodeRow(rowV1)
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}

	// 验证 v1 Row 里确实只有 1 个字段。
	if len(rowV1.Fields) != 1 {
		t.Fatalf("v1 row has %d fields, want 1", len(rowV1.Fields))
	}

	// v2 时刻（重启之后）：同一份 bytes 解出来，用 v2 Def Fill 新 struct。
	decoded, err := schema.DecodeRow(bytes)
	if err != nil {
		t.Fatalf("DecodeRow: %v", err)
	}
	if len(decoded.Fields) != 1 {
		t.Fatalf("decoded row has %d fields, want 1 (old data shouldn't grow)", len(decoded.Fields))
	}

	out := &v2RecStr{}
	if err := v2Def.Fill(decoded, out); err != nil {
		t.Fatalf("Fill: %v", err)
	}

	if out.ID != 42 {
		t.Fatalf("historical ID lost: %d", out.ID)
	}
	if out.Status != "open" {
		t.Fatalf("new field default not applied: got %q, want %q (zero value '' would indicate default broken)",
			out.Status, "open")
	}
}

// ---- int32 非零默认值 ----

type v1RecI32 struct {
	ID int64 `store:"id,num=1"`
}

type v2RecI32 struct {
	ID     int64 `store:"id,num=1"`
	Status int32 `store:"status,num=2,default=1"` // 1 = Open
}

func TestEvolve_NewField_Int32NonZeroDefault(t *testing.T) {
	kindV1, _ := NewKind("evo", "newfield-i32-v1")
	kindV2, _ := NewKind("evo", "newfield-i32-v2")
	v1Def, _ := BuildFromStruct[v1RecI32](kindV1)
	v2Def, _ := BuildFromStruct[v2RecI32](kindV2)

	rowV1 := v1Def.Extract(&v1RecI32{ID: 100})
	bytes, _ := schema.EncodeRow(rowV1)

	decoded, _ := schema.DecodeRow(bytes)
	out := &v2RecI32{}
	if err := v2Def.Fill(decoded, out); err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if out.ID != 100 {
		t.Fatalf("ID lost: %d", out.ID)
	}
	if out.Status != 1 {
		t.Fatalf("new int32 field default not applied: got %d, want 1", out.Status)
	}
}

// ---- 老的 Write + 新的 Write 混杂：旧 Row 仍然只有 1 字段，新 Row 有 2 字段 ----
// 这是 master 重启后的真实态：存量 Row 不会自动扩展，新写入是 v2 格式。

func TestEvolve_NewField_MixedOldNewRows(t *testing.T) {
	kindV1, _ := NewKind("evo", "newfield-mixed-v1")
	kindV2, _ := NewKind("evo", "newfield-mixed-v2")
	v1Def, _ := BuildFromStruct[v1RecStr](kindV1)
	v2Def, _ := BuildFromStruct[v2RecStr](kindV2)

	// v1 时期的旧 Row。
	oldRowBytes, _ := schema.EncodeRow(v1Def.Extract(&v1RecStr{ID: 1}))

	// v2 时期的新 Row。
	newRowBytes, _ := schema.EncodeRow(v2Def.Extract(&v2RecStr{ID: 2, Status: "filled"}))

	// 旧 Row 用 v2 Def Fill —— Status 走 default "open"。
	oldDecoded, _ := schema.DecodeRow(oldRowBytes)
	oldOut := &v2RecStr{}
	_ = v2Def.Fill(oldDecoded, oldOut)
	if oldOut.Status != "open" {
		t.Fatalf("old row should fall back to default: got %q", oldOut.Status)
	}

	// 新 Row 用 v2 Def Fill —— Status 保留业务写入的 "filled"。
	newDecoded, _ := schema.DecodeRow(newRowBytes)
	newOut := &v2RecStr{}
	_ = v2Def.Fill(newDecoded, newOut)
	if newOut.Status != "filled" {
		t.Fatalf("new row should keep written value: got %q", newOut.Status)
	}

	t.Logf("mixed state OK: old row has %d fields (default-fill on read), new row has %d fields (real value)",
		len(oldDecoded.Fields), len(newDecoded.Fields))
}

// ---- Row 层面：历史 Row 只有 v1 时期的字段 ----
// 记录一个事实：v1 Row 里只有 Num=1；v2 新增 indexed 字段 Num=2。Num 不在 Row 里
// 时 shard 层 add/removeRowFromIndexes 会用 spec.Default 兜底（见
// store/shard_evolve_test.go 的端到端测试），Fill 侧 default 也覆盖（
// TestEvolve_NewField_StringNonZeroDefault 已验证）。

type v2RecIndexed struct {
	ID     int64  `store:"id,num=1"`
	Status string `store:"status,num=2,index,default=open"`
}

func TestEvolve_NewIndexedField_RowMissingNum(t *testing.T) {
	kindV1, _ := NewKind("evo", "newfield-idx-v1")
	kindV2, _ := NewKind("evo", "newfield-idx-v2")
	v1Def, _ := BuildFromStruct[v1RecStr](kindV1)
	v2Def, _ := BuildFromStruct[v2RecIndexed](kindV2)

	rowV1 := v1Def.Extract(&v1RecStr{ID: 99})

	indexedFields := v2Def.IndexedFields()
	if len(indexedFields) != 1 {
		t.Fatalf("v2 should have 1 indexed field, got %d", len(indexedFields))
	}
	statusNum := indexedFields[0].Num
	if _, has := rowV1.GetByNum(statusNum); has {
		t.Fatalf("unexpected: v1 row contains num=%d (v2 indexed field)", statusNum)
	}
	t.Logf("v1 row has no num=%d (difference absorbed by shard+fill default)", statusNum)

	// Fill 侧的 default 覆盖（shard 侧 default 覆盖见另一个端到端测试）。
	out := &v2RecIndexed{}
	_ = v2Def.Fill(rowV1, out)
	if out.Status != "open" {
		t.Fatalf("Fill on v1 row should apply default: got %q", out.Status)
	}
}
