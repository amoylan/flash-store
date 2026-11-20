package store

import (
	"strings"
	"testing"

	"github.com/amoylan/flash-store/store/index"
	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/schema"
	"github.com/amoylan/flash-store/store/wal"
)

// Schema 演进端到端场景：v1 时期写下的 Row（只带 Num=1）在 shard 层应该按
// 当前 v2 schema 里新增的 indexed 字段 Default 进索引——而不是因为 Row 里没
// 这个 Num 就跳过。这个测试直接走 table.apply + 索引 Lookup，覆盖 shard 层
// addRowToIndexes 的修复。

type evolveV2Rec struct {
	ID     int64  `store:"id,num=1"`
	Status string `store:"status,num=2,index,default=open"`
}

func TestShard_NewIndexedFieldWithDefault_CoversHistoricalRows(t *testing.T) {
	kind, err := registry.NewKind("shardevo", "rec")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.RegisterStruct[evolveV2Rec](kind); err != nil {
		t.Fatal(err)
	}
	def, _ := registry.GetTypedDef(kind)

	// 手工构造一条 v1 时期的 Row：只有 ID (Num=1)，没有 Status——
	// 对应从 v1 snapshot / v1 WAL 解码出来的历史数据。
	historicalRow := schema.Row{
		Version: 1,
		Fields:  []schema.Field{{Num: 1, Value: schema.NewInt64(42)}},
	}

	// 应用到 shard（模拟 snapshot restore 或 WAL replay 的 apply 路径）。
	sh := newShard(0)
	_, err = sh.apply([]identifiedCmds{{
		identity: 1,
		cmds: []wal.Cmd{
			{Op: wal.OpSet, Key: "42", Kind: kind, Row: historicalRow},
		},
	}})
	if err != nil {
		t.Fatalf("shard.apply: v1-style row: %v", err)
	}

	// 验证历史 Row 按 default 值 "open" 进了 status 索引。
	tbl := sh.tables[kind]
	if tbl == nil {
		t.Fatal("table not created")
	}
	tree, ok := tbl.indexes[schema.FieldName("status")]
	if !ok {
		t.Fatal("status index tree not created")
	}
	// status 的 default = "open"
	statusSpec, ok := def.FieldByNum(2)
	if !ok {
		t.Fatal("status (num=2) missing in def")
	}
	defaultValue := statusSpec.Default
	set, err := tree.Lookup(index.Ranges{index.NewEqualRange(defaultValue)})
	if err != nil {
		t.Fatalf("index lookup: %v", err)
	}
	if _, has := set["42"]; !has {
		t.Fatalf("historical row 42 not in index under default 'open'; index=%v", set)
	}
}

// Schema drift：字段类型被改了却没做迁移，老 Row 带 Int32 值被加进声明为
// String 的索引——混合类型会污染 B-tree 比较器。indexedFieldValue 应提前 panic。
type driftV2Rec struct {
	ID     int64  `store:"id,num=1"`
	Status string `store:"status,num=2,index"` // 代码里是 string，但构造一个 Int32 存储的 Row 模拟 drift
}

func TestShard_SchemaDrift_PanicsOnTypeMismatch(t *testing.T) {
	kind, _ := registry.NewKind("drift", "rec")
	if _, err := registry.RegisterStruct[driftV2Rec](kind); err != nil {
		t.Fatal(err)
	}

	// 构造一条"历史" Row，在 Num=2 存了 Int32 值——模拟代码把字段从
	// int32 改成 string 之后 restart，snapshot 里还是老 Row 的现场。
	driftRow := schema.Row{
		Version: 1,
		Fields: []schema.Field{
			{Num: 1, Value: schema.NewInt64(42)},
			{Num: 2, Value: schema.NewInt32(7)}, // ⚠ schema 声明为 String
		},
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on schema drift (Int32 stored vs String declared)")
		}
		msg, _ := r.(string)
		if msg == "" {
			t.Fatalf("panic value not string: %T %v", r, r)
		}
		// 错误信息要提示字段名、Num、两种类型，便于运维定位。
		for _, needle := range []string{"schema drift", "status", "int32", "string"} {
			if !strings.Contains(msg, needle) {
				t.Errorf("panic message missing %q: %s", needle, msg)
			}
		}
	}()

	sh := newShard(0)
	_, _ = sh.apply([]identifiedCmds{{
		identity: 1,
		cmds: []wal.Cmd{
			{Op: wal.OpSet, Key: "42", Kind: kind, Row: driftRow},
		},
	}})
}

// 演进后删除/更新历史 Row 也要对称工作——add 用 default 进，remove 必须
// 用 default 出，否则留脏索引项。
func TestShard_HistoricalRowUpdateThenDelete_IndexCleansUp(t *testing.T) {
	kind, err := registry.NewKind("shardevo", "update-delete")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.RegisterStruct[evolveV2Rec](kind); err != nil {
		t.Fatal(err)
	}
	def, _ := registry.GetTypedDef(kind)

	sh := newShard(0)

	// 1. 先放一条 v1 历史 Row（Status 用 default "open" 进索引）。
	shortRow := schema.Row{Version: 1, Fields: []schema.Field{{Num: 1, Value: schema.NewInt64(7)}}}
	_, _ = sh.apply([]identifiedCmds{{
		identity: 1,
		cmds:     []wal.Cmd{{Op: wal.OpSet, Key: "7", Kind: kind, Row: shortRow}},
	}})

	// 2. 业务更新：写入完整 v2 Row，Status="filled"。
	fullRow := schema.Row{
		Version: 2,
		Fields: []schema.Field{
			{Num: 1, Value: schema.NewInt64(7)},
			{Num: 2, Value: schema.NewString("filled")},
		},
	}
	_, _ = sh.apply([]identifiedCmds{{
		identity: 1,
		cmds:     []wal.Cmd{{Op: wal.OpSet, Key: "7", Kind: kind, Row: fullRow}},
	}})

	// 索引里 "open" 应该已无 key=7（remove 时用 default 回查正确命中），
	// "filled" 应该新有 key=7。
	statusSpec, _ := def.FieldByNum(2)
	tree := sh.tables[kind].indexes[schema.FieldName("status")]
	openResult, _ := tree.Lookup(index.Ranges{index.NewEqualRange(statusSpec.Default)})
	if _, still := openResult["7"]; still {
		t.Fatalf("stale 'open' index entry for key 7 after update (remove did not use default)")
	}
	filledResult, _ := tree.Lookup(index.Ranges{index.NewEqualRange(schema.NewString("filled"))})
	if _, has := filledResult["7"]; !has {
		t.Fatalf("updated 'filled' index entry for key 7 missing")
	}

	// 3. 删除——索引里应完全没有 key=7。
	_, _ = sh.apply([]identifiedCmds{{
		identity: 1,
		cmds:     []wal.Cmd{{Op: wal.OpDelete, Key: "7", Kind: kind}},
	}})
	filledAfterDel, _ := tree.Lookup(index.Ranges{index.NewEqualRange(schema.NewString("filled"))})
	if _, still := filledAfterDel["7"]; still {
		t.Fatalf("stale 'filled' index entry for key 7 after delete")
	}
}
