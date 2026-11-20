package registry

import (
	"testing"
	"time"

	"github.com/amoylan/flash-store/store/schema"
	"github.com/shopspring/decimal"
)

type defTestRecordV1 struct {
	ID     int64
	Status int32
	Price  decimal.Decimal
}

// defTestRecordV2 增加了 Note 字段，演示新增字段 + Default 的演进场景。
type defTestRecordV2 struct {
	ID     int64
	Status int32
	Price  decimal.Decimal
	Note   string
}

func mustKind(t *testing.T, domain, name string) Kind {
	t.Helper()
	k, err := NewKind(domain, name)
	if err != nil {
		t.Fatalf("NewKind: %v", err)
	}
	return k
}

// buildV1 构造 v1 schema：ID(1) / Status(2) / Price(3)（Price 带 8 位精度约束）。
// 每个字段显式分配 Num——身份稳定、便于演进对比。
func buildV1(t *testing.T, kind Kind) *Def[defTestRecordV1] {
	t.Helper()
	return For[defTestRecordV1](kind).
		Int64(1, "id", func(r *defTestRecordV1) *int64 { return &r.ID }).
		Int32(2, "status", func(r *defTestRecordV1) *int32 { return &r.Status }, schema.Indexed()).
		Decimal(3, "price", func(r *defTestRecordV1) *decimal.Decimal { return &r.Price },
			schema.Indexed(), schema.DecimalPrec(8)).
		Build()
}

// buildV2ForV2 构造 v2 schema，目标类型就是 v2：多出 note(4)，Default = "n/a"。
func buildV2ForV2(t *testing.T, kind Kind) *Def[defTestRecordV2] {
	t.Helper()
	return For[defTestRecordV2](kind).
		Int64(1, "id", func(r *defTestRecordV2) *int64 { return &r.ID }).
		Int32(2, "status", func(r *defTestRecordV2) *int32 { return &r.Status }, schema.Indexed()).
		Decimal(3, "price", func(r *defTestRecordV2) *decimal.Decimal { return &r.Price },
			schema.Indexed(), schema.DecimalPrec(8)).
		String(4, "note", func(r *defTestRecordV2) *string { return &r.Note },
			schema.Default("n/a")).
		Build()
}

// buildV2ForV1 构造 v2 schema，目标类型是 v1（缺 Note 字段）：note 的 getter 传 nil，
// 对应"partial Get"——读回时 Note 位置被静默跳过；写入时 Extract 会用 Default "n/a" 填。
func buildV2ForV1(t *testing.T, kind Kind) *Def[defTestRecordV1] {
	t.Helper()
	return For[defTestRecordV1](kind).
		Int64(1, "id", func(r *defTestRecordV1) *int64 { return &r.ID }).
		Int32(2, "status", func(r *defTestRecordV1) *int32 { return &r.Status }, schema.Indexed()).
		Decimal(3, "price", func(r *defTestRecordV1) *decimal.Decimal { return &r.Price },
			schema.Indexed(), schema.DecimalPrec(8)).
		String(4, "note", nil, schema.Default("n/a")).
		Build()
}

func TestDef_ExtractAndFillRoundTrip(t *testing.T) {
	kind := mustKind(t, "deftest", "roundtrip")
	def := buildV1(t, kind)

	// 模拟 store 启动期赋版本：测试里隔离调用，显式 SetVersion。
	def.SetVersion(1)
	in := &defTestRecordV1{ID: 42, Status: 1, Price: decimal.RequireFromString("12.345678912345")}
	row := def.Extract(in)

	if row.Version != 1 {
		t.Fatalf("Row.Version = %d, want 1", row.Version)
	}
	wantPrice := decimal.RequireFromString("12.34567891")
	priceFV, ok := row.GetByNum(3)
	if !ok {
		t.Fatal("price (num=3) missing from row")
	}
	if got, _ := priceFV.Decimal(); !got.Equal(wantPrice) {
		t.Fatalf("Price truncated = %s, want %s (DecimalPrec=8)", got, wantPrice)
	}

	out := &defTestRecordV1{}
	if err := def.Fill(row, out); err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if out.ID != 42 || out.Status != 1 {
		t.Fatalf("filled struct = %+v", out)
	}
	if !out.Price.Equal(wantPrice) {
		t.Fatalf("filled Price = %s, want %s", out.Price, wantPrice)
	}
}

func TestDef_CloneIsDeep(t *testing.T) {
	kind := mustKind(t, "deftest", "clone")
	def := buildV1(t, kind)

	orig := &defTestRecordV1{ID: 1, Status: 9, Price: decimal.NewFromInt(100)}
	dup := def.Clone(orig)
	if dup == orig {
		t.Fatalf("Clone returned same pointer")
	}
	dup.Status = 7
	if orig.Status == 7 {
		t.Fatalf("mutating clone leaked to original — not a deep copy")
	}
}

func TestDef_SchemaEvolutionFillDefault(t *testing.T) {
	// 构造一个只有 3 个字段的"旧 Row"（v1 写入的场景），
	// 用 v2 schema 的 Fill 读回 —— note 应该被填为 Default "n/a"。
	kind := mustKind(t, "deftest", "evolve")
	defV2 := buildV2ForV2(t, kind)

	oldRow := schema.Row{
		Version: 1,
		Fields: []schema.Field{
			{Num: 1, Value: schema.NewFieldValue[int64](7)},
			{Num: 2, Value: schema.NewFieldValue[int32](2)},
			{Num: 3, Value: schema.NewFieldValue(decimal.NewFromInt(50))},
			// Num=4 (note) 缺失 —— v1 writer schema 不含
		},
	}

	out := &defTestRecordV2{}
	if err := defV2.Fill(oldRow, out); err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if out.ID != 7 || out.Status != 2 || out.Price.String() != "50" {
		t.Fatalf("filled = %+v", out)
	}
	if out.Note != "n/a" {
		t.Fatalf("Note = %q, want %q (Default)", out.Note, "n/a")
	}
}

func TestDef_PartialGet_TargetMissingField(t *testing.T) {
	// 目标 struct 是 v1（不带 Note），用 v2 schema 读。
	// buildV2ForV1 对 note 字段传 nil getter ⇒ Binder.Set == nil ⇒ Fill 跳过 Note。
	kind := mustKind(t, "deftest", "partial")
	def := buildV2ForV1(t, kind)

	v2Row := schema.Row{
		Version: 2,
		Fields: []schema.Field{
			{Num: 1, Value: schema.NewFieldValue[int64](100)},
			{Num: 2, Value: schema.NewFieldValue[int32](3)},
			{Num: 3, Value: schema.NewFieldValue(decimal.NewFromInt(1))},
			{Num: 4, Value: schema.NewFieldValue("memo")}, // note 值，但目标不携带
		},
	}

	out := &defTestRecordV1{}
	if err := def.Fill(v2Row, out); err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if out.ID != 100 || out.Status != 3 {
		t.Fatalf("filled = %+v", out)
	}
	// Note 位置被静默跳过——目标类型根本没这个字段，Fill 不报错。
}

func TestDef_PartialExtract_SourceMissingField(t *testing.T) {
	// v1 struct → v2 schema 的 Extract：note 的 getter 是 nil ⇒ Extract 走 Default "n/a"。
	kind := mustKind(t, "deftest", "partial-ext")
	def := buildV2ForV1(t, kind)

	def.SetVersion(2) // 模拟 store 启动期赋版本
	in := &defTestRecordV1{ID: 100, Status: 3, Price: decimal.NewFromInt(1)}
	row := def.Extract(in)

	if row.Version != 2 {
		t.Fatalf("Version = %d, want 2", row.Version)
	}
	if len(row.Fields) != 4 {
		t.Fatalf("len(Fields) = %d, want 4", len(row.Fields))
	}
	noteFV, ok := row.GetByNum(4)
	if !ok {
		t.Fatal("note (num=4) missing from row")
	}
	note, err := noteFV.String()
	if err != nil {
		t.Fatalf("Note get: %v", err)
	}
	if note != "n/a" {
		t.Fatalf("Note = %q, want %q (Default from source-missing)", note, "n/a")
	}
}

func TestDef_RegisterAndLookup(t *testing.T) {
	kind := mustKind(t, "deftest", "bridge")
	def := buildV1(t, kind)

	if err := RegisterDef(def); err != nil {
		t.Fatalf("RegisterDef: %v", err)
	}

	// 按 Kind 拿回擦除 T 的 TypedDef（shard/index 走的路径）。
	td, ok := GetTypedDef(kind)
	if !ok {
		t.Fatalf("GetTypedDef failed after RegisterDef")
	}
	if len(td.Fields()) != 3 || len(td.IndexedFields()) != 2 {
		t.Fatalf("TypedDef Fields=%d IndexedFields=%d", len(td.Fields()), len(td.IndexedFields()))
	}

	// GetDef[T] 回到类型化 Def，业务代码用它做 Extract/Fill/Clone。
	typed, ok := GetDef[defTestRecordV1](kind)
	if !ok {
		t.Fatalf("GetDef[defTestRecordV1] failed")
	}
	orig := &defTestRecordV1{ID: 5, Status: 1, Price: decimal.NewFromInt(8)}
	cloned := typed.Clone(orig)
	if cloned == orig || cloned.Status != 1 {
		t.Fatalf("Clone failed: %+v", cloned)
	}

	// RowView：按 Row + TypedDef 直接取字段。
	row := typed.Extract(orig)
	view := NewRowView(td, row)
	if got, _ := view.GetResult("status").Int32(); got != 1 {
		t.Fatalf("RowView status = %d, want 1", got)
	}
}

// 防回归：Builder 能跑过"无索引"场景。
func TestDef_TimestampDefaultIsZeroTime(t *testing.T) {
	kind := mustKind(t, "deftest", "ts")
	type rec struct{ T time.Time }
	def := For[rec](kind).
		Timestamp(1, "t", func(r *rec) *time.Time { return &r.T }).
		Build()
	row := def.Extract(&rec{})
	tFV, ok := row.GetByNum(1)
	if !ok {
		t.Fatal("timestamp (num=1) missing from row")
	}
	got, err := tFV.Time()
	if err != nil {
		t.Fatalf("Time: %v", err)
	}
	if !got.IsZero() {
		t.Fatalf("zero-value Time = %v", got)
	}
}

// Builder 拒绝：num <= 0、duplicate num、duplicate name。
func TestBuilder_RejectsBadNum(t *testing.T) {
	cases := []struct {
		name  string
		build func()
	}{
		{
			"num=0",
			func() {
				kind, _ := NewKind("builder", "zero")
				For[defTestRecordV1](kind).
					Int64(0, "id", func(r *defTestRecordV1) *int64 { return &r.ID }).
					Build()
			},
		},
		{
			"negative num",
			func() {
				kind, _ := NewKind("builder", "neg")
				For[defTestRecordV1](kind).
					Int64(-1, "id", func(r *defTestRecordV1) *int64 { return &r.ID }).
					Build()
			},
		},
		{
			"duplicate num",
			func() {
				kind, _ := NewKind("builder", "dupnum")
				For[defTestRecordV1](kind).
					Int64(1, "id", func(r *defTestRecordV1) *int64 { return &r.ID }).
					Int32(1, "status", func(r *defTestRecordV1) *int32 { return &r.Status }).
					Build()
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic")
				}
			}()
			c.build()
		})
	}
}

// Retire 声明 + 新字段撞 Num：注册期立刻 panic，而不是延迟到 master 启动期。
func TestBuilder_Retire_ConflictsCaughtAtRegisterTime(t *testing.T) {
	type rec struct{ ID int64 }
	cases := []struct {
		name  string
		build func()
	}{
		{
			"Retire after addField with same num",
			func() {
				kind, _ := NewKind("retire", "after-add")
				For[rec](kind).
					Int64(5, "id", func(r *rec) *int64 { return &r.ID }).
					Retire(schema.RetiredField{Num: 5, Name: "id", Type: schema.FieldTypeInt64,
						Default: schema.ZeroDefault(schema.FieldTypeInt64)}).
					Build()
			},
		},
		{
			"addField after Retire with same num",
			func() {
				kind, _ := NewKind("retire", "add-after-retire")
				For[rec](kind).
					Retire(schema.RetiredField{Num: 5, Name: "old", Type: schema.FieldTypeInt32,
						Default: schema.ZeroDefault(schema.FieldTypeInt32)}).
					Int64(5, "id", func(r *rec) *int64 { return &r.ID }).
					Build()
			},
		},
		{
			"Retire with num=0",
			func() {
				kind, _ := NewKind("retire", "zero")
				For[rec](kind).Retire(schema.RetiredField{Num: 0, Type: schema.FieldTypeInt32}).Build()
			},
		},
		{
			"Retire with Type=Undefined",
			func() {
				kind, _ := NewKind("retire", "undef")
				For[rec](kind).Retire(schema.RetiredField{Num: 3}).Build()
			},
		},
		{
			"Retire twice with conflicting Type",
			func() {
				kind, _ := NewKind("retire", "twice")
				For[rec](kind).
					Retire(schema.RetiredField{Num: 3, Name: "x", Type: schema.FieldTypeInt32,
						Default: schema.ZeroDefault(schema.FieldTypeInt32)}).
					Retire(schema.RetiredField{Num: 3, Name: "x", Type: schema.FieldTypeString,
						Default: schema.NewString("")}).
					Build()
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic")
				}
			}()
			c.build()
		})
	}
}

// Retire 幂等：同 Num 重复声明且 Type/Default/Name 都一致 → 不 panic。
func TestBuilder_Retire_IdempotentSameContent(t *testing.T) {
	type rec struct{ ID int64 }
	kind, _ := NewKind("retire", "idem")
	rf := schema.RetiredField{Num: 2, Name: "old", Type: schema.FieldTypeInt32,
		Default: schema.ZeroDefault(schema.FieldTypeInt32)}
	def := For[rec](kind).
		Retire(rf).
		Retire(rf). // 完全相同，允许
		Int64(1, "id", func(r *rec) *int64 { return &r.ID }).
		Build()
	if got := def.RetiredFields(); len(got) != 1 || got[0].Num != 2 {
		t.Fatalf("RetiredFields = %+v, want one entry with Num=2", got)
	}
}

// RegisterStruct 的 retired 参数：struct 里有 num=2，但 Retired{Num=2} 也传进来 → panic（alive 冲突）。
func TestRegisterStruct_Retired_ConflictsWithAlive(t *testing.T) {
	type rec struct {
		ID     int64 `store:"id,num=1"`
		Status int32 `store:"status,num=2,type=int32"`
	}
	kind, _ := NewKind("retire", "struct-conflict")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when Retired Num overlaps alive field")
		}
	}()
	_, _ = BuildFromStruct[rec](kind, schema.RetiredField{
		Num: 2, Name: "status", Type: schema.FieldTypeInt32,
		Default: schema.ZeroDefault(schema.FieldTypeInt32),
	})
}

// RegisterStruct 的 retired 参数：struct 里新字段尝试用已 retired 的 Num → panic。
func TestRegisterStruct_Retired_PreventsNumReuseInNewField(t *testing.T) {
	type rec struct {
		ID    int64  `store:"id,num=1"`
		Label string `store:"label,num=5"` // 试图用已退休的 Num=5
	}
	kind, _ := NewKind("retire", "reuse-prevent")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when new field uses retired Num")
		}
	}()
	_, _ = BuildFromStruct[rec](kind, schema.RetiredField{
		Num: 5, Name: "flag", Type: schema.FieldTypeInt32,
		Default: schema.ZeroDefault(schema.FieldTypeInt32),
	})
}

// Reorder-free：Go struct 字段顺序和 Num 无关。这里声明顺序 3,1,2 然后验证 Num 身份。
func TestBuilder_NumIdentityDecoupledFromDeclarationOrder(t *testing.T) {
	kind := mustKind(t, "builder", "reorder")
	def := For[defTestRecordV1](kind).
		Decimal(3, "price", func(r *defTestRecordV1) *decimal.Decimal { return &r.Price }).
		Int64(1, "id", func(r *defTestRecordV1) *int64 { return &r.ID }).
		Int32(2, "status", func(r *defTestRecordV1) *int32 { return &r.Status }).
		Build()

	def.SetVersion(1)
	in := &defTestRecordV1{ID: 42, Status: 5, Price: decimal.NewFromInt(100)}
	row := def.Extract(in)

	// Extract 沿声明顺序，但 Fill 靠 Num 正确回填 struct。
	out := &defTestRecordV1{}
	if err := def.Fill(row, out); err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if out.ID != 42 || out.Status != 5 || !out.Price.Equal(decimal.NewFromInt(100)) {
		t.Fatalf("round-trip with reordered declaration failed: %+v", out)
	}
}
