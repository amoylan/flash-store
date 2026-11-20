package store

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/schema"
	"github.com/amoylan/flash-store/store/snapshot"
	"github.com/shopspring/decimal"
)

// 覆盖 snapshot_manager.go 的 checkSchemasCompat。
//
// 新语义（Num 身份 + 全历史遍历）：
//   - 参数 newSchemas 是"当前代码的 schema"（registry.AllDescriptors()）
//   - 对比基准是 store.schemasByKind 里累积的**全部历史版本**
//   - newDesc 需要对每个历史版本都 compat（拦截跨非邻接版本 Num 复用）
//   - Num 相同的字段必须同 Type + 同 Default；否则 loud-fail
//   - 改名 / 删字段 / 重排字段（Num 不变即可）—— 全部允许

func newTestStoreForCompat(cached []schema.KindDescriptor) *Store {
	s := &Store{schemasByKind: make(map[registry.Kind]*fieldsRegistry)}
	s.schemasByKindMu = sync.RWMutex{}
	// 测试 fixture 传入的 cached 可能模拟"历史多个版本"场景：此时每条
	// KindDescriptor 依次 UpsertSchema，最终 reg 反映"最后一个 desc 的 alive
	// + 沿途被 drop 的 tombstone"，这正是 numHistory 的语义。
	for _, d := range cached {
		s.UpsertSchema(d)
	}
	return s
}

func TestCheckSchemasCompat_NilNewSchemasAllowed(t *testing.T) {
	s := newTestStoreForCompat(nil)
	if err := s.checkSchemasCompat(nil); err != nil {
		t.Fatalf("nil newSchemas should be allowed: %v", err)
	}
}

func TestCheckSchemasCompat_NewKindAllowed(t *testing.T) {
	s := newTestStoreForCompat(nil)
	newSchemas := []schema.KindDescriptor{{
		Kind:    "new.kind",
		Version: 1,
		Fields: []schema.FieldDescriptor{
			{Num: 1, Name: "id", Type: schema.FieldTypeInt64, Default: schema.ZeroDefault(schema.FieldTypeInt64)},
		},
	}}
	if err := s.checkSchemasCompat(newSchemas); err != nil {
		t.Fatalf("new kind without cached baseline should be allowed: %v", err)
	}
}

func TestCheckSchemasCompat_CompatibleEvolution(t *testing.T) {
	// 缓存：v1 有 id(1) + status(2)；新代码新增 note(3)，status 加 index。
	kind, _ := registry.NewKind("schtest", "compat-ok")
	cached := []schema.KindDescriptor{{
		Kind:    kind.String(),
		Version: 1,
		Fields: []schema.FieldDescriptor{
			{Num: 1, Name: "id", Type: schema.FieldTypeInt64, Default: schema.ZeroDefault(schema.FieldTypeInt64)},
			{Num: 2, Name: "status", Type: schema.FieldTypeInt32, Default: schema.ZeroDefault(schema.FieldTypeInt32)},
		},
	}}
	s := newTestStoreForCompat(cached)

	newSchemas := []schema.KindDescriptor{{
		Kind:    kind.String(),
		Version: 2,
		Fields: []schema.FieldDescriptor{
			{Num: 1, Name: "id", Type: schema.FieldTypeInt64, Default: schema.ZeroDefault(schema.FieldTypeInt64)},
			{Num: 2, Name: "status", Type: schema.FieldTypeInt32, Indexed: true, Default: schema.ZeroDefault(schema.FieldTypeInt32)},
			{Num: 3, Name: "note", Type: schema.FieldTypeString, Default: schema.NewString("n/a")},
		},
	}}
	if err := s.checkSchemasCompat(newSchemas); err != nil {
		t.Fatalf("append-field evolution should be compat: %v", err)
	}
}

func TestCheckSchemasCompat_TypeChangeRejected(t *testing.T) {
	kind, _ := registry.NewKind("schtest", "typechange")
	cached := []schema.KindDescriptor{{
		Kind:    kind.String(),
		Version: 1,
		Fields: []schema.FieldDescriptor{
			{Num: 1, Name: "status", Type: schema.FieldTypeInt32, Indexed: true, Default: schema.ZeroDefault(schema.FieldTypeInt32)},
		},
	}}
	s := newTestStoreForCompat(cached)

	newSchemas := []schema.KindDescriptor{{
		Kind:    kind.String(),
		Version: 2,
		Fields: []schema.FieldDescriptor{
			{Num: 1, Name: "status", Type: schema.FieldTypeString, Indexed: true, Default: schema.NewString("")},
		},
	}}
	err := s.checkSchemasCompat(newSchemas)
	if err == nil {
		t.Fatal("same Num, type change int32→string should be rejected")
	}
	for _, needle := range []string{"type changed", "status", "int32", "string"} {
		if !strings.Contains(err.Error(), needle) {
			t.Errorf("error missing %q: %s", needle, err)
		}
	}
}

// 改名（同 Num）——现在**允许**（num-identity 设计的核心优点）。
func TestCheckSchemasCompat_RenameAllowed(t *testing.T) {
	kind, _ := registry.NewKind("schtest", "rename-ok")
	cached := []schema.KindDescriptor{{
		Kind:    kind.String(),
		Version: 1,
		Fields: []schema.FieldDescriptor{
			{Num: 1, Name: "id", Type: schema.FieldTypeString, Default: schema.NewString("")},
		},
	}}
	s := newTestStoreForCompat(cached)
	newSchemas := []schema.KindDescriptor{{
		Kind:    kind.String(),
		Version: 2,
		Fields: []schema.FieldDescriptor{
			{Num: 1, Name: "order_id", Type: schema.FieldTypeString, Default: schema.NewString("")},
		},
	}}
	if err := s.checkSchemasCompat(newSchemas); err != nil {
		t.Fatalf("rename with stable Num should be allowed, got: %v", err)
	}
}

// （原 UndeclaredAliveShrinkageRejected 与 FieldDroppedWithRetireAllowed 已移除：
//   - 删字段不再要求显式声明 RetiredField；rollback / 删字段都走"alive 缩水"路径
//     由 fieldsRegistry.upsert 自动把消失的 alive 迁到 tombstone，静默接受。
//   - 端到端行为由 TestCheckSchemasCompat_AliveShrinkageAllowed
//     和 TestCheckSchemasCompat_FieldResurrectionAllowed 共同覆盖。）

// 跨非邻接版本的 Num 复用必须被全历史遍历拦截。
//
//	v1: Num=5 是 Int32
//	v2: Num=5 被删除（允许）
//	v3: Num=5 重新加回，Type=String —— 对比 v2 是"新字段"（相邻允许），
//	    对比 v1 是"Type 变了"（必须拒绝）。
func TestCheckSchemasCompat_NumReuseAcrossHistoryRejected(t *testing.T) {
	kind, _ := registry.NewKind("schtest", "numreuse")
	cached := []schema.KindDescriptor{
		// v1 — Num=5 是 int32
		{
			Kind:    kind.String(),
			Version: 1,
			Fields: []schema.FieldDescriptor{
				{Num: 1, Name: "id", Type: schema.FieldTypeInt64, Default: schema.ZeroDefault(schema.FieldTypeInt64)},
				{Num: 5, Name: "old_flag", Type: schema.FieldTypeInt32, Default: schema.ZeroDefault(schema.FieldTypeInt32)},
			},
		},
		// v2 — Num=5 被删除
		{
			Kind:    kind.String(),
			Version: 2,
			Fields: []schema.FieldDescriptor{
				{Num: 1, Name: "id", Type: schema.FieldTypeInt64, Default: schema.ZeroDefault(schema.FieldTypeInt64)},
			},
		},
	}
	s := newTestStoreForCompat(cached)

	// v3 试图把 Num=5 作为 string 加回——v1 里是 int32，必须拒绝。
	newSchemas := []schema.KindDescriptor{{
		Kind:    kind.String(),
		Version: 3,
		Fields: []schema.FieldDescriptor{
			{Num: 1, Name: "id", Type: schema.FieldTypeInt64, Default: schema.ZeroDefault(schema.FieldTypeInt64)},
			{Num: 5, Name: "new_label", Type: schema.FieldTypeString, Default: schema.NewString("")},
		},
	}}
	err := s.checkSchemasCompat(newSchemas)
	if err == nil {
		t.Fatal("cross-version Num reuse (int32 → string) should be rejected")
	}
	// 新的 tombstone 错误信息显式提及"Num reuse with different type forbidden"。
	for _, needle := range []string{"num=5", "int32", "string", "forbidden"} {
		if !strings.Contains(err.Error(), needle) {
			t.Fatalf("error should mention %q: %v", needle, err)
		}
	}
}

// 场景：snapshot cache 里有 tombstone（Num=5 历史上被删），当前代码**没有** Retired{Num:5}
// 声明——允许通过（典型场景：代码被 rollback 到更早版本，老代码不知道该字段）。
// 保护机制是第 3 步——如果 Num=5 被作为不同 Type/Default 的字段加回来才会拒绝。
func TestCheckSchemasCompat_TombstoneWithoutDeclarationAllowed(t *testing.T) {
	kind, _ := registry.NewKind("schtest", "undeclared-tombstone")
	// cache: v1 有 {1, 5}, v2 删除 5 → alive={1}, tombstone={5}
	cached := []schema.KindDescriptor{
		{
			Kind:    kind.String(),
			Version: 1,
			Fields: []schema.FieldDescriptor{
				{Num: 1, Name: "id", Type: schema.FieldTypeInt64, Default: schema.ZeroDefault(schema.FieldTypeInt64)},
				{Num: 5, Name: "flag", Type: schema.FieldTypeInt32, Default: schema.ZeroDefault(schema.FieldTypeInt32)},
			},
		},
		{
			Kind:    kind.String(),
			Version: 2,
			Fields: []schema.FieldDescriptor{
				{Num: 1, Name: "id", Type: schema.FieldTypeInt64, Default: schema.ZeroDefault(schema.FieldTypeInt64)},
			},
		},
	}
	s := newTestStoreForCompat(cached)

	// 新代码: alive = {1}, 不声明 Retired — 不是错误，只是代码不知道 Num=5 的历史
	newSchemas := []schema.KindDescriptor{{
		Kind:    kind.String(),
		Version: 2,
		Fields: []schema.FieldDescriptor{
			{Num: 1, Name: "id", Type: schema.FieldTypeInt64, Default: schema.ZeroDefault(schema.FieldTypeInt64)},
		},
	}}
	if err := s.checkSchemasCompat(newSchemas); err != nil {
		t.Fatalf("tombstone-without-declaration should pass (rollback friendliness): %v", err)
	}
}

// 场景：rollback 到不知道 Num=C 的老代码。cache alive 里还有 C，新 code 里没有。
// 允许通过；C 会在下一次 UpsertSchema 时自动迁到 tombstone。
func TestCheckSchemasCompat_AliveShrinkageAllowed(t *testing.T) {
	kind, _ := registry.NewKind("schtest", "rollback-friendly")
	cached := []schema.KindDescriptor{{
		Kind:    kind.String(),
		Version: 2,
		Fields: []schema.FieldDescriptor{
			{Num: 1, Name: "id", Type: schema.FieldTypeInt64, Default: schema.ZeroDefault(schema.FieldTypeInt64)},
			{Num: 2, Name: "status", Type: schema.FieldTypeInt32, Default: schema.ZeroDefault(schema.FieldTypeInt32)},
		},
	}}
	s := newTestStoreForCompat(cached)

	// v1 代码 rollback 上来，struct 只有 id，不知道 status
	newSchemas := []schema.KindDescriptor{{
		Kind:    kind.String(),
		Version: 3,
		Fields: []schema.FieldDescriptor{
			{Num: 1, Name: "id", Type: schema.FieldTypeInt64, Default: schema.ZeroDefault(schema.FieldTypeInt64)},
		},
	}}
	if err := s.checkSchemasCompat(newSchemas); err != nil {
		t.Fatalf("alive shrinkage on rollback should be allowed: %v", err)
	}
}

// 复活已删除字段：同 Type/Default 的 Num 可以从 tombstone 搬回 alive。
// 验证 UpsertSchema 会自动把它从 tombstone 清掉。
func TestCheckSchemasCompat_FieldResurrectionAllowed(t *testing.T) {
	kind, _ := registry.NewKind("schtest", "resurrect")
	cached := []schema.KindDescriptor{
		{
			Kind:    kind.String(),
			Version: 1,
			Fields: []schema.FieldDescriptor{
				{Num: 1, Name: "id", Type: schema.FieldTypeInt64, Default: schema.ZeroDefault(schema.FieldTypeInt64)},
				{Num: 3, Name: "note", Type: schema.FieldTypeString, Default: schema.NewString("")},
			},
		},
		{
			// 删除 note
			Kind:    kind.String(),
			Version: 2,
			Fields: []schema.FieldDescriptor{
				{Num: 1, Name: "id", Type: schema.FieldTypeInt64, Default: schema.ZeroDefault(schema.FieldTypeInt64)},
			},
		},
	}
	s := newTestStoreForCompat(cached)

	// 确认 tombstone 含 Num=3
	reg := s.schemasByKind[kind]
	if _, has := reg.tombstone[3]; !has {
		t.Fatalf("precondition: tombstone should contain Num=3, got %+v", reg.tombstone)
	}

	// v3 把 Num=3 加回来（完全同 Type/Default）
	resurrected := []schema.KindDescriptor{{
		Kind:    kind.String(),
		Version: 3,
		Fields: []schema.FieldDescriptor{
			{Num: 1, Name: "id", Type: schema.FieldTypeInt64, Default: schema.ZeroDefault(schema.FieldTypeInt64)},
			{Num: 3, Name: "note", Type: schema.FieldTypeString, Default: schema.NewString("")},
		},
	}}
	if err := s.checkSchemasCompat(resurrected); err != nil {
		t.Fatalf("same-Type/Default field resurrection should pass: %v", err)
	}

	// Compat 通过后，模拟 UpsertSchema 合并 — tombstone 应自动清理 Num=3
	s.UpsertSchema(resurrected[0])
	reg = s.schemasByKind[kind]
	if _, has := reg.tombstone[3]; has {
		t.Fatalf("after resurrection, tombstone must drop Num=3; got %+v", reg.tombstone)
	}
	if _, has := reg.alive[3]; !has {
		t.Fatalf("after resurrection, alive must contain Num=3; got %+v", reg.alive)
	}
}

// （TombstoneWithDeclarationAccepted / RetiredDeclarationMismatchesTombstone 已移除：
// RetiredField 不再喂入 KindDescriptor，checkCompatAgainstRegistry 不再做
// newDesc.DroppedFields vs tombstone 的对账。Builder.Retire 在源码层做同包内
// Num 撞车检测，已由 store/registry/def_test.go 覆盖。）

// Tombstone 通过 snapshot header 持久化后，restore 回来仍能拦截 Num 复用。
// 场景：
//  1. v1 写了 Num=5 int32
//  2. v2 删除 Num=5 —— 此时 tombstone 含 Num=5 int32
//  3. snapshot header 带出 DroppedFields=[Num=5 int32]
//  4. 模拟 master 重启：从 header 的 KindDescriptor（alive + DroppedFields）UpsertSchema
//  5. v3 试图把 Num=5 作为 string 加回 —— 必须被拒
func TestCheckSchemasCompat_TombstoneSurvivesSnapshotRestore(t *testing.T) {
	cd := "schtest.tombstone-restore"
	// Snapshot header 里一条 KindDescriptor，含 alive(id) + dropped(flag at num=5 int32)
	fromSnapshot := schema.KindDescriptor{
		Kind:    cd,
		Version: 2,
		Fields: []schema.FieldDescriptor{
			{Num: 1, Name: "id", Type: schema.FieldTypeInt64, Default: schema.ZeroDefault(schema.FieldTypeInt64)},
		},
		DroppedFields: []schema.FieldDescriptor{
			{Num: 5, Name: "flag", Type: schema.FieldTypeInt32, Default: schema.ZeroDefault(schema.FieldTypeInt32)},
		},
	}
	s := newTestStoreForCompat([]schema.KindDescriptor{fromSnapshot})

	// v3 试图把 Num=5 复用为 string —— 应被拒
	newSchemas := []schema.KindDescriptor{{
		Kind:    cd,
		Version: 3,
		Fields: []schema.FieldDescriptor{
			{Num: 1, Name: "id", Type: schema.FieldTypeInt64, Default: schema.ZeroDefault(schema.FieldTypeInt64)},
			{Num: 5, Name: "label", Type: schema.FieldTypeString, Default: schema.NewString("")},
		},
	}}
	err := s.checkSchemasCompat(newSchemas)
	if err == nil {
		t.Fatal("tombstone from snapshot should still reject Num reuse across restart")
	}
	if !strings.Contains(err.Error(), "num=5") || !strings.Contains(err.Error(), "forbidden") {
		t.Fatalf("error should mention tombstone num reuse: %v", err)
	}
}

// DroppedFields 在 snapshot Header 里 JSON round-trip 保真。
func TestKindDescriptor_DroppedFieldsJSONRoundTrip(t *testing.T) {
	orig := schema.KindDescriptor{
		Kind:    "schtest.dropped-json",
		Version: 5,
		Fields: []schema.FieldDescriptor{
			{Num: 1, Name: "id", Type: schema.FieldTypeInt64},
		},
		DroppedFields: []schema.FieldDescriptor{
			{Num: 7, Name: "old_flag", Type: schema.FieldTypeInt32, Default: schema.ZeroDefault(schema.FieldTypeInt32)},
			{Num: 9, Name: "old_note", Type: schema.FieldTypeString, Default: schema.NewString("gone")},
		},
	}
	data, err := json.Marshal(&orig)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"dropped_fields"`) {
		t.Fatalf("dropped_fields not serialized: %s", data)
	}
	var got schema.KindDescriptor
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.DroppedFields) != 2 {
		t.Fatalf("DroppedFields round-trip lost entries: got %d want 2", len(got.DroppedFields))
	}
	if got.DroppedFields[1].Default.Type() != schema.FieldTypeString {
		t.Fatalf("DroppedFields Default type lost")
	}
}

// 校验 CreateSnapshot 构造的 Header 里 Schemas 字段填进去了、可 JSON round-trip。
func TestSnapshotHeader_SchemasRoundTrip(t *testing.T) {
	type rec struct {
		ID    int64           `store:"id,num=1"`
		Name  string          `store:"name,num=2,index,max=64"`
		Price decimal.Decimal `store:"price,num=3,prec=8"`
	}
	kind, _ := registry.NewKind("schtest", "roundtrip")
	def, err := registry.RegisterStruct[rec](kind)
	if err != nil {
		t.Fatal(err)
	}
	def.SetVersion(7)

	header := snapshot.Header{
		Version:   1,
		NumShards: 1, NumWorkers: 1,
		Schemas: registry.AllDescriptors(),
	}
	data, err := json.Marshal(&header)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"int64"`) || !strings.Contains(string(data), `"decimal"`) {
		t.Fatalf("FieldType not serialized as string: %s", data)
	}

	var got snapshot.Header
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}

	var found *schema.KindDescriptor
	for i := range got.Schemas {
		if got.Schemas[i].Kind == kind.String() {
			found = &got.Schemas[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("kind %s missing from decoded Schemas", kind.String())
	}
	if found.Version != 7 {
		t.Errorf("Version round-trip: got %d want 7", found.Version)
	}
	if len(found.Fields) != 3 {
		t.Fatalf("Fields count: got %d want 3", len(found.Fields))
	}
	// Num 来自 tag (num=1/2/3)，与声明顺序不必一致——这里恰好一致。
	byNum := make(map[int32]schema.FieldDescriptor, len(found.Fields))
	for _, f := range found.Fields {
		byNum[f.Num] = f
	}
	if id, ok := byNum[1]; !ok || id.Name != "id" {
		t.Errorf("num=1 should be id, got %+v", id)
	}
	if name, ok := byNum[2]; !ok || name.Name != "name" || !name.Indexed || name.StringMaxLen != 64 {
		t.Errorf("Name field lost metadata: %+v", name)
	}
	if price, ok := byNum[3]; !ok || price.Type != schema.FieldTypeDecimal || price.DecimalPrecision != 8 {
		t.Errorf("Price field lost prec: %+v", price)
	}
}
