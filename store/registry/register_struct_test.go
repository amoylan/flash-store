package registry

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/amoylan/flash-store/store/schema"
	"github.com/shopspring/decimal"
)

// 覆盖关键场景：scalar、enum 底层类型、Decimal 精度、Timestamp、Custom、skip。
type tagOrderStatus int32

const (
	tagStatusOpen   tagOrderStatus = 1
	tagStatusFilled tagOrderStatus = 2
)

type tagExtra struct {
	Fee  decimal.Decimal `json:"fee"`
	Tags []string        `json:"tags"`
}

func (e *tagExtra) Value() ([]byte, error) { return json.Marshal(e) }
func (e *tagExtra) Scan(b []byte) error {
	if len(b) == 0 {
		*e = tagExtra{}
		return nil
	}
	return json.Unmarshal(b, e)
}

type taggedOrder struct {
	OrderID    string          `store:"order_id,num=1"`
	Status     tagOrderStatus  `store:"status,num=2,index,type=int32"` // 自定义 ~int32 → 必须显式 type=int32
	ContractID int64           `store:"contract_id,num=3,index"`
	Price      decimal.Decimal `store:"price,num=4,index,prec=8"`
	Vol        decimal.Decimal `store:"vol,num=5,prec=8"`
	CreatedAt  time.Time       `store:"created_at,num=6"`
	Extra      tagExtra        `store:"extra,num=7"`
	Internal   string          `store:"-"`
	untagged   string          // no tag → skip
}

func TestRegisterStruct_RoundTrip(t *testing.T) {
	kind, err := NewKind("tag", "order")
	if err != nil {
		t.Fatalf("NewKind: %v", err)
	}
	def, err := BuildFromStruct[taggedOrder](kind)
	if err != nil {
		t.Fatalf("BuildFromStruct: %v", err)
	}

	in := &taggedOrder{
		OrderID:    "o-1",
		Status:     tagStatusFilled,
		ContractID: 777,
		Price:      decimal.RequireFromString("100.123456789"), // 精度 8 → Truncate
		Vol:        decimal.RequireFromString("1.5"),
		CreatedAt:  time.Date(2026, 4, 21, 10, 0, 0, 0, time.UTC),
		Extra:      tagExtra{Fee: decimal.NewFromInt(1), Tags: []string{"a", "b"}},
		Internal:   "ignored",
		untagged:   "also ignored",
	}

	row := def.Extract(in)

	// 校验 schema 只包含 7 个字段（Internal + untagged 应被跳过）。
	if len(row.Fields) != 7 {
		t.Fatalf("row fields = %d, want 7; schema = %+v", len(row.Fields), def.Fields())
	}

	// Price 应已按 prec=8 Truncate（按 Num 找字段，不依赖声明顺序）。
	priceFV, ok := row.GetByNum(4)
	if !ok {
		t.Fatal("price (num=4) missing from row")
	}
	wantPrice := decimal.RequireFromString("100.12345678")
	if d, _ := priceFV.Decimal(); !d.Equal(wantPrice) {
		t.Fatalf("Price truncated = %s, want %s", d, wantPrice)
	}

	out := &taggedOrder{}
	if err := def.Fill(row, out); err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if out.OrderID != "o-1" || out.Status != tagStatusFilled || out.ContractID != 777 {
		t.Fatalf("Fill scalars: %+v", out)
	}
	if !out.Price.Equal(wantPrice) {
		t.Fatalf("Fill Price = %s, want %s", out.Price, wantPrice)
	}
	if !out.CreatedAt.Equal(in.CreatedAt) {
		t.Fatalf("Fill CreatedAt = %v, want %v", out.CreatedAt, in.CreatedAt)
	}
	if len(out.Extra.Tags) != 2 || out.Extra.Tags[0] != "a" {
		t.Fatalf("Fill Extra: %+v", out.Extra)
	}
	// 跳过的字段 Fill 不会动它——Fill 的目标是新对象，应保持零值。
	if out.Internal != "" || out.untagged != "" {
		t.Fatalf("skipped fields should not be populated: %+v", out)
	}
}

// enum 字段（底层 int32）是 RegisterStruct 相比 Builder 的核心价值之一。
func TestRegisterStruct_EnumUnderlyingInt32(t *testing.T) {
	kind, _ := NewKind("tag", "enum")
	def, err := BuildFromStruct[taggedOrder](kind)
	if err != nil {
		t.Fatal(err)
	}
	in := &taggedOrder{Status: tagStatusOpen}
	row := def.Extract(in)

	statusFV, ok := row.GetByNum(2)
	if !ok {
		t.Fatal("status (num=2) missing from row")
	}
	got, err := statusFV.Int32()
	if err != nil {
		t.Fatalf("Int32: %v", err)
	}
	if tagOrderStatus(got) != tagStatusOpen {
		t.Fatalf("extracted status = %d, want %d", got, tagStatusOpen)
	}

	out := &taggedOrder{}
	if err := def.Fill(row, out); err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if out.Status != tagStatusOpen {
		t.Fatalf("filled status = %d, want %d", out.Status, tagStatusOpen)
	}
}

func TestRegisterStruct_IndexedFields(t *testing.T) {
	kind, _ := NewKind("tag", "idx")
	def, err := BuildFromStruct[taggedOrder](kind)
	if err != nil {
		t.Fatal(err)
	}
	// status / contract_id / price 标了 index，共 3 个。
	if len(def.IndexedFields()) != 3 {
		t.Fatalf("indexed fields = %d, want 3", len(def.IndexedFields()))
	}
	wantIdx := map[schema.FieldName]bool{"status": true, "contract_id": true, "price": true}
	for _, spec := range def.IndexedFields() {
		if !wantIdx[spec.Name] {
			t.Errorf("unexpected indexed field: %s", spec.Name)
		}
		delete(wantIdx, spec.Name)
	}
	if len(wantIdx) > 0 {
		t.Errorf("missing indexed fields: %v", wantIdx)
	}
}

func TestRegisterStruct_CustomViaValuerScanner(t *testing.T) {
	kind, _ := NewKind("tag", "custom")
	def, err := BuildFromStruct[taggedOrder](kind)
	if err != nil {
		t.Fatal(err)
	}
	extraSpec, ok := def.FieldByNum(7)
	if !ok {
		t.Fatal("extra (num=7) missing from def")
	}
	if extraSpec.Type != schema.FieldTypeCustom {
		t.Fatalf("Extra FieldType = %v, want Custom", extraSpec.Type)
	}

	in := &taggedOrder{Extra: tagExtra{Fee: decimal.NewFromInt(7), Tags: []string{"x"}}}
	row := def.Extract(in)
	extraFV, ok := row.GetByNum(7)
	if !ok {
		t.Fatal("extra (num=7) missing from row")
	}
	if extraFV.Type() != schema.FieldTypeCustom {
		t.Fatalf("Extra value Type = %v", extraFV.Type())
	}

	out := &taggedOrder{}
	if err := def.Fill(row, out); err != nil {
		t.Fatal(err)
	}
	if len(out.Extra.Tags) != 1 || out.Extra.Tags[0] != "x" {
		t.Fatalf("Extra Fill failed: %+v", out.Extra)
	}
}

// 演进场景：tag 上的 default 被 Fill 用于填补缺失 Row 字段。
type taggedEvolved struct {
	ID   int64  `store:"id,num=1"`
	Note string `store:"note,num=2,default=n/a"`
}

func TestRegisterStruct_Default(t *testing.T) {
	kind, _ := NewKind("tag", "default")
	def, err := BuildFromStruct[taggedEvolved](kind)
	if err != nil {
		t.Fatal(err)
	}
	oldRow := schema.Row{
		Version: 1,
		Fields: []schema.Field{
			{Num: 1, Value: schema.NewInt64(5)},
			// Note (num=2) 缺失 —— 应走 default "n/a"
		},
	}
	out := &taggedEvolved{}
	if err := def.Fill(oldRow, out); err != nil {
		t.Fatal(err)
	}
	if out.ID != 5 || out.Note != "n/a" {
		t.Fatalf("filled = %+v", out)
	}
}

func TestRegisterStruct_ErrorsOnBadField(t *testing.T) {
	// 不支持的类型 + 无 Valuer/Scanner → 明确报错。
	type bad struct {
		Nested struct{} `store:"nested,num=1"`
	}
	kind, _ := NewKind("tag", "bad")
	if _, err := BuildFromStruct[bad](kind); err == nil {
		t.Fatal("expected error for unsupported field type")
	}
}

func TestRegisterStruct_RejectsNonStruct(t *testing.T) {
	kind, _ := NewKind("tag", "int")
	if _, err := BuildFromStruct[int](kind); err == nil {
		t.Fatal("expected error for non-struct T")
	}
}

func TestRegisterStruct_ErrorsOnPrecMismatch(t *testing.T) {
	type bad struct {
		Score int32 `store:"score,num=1,prec=8"` // prec 不能用于 int32
	}
	kind, _ := NewKind("tag", "precmismatch")
	if _, err := BuildFromStruct[bad](kind); err == nil {
		t.Fatal("expected error: prec= on Int32")
	}
}

// num 必填：缺 num 的 tag 必须注册期失败。
func TestRegisterStruct_RejectsMissingNum(t *testing.T) {
	type bad struct {
		ID int64 `store:"id"` // 缺 num
	}
	kind, _ := NewKind("tag", "missingnum")
	_, err := BuildFromStruct[bad](kind)
	if err == nil {
		t.Fatal("expected error: missing num=<N>")
	}
	if !strings.Contains(err.Error(), "num") {
		t.Fatalf("error should mention num: %v", err)
	}
}

// num=0 非法——0 是"未分配"的哨兵。
func TestRegisterStruct_RejectsZeroNum(t *testing.T) {
	type bad struct {
		ID int64 `store:"id,num=0"`
	}
	kind, _ := NewKind("tag", "zeronum")
	if _, err := BuildFromStruct[bad](kind); err == nil {
		t.Fatal("expected error: num=0")
	}
}

// 重复 num：同一 struct 内两字段用同一个 num 必须报错。
func TestRegisterStruct_RejectsDuplicateNum(t *testing.T) {
	type bad struct {
		A int64 `store:"a,num=1"`
		B int64 `store:"b,num=1"`
	}
	kind, _ := NewKind("tag", "dupnum")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate num")
		}
	}()
	_, _ = BuildFromStruct[bad](kind)
}

// ~int 底层类型：iota-based 枚举常见场景，Go kind 为 reflect.Int。
// 之前用 (*int64)(ptr) 会在 32-bit 系统越界；现在按 kind 分派到 (*int)(ptr)。
type intBasedStatus int

const (
	intStatusOpen intBasedStatus = iota + 1
	intStatusClosed
)

type intBasedRec struct {
	ID     int64          `store:"id,num=1"`
	Status intBasedStatus `store:"status,num=2,index,type=int64"` // ~int 自定义类型 → 必须显式 type=int64
	Count  int            `store:"count,num=3"`                   // 预声明 int 无需 type=
}

func TestRegisterStruct_UnderlyingInt(t *testing.T) {
	kind, _ := NewKind("tag", "intbased")
	def, err := BuildFromStruct[intBasedRec](kind)
	if err != nil {
		t.Fatalf("BuildFromStruct: %v", err)
	}
	in := &intBasedRec{ID: 9, Status: intStatusClosed, Count: 123}
	row := def.Extract(in)

	out := &intBasedRec{}
	if err := def.Fill(row, out); err != nil {
		t.Fatal(err)
	}
	if out.ID != 9 || out.Status != intStatusClosed || out.Count != 123 {
		t.Fatalf("round-trip = %+v, want %+v", out, in)
	}

	// Status 在 row 里是 Int64 形态（~int 映射到 FieldTypeInt64）。
	statusFV, ok := row.GetByNum(2)
	if !ok {
		t.Fatal("status (num=2) missing")
	}
	if statusFV.Type() != schema.FieldTypeInt64 {
		t.Fatalf("status FieldType = %v, want Int64", statusFV.Type())
	}
}

// type= 显式覆盖：Go kind 已经足够推断时 tag 也允许重复声明以增强可读性。
func TestRegisterStruct_TypeOverride_ExplicitMatch(t *testing.T) {
	type rec struct {
		Seq int64 `store:"seq,num=1,type=int64"`
	}
	kind, _ := NewKind("tag", "typeok")
	def, err := BuildFromStruct[rec](kind)
	if err != nil {
		t.Fatal(err)
	}
	in := &rec{Seq: 42}
	row := def.Extract(in)
	out := &rec{}
	_ = def.Fill(row, out)
	if out.Seq != 42 {
		t.Fatalf("Seq round-trip failed: %+v", out)
	}
}

// type= 显式覆盖不匹配的 Go kind——必须在注册期明确报错，而不是运行时损坏数据。
func TestRegisterStruct_TypeOverride_Incompatible(t *testing.T) {
	type rec struct {
		Name string `store:"name,num=1,type=int32"` // kind String 和 type=int32 不匹配
	}
	kind, _ := NewKind("tag", "typebad")
	if _, err := BuildFromStruct[rec](kind); err == nil {
		t.Fatal("expected error: type=int32 on string field")
	}
}

// int32 FieldType + ~int 字段也必须拒绝——内存宽度不同，写回会越界。
func TestRegisterStruct_Int32OnIntField_Rejected(t *testing.T) {
	type rec struct {
		Count int `store:"count,num=1,type=int32"`
	}
	kind, _ := NewKind("tag", "int32onint")
	if _, err := BuildFromStruct[rec](kind); err == nil {
		t.Fatal("expected error: type=int32 on ~int field (width mismatch)")
	}
}

// 自定义命名 scalar 类型必须强制显式 type= —— 防止业务悄悄改了 underlying
// 类型（如 `type Status int32` → `int64`）后 schema 跟着静默变化。
func TestRegisterStruct_DefinedScalarRequiresExplicitType(t *testing.T) {
	type Status int32
	type rec struct {
		S Status `store:"s,num=1,index"` // 无 type= → 注册期报错
	}
	kind, _ := NewKind("tag", "enforce-type")
	_, err := BuildFromStruct[rec](kind)
	if err == nil {
		t.Fatal("expected error: defined scalar type without explicit type= tag")
	}
	// 错误信息要够有帮助——明确说明应加 `type=int32`
	if !containsAll(err.Error(), "defined type", "type=int32") {
		t.Fatalf("error message lacks guidance: %v", err)
	}
}

// 同一个字段加上 type=int32 后注册成功。
func TestRegisterStruct_DefinedScalarWithExplicitType(t *testing.T) {
	type Status int32
	type rec struct {
		S Status `store:"s,num=1,index,type=int32"`
	}
	kind, _ := NewKind("tag", "enforce-type-ok")
	def, err := BuildFromStruct[rec](kind)
	if err != nil {
		t.Fatalf("expected ok: %v", err)
	}
	in := &rec{S: 7}
	row := def.Extract(in)
	out := &rec{}
	_ = def.Fill(row, out)
	if out.S != 7 {
		t.Fatalf("round-trip failed: %+v", out)
	}
}

// 预声明基本类型（int32/int64/int/string/decimal.Decimal/time.Time）不需要 type=，
// 自动推断仍然工作。
func TestRegisterStruct_PredeclaredBasicsNoTypeNeeded(t *testing.T) {
	type rec struct {
		A int32           `store:"a,num=1"`
		B int64           `store:"b,num=2"`
		C int             `store:"c,num=3"`
		D string          `store:"d,num=4"`
		E decimal.Decimal `store:"e,num=5"`
		F time.Time       `store:"f,num=6"`
	}
	kind, _ := NewKind("tag", "basics")
	if _, err := BuildFromStruct[rec](kind); err != nil {
		t.Fatalf("predeclared basics should infer without type=: %v", err)
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}
