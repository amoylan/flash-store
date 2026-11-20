package registry

import (
	"encoding/json"
	"testing"

	"github.com/amoylan/flash-store/store/schema"
	"github.com/shopspring/decimal"
)

// extra 是一个自定义类型，通过 JSON 实现 Valuer / Scanner。
type extra struct {
	Fee  decimal.Decimal `json:"fee"`
	Tags []string        `json:"tags"`
}

func (e *extra) Value() ([]byte, error) { return json.Marshal(e) }
func (e *extra) Scan(data []byte) error {
	if len(data) == 0 {
		*e = extra{}
		return nil
	}
	return json.Unmarshal(data, e)
}

type customRec struct {
	ID    int64
	Extra extra
}

func buildCustomDef(t *testing.T, kind Kind) *Def[customRec] {
	t.Helper()
	b := For[customRec](kind).
		Int64(1, "id", func(r *customRec) *int64 { return &r.ID })
	b = CustomField[customRec, extra](b, 2, "extra", func(r *customRec) *extra { return &r.Extra })
	return b.Build()
}

func TestDef_CustomRoundTrip(t *testing.T) {
	kind := mustKind(t, "deftest", "custom-rt")
	def := buildCustomDef(t, kind)

	in := &customRec{
		ID: 42,
		Extra: extra{
			Fee:  decimal.RequireFromString("0.001"),
			Tags: []string{"vip", "mm"},
		},
	}

	row := def.Extract(in)
	extraFV, ok := row.GetByNum(2)
	if !ok {
		t.Fatal("extra (num=2) missing from row")
	}
	if extraFV.Type() != schema.FieldTypeCustom {
		t.Fatalf("extra field Type = %v", extraFV.Type())
	}
	// 验证 Row 里存的是 bytes，不是 *extra（不可能泄漏共享引用）。
	raw, err := extraFV.CustomValue()
	if err != nil {
		t.Fatalf("CustomValue: %v", err)
	}
	b, ok := raw.([]byte)
	if !ok {
		t.Fatalf("Custom payload = %T, want []byte", raw)
	}
	if len(b) == 0 || b[0] != '{' {
		t.Fatalf("Custom bytes not JSON: %q", b)
	}

	// Row 字节流往返。
	buf, err := schema.EncodeRow(row)
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	decoded, err := schema.DecodeRow(buf)
	if err != nil {
		t.Fatalf("DecodeRow: %v", err)
	}

	// Fill 回到新对象，Scan 复原 extra。
	out := &customRec{}
	if err := def.Fill(decoded, out); err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if out.ID != 42 {
		t.Fatalf("ID = %d", out.ID)
	}
	if !out.Extra.Fee.Equal(in.Extra.Fee) {
		t.Fatalf("Fee = %s, want %s", out.Extra.Fee, in.Extra.Fee)
	}
	if len(out.Extra.Tags) != 2 || out.Extra.Tags[0] != "vip" || out.Extra.Tags[1] != "mm" {
		t.Fatalf("Tags = %v", out.Extra.Tags)
	}
}

// 验证 Row 里的字节和业务原对象**完全解耦**：业务修改原对象不影响存储。
// 这正是"不需要 Clone"的核心保证。
func TestDef_CustomNoAliasing(t *testing.T) {
	kind := mustKind(t, "deftest", "custom-alias")
	def := buildCustomDef(t, kind)

	in := &customRec{ID: 1, Extra: extra{Tags: []string{"a", "b"}}}
	row := def.Extract(in)

	// 业务事后改自己的对象——存储的 Row bytes 不受影响。
	in.Extra.Tags[0] = "mutated"
	in.Extra.Tags = append(in.Extra.Tags, "c")

	out := &customRec{}
	if err := def.Fill(row, out); err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if len(out.Extra.Tags) != 2 || out.Extra.Tags[0] != "a" || out.Extra.Tags[1] != "b" {
		t.Fatalf("Tags leaked mutation: %v", out.Extra.Tags)
	}
}

// 验证 Builder 拒绝 Custom 字段 Indexed。
func TestDef_CustomCannotBeIndexed(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for indexed Custom field")
		}
	}()
	kind := mustKind(t, "deftest", "custom-idx")
	b := For[customRec](kind)
	b = CustomField[customRec, extra](b, 1, "extra", func(r *customRec) *extra { return &r.Extra }, schema.Indexed())
	b.Build()
}

// 验证 Custom 字段的 Default（[]byte）在 source 对象 nil / getter nil 时生效。
func TestDef_CustomDefaultOnNilExtract(t *testing.T) {
	kind := mustKind(t, "deftest", "custom-default")
	defaultBytes, _ := json.Marshal(&extra{Tags: []string{"default"}})
	b := For[customRec](kind).
		Int64(1, "id", func(r *customRec) *int64 { return &r.ID })
	b = CustomField[customRec, extra](
		b, 2, "extra",
		func(r *customRec) *extra { return &r.Extra },
		schema.Default(defaultBytes),
	)
	def := b.Build()

	// nil 对象 → Extract 走 Default。
	row := def.Extract(nil)
	extraFV, ok := row.GetByNum(2)
	if !ok {
		t.Fatal("extra (num=2) missing from row")
	}
	got, _ := extraFV.CustomValue()
	if string(got.([]byte)) != string(defaultBytes) {
		t.Fatalf("Default not applied on nil Extract: got %s", got)
	}

	// Fill 回来时 Scan 应解析 Default JSON 成 extra.Tags=["default"]。
	out := &customRec{}
	if err := def.Fill(row, out); err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if len(out.Extra.Tags) != 1 || out.Extra.Tags[0] != "default" {
		t.Fatalf("Default roundtrip failed: %v", out.Extra.Tags)
	}
}
