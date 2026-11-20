package wal

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/amoylan/flash-store/store/registry"
	_ "github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/schema"
)

var testKind registry.Kind

func init() {
	var err error
	testKind, err = registry.NewKind("test", "order")
	if err != nil {
		panic(err)
	}
}

func makeTestRow(orderID int64, status string) schema.Row {
	return schema.Row{
		Version: 1,
		Fields: []schema.Field{
			{Num: 1, Value: schema.NewFieldValue[int64](orderID)},
			{Num: 2, Value: schema.NewFieldValue(status)},
		},
	}
}

// TestJsonCodec_SchemaConfig 验证 WalTypeSchemaConfig 的 Schemas 字段 round-trip。
func TestJsonCodec_SchemaConfig(t *testing.T) {
	codec := jsonWalCodec{}
	original := &Wal{
		Schemas: []schema.KindDescriptor{
			{
				Kind:    "future.order",
				Version: 3,
				Fields: []schema.FieldDescriptor{
					{Num: 1, Name: "id", Type: schema.FieldTypeInt64},
					{Num: 2, Name: "status", Type: schema.FieldTypeInt32, Indexed: true},
				},
			},
			{
				Kind:    "future.account",
				Version: 1,
				Fields: []schema.FieldDescriptor{
					{Num: 1, Name: "balance", Type: schema.FieldTypeDecimal, DecimalPrecision: 8},
				},
			},
		},
	}
	data, err := codec.Encode(original)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Schemas) != 2 {
		t.Fatalf("len(Schemas) = %d, want 2", len(decoded.Schemas))
	}
	if decoded.Schemas[0].Kind != "future.order" || decoded.Schemas[0].Version != 3 {
		t.Errorf("schema[0] = %+v", decoded.Schemas[0])
	}
	if !decoded.Schemas[0].Fields[1].Indexed {
		t.Errorf("indexed flag lost")
	}
	if decoded.Schemas[1].Fields[0].DecimalPrecision != 8 {
		t.Errorf("precision lost")
	}
}

// TestJsonCodec_Set_RoundTrip 验证 OpSet 的 Row 往返。
func TestJsonCodec_Set_RoundTrip(t *testing.T) {
	codec := jsonWalCodec{}

	row := makeTestRow(42, "open")
	original := &Wal{
		Events: []Event{
			{
				EventType: "CreateOrder",
				Identity:  100,
				Cmds: []Cmd{
					{Op: OpSet, Key: "order-42", Kind: testKind, Row: row, Ttl: 5 * time.Second},
				},
			},
		},
	}

	data, err := codec.Encode(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// 验证 wire 中 kind 是字符串 "test.order"，row 是 base64 字节。
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal wire: %v", err)
	}
	cmd := raw["events"].([]any)[0].(map[string]any)["cmds"].([]any)[0].(map[string]any)
	if cmd["kind"].(string) != "test.order" {
		t.Fatalf("kind=%v want test.order", cmd["kind"])
	}
	if _, ok := cmd["row"].(string); !ok {
		t.Fatalf("row should be base64 string, got %T", cmd["row"])
	}

	// round-trip
	decoded, err := codec.Decode(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.Events) != 1 || len(decoded.Events[0].Cmds) != 1 {
		t.Fatalf("unexpected structure: %+v", decoded)
	}
	got := decoded.Events[0].Cmds[0]
	if got.Kind != testKind {
		t.Fatalf("kind=%v want %v", got.Kind, testKind)
	}
	if got.Ttl != 5*time.Second {
		t.Fatalf("ttl=%v want 5s", got.Ttl)
	}
	if len(got.Row.Fields) != 2 {
		t.Fatalf("row fields = %d, want 2", len(got.Row.Fields))
	}
	idFV, _ := got.Row.GetByNum(1)
	if v, _ := idFV.Int64(); v != 42 {
		t.Fatalf("row num=1 value = %d, want 42", v)
	}
	statusFV, _ := got.Row.GetByNum(2)
	if s, _ := statusFV.String(); s != "open" {
		t.Fatalf("row num=2 value = %q, want open", s)
	}
}

// TestJsonCodec_Delete_RoundTrip 验证 OpDelete 的 round-trip（无 Row）。
func TestJsonCodec_Delete_RoundTrip(t *testing.T) {
	codec := jsonWalCodec{}

	original := &Wal{
		Events: []Event{
			{
				EventType: "DeleteOrder",
				Identity:  100,
				Cmds: []Cmd{
					{Op: OpDelete, Key: "order-42", Kind: testKind},
				},
			},
		},
	}

	data, err := codec.Encode(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := codec.Decode(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	cmd := decoded.Events[0].Cmds[0]
	if cmd.Op != OpDelete || cmd.Key != "order-42" || cmd.Kind != testKind {
		t.Fatalf("cmd = %+v", cmd)
	}
}

// TestJsonCodec_MixedCmds_RoundTrip 验证同一 event 中多种 cmd 混合。
func TestJsonCodec_MixedCmds_RoundTrip(t *testing.T) {
	codec := jsonWalCodec{}

	row1 := makeTestRow(1, "open")
	row2 := makeTestRow(2, "closed")

	original := &Wal{
		Events: []Event{
			{
				EventType: "Mixed",
				Identity:  100,
				Cmds: []Cmd{
					{Op: OpSet, Key: "a", Kind: testKind, Row: row1},
					{Op: OpSet, Key: "b", Kind: testKind, Row: row2},
					{Op: OpDelete, Key: "c", Kind: testKind},
				},
				KvCmds: []KvCmd{
					{Op: OpSet, Key: "kv1", Value: []byte("hello")},
				},
				BizData: RawBizData([]byte{1, 0x41, 0x42}),
			},
		},
	}

	data, err := codec.Encode(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := codec.Decode(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	ev := decoded.Events[0]
	if len(ev.Cmds) != 3 {
		t.Fatalf("cmds=%d want 3", len(ev.Cmds))
	}
	idFV0, _ := ev.Cmds[0].Row.GetByNum(1)
	if v, _ := idFV0.Int64(); v != 1 {
		t.Fatalf("cmd[0] num=1 = %d", v)
	}
	statusFV1, _ := ev.Cmds[1].Row.GetByNum(2)
	if s, _ := statusFV1.String(); s != "closed" {
		t.Fatalf("cmd[1] num=2 = %q", s)
	}
	if ev.Cmds[2].Op != OpDelete {
		t.Fatalf("cmd[2].Op = %d", ev.Cmds[2].Op)
	}
	if len(ev.KvCmds) != 1 || string(ev.KvCmds[0].Value) != "hello" {
		t.Fatalf("kv_cmds: %+v", ev.KvCmds)
	}
	if ev.BizData.Version() != 1 {
		t.Fatalf("biz_data version=%d", ev.BizData.Version())
	}
}
