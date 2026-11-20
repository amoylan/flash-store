package wal

import (
	"testing"
	"time"

	"github.com/amoylan/flash-store/store/schema"
)

// TestProtoCodec_SchemaConfig 验证 Proto 路径的 Schemas 往返（走内嵌 JSON）。
func TestProtoCodec_SchemaConfig(t *testing.T) {
	codec := protoWalCodec{}
	original := &Wal{
		Schemas: []schema.KindDescriptor{
			{
				Kind:    "future.order",
				Version: 7,
				Fields: []schema.FieldDescriptor{
					{Num: 1, Name: "id", Type: schema.FieldTypeInt64},
					{Num: 2, Name: "price", Type: schema.FieldTypeDecimal, Indexed: true, DecimalPrecision: 8},
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
	if len(decoded.Schemas) != 1 {
		t.Fatalf("len(Schemas) = %d, want 1", len(decoded.Schemas))
	}
	if decoded.Schemas[0].Version != 7 || len(decoded.Schemas[0].Fields) != 2 {
		t.Fatalf("schema round-trip failed: %+v", decoded.Schemas[0])
	}
	if decoded.Schemas[0].Fields[1].DecimalPrecision != 8 {
		t.Errorf("precision lost")
	}
}

// TestProtoCodec_CmdWithRow_RoundTrip 验证 Cmd 的 Row 往返。
func TestProtoCodec_CmdWithRow_RoundTrip(t *testing.T) {
	codec := protoWalCodec{}

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
	idFV, _ := got.Row.GetByNum(1)
	if v, _ := idFV.Int64(); v != 42 {
		t.Fatalf("row num=1 = %d", v)
	}
	if got.Ttl != 5*time.Second {
		t.Fatalf("ttl=%v want 5s", got.Ttl)
	}
}

// TestProtoCodec_Delete_RoundTrip 验证 OpDelete 的 round-trip。
func TestProtoCodec_Delete_RoundTrip(t *testing.T) {
	codec := protoWalCodec{}

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
	if cmd.Op != OpDelete {
		t.Fatalf("op=%d want OpDelete", cmd.Op)
	}
	if cmd.Key != "order-42" {
		t.Fatalf("key=%s want order-42", cmd.Key)
	}
}

// TestProtoCodec_FullEvent_RoundTrip 验证包含 Cmds、KvCmds、BizData 的完整事件。
func TestProtoCodec_FullEvent_RoundTrip(t *testing.T) {
	codec := protoWalCodec{}

	row1 := makeTestRow(1, "open")
	row2 := makeTestRow(2, "closed")

	original := &Wal{
		Events: []Event{
			{
				EventType: "Complex",
				Identity:  999,
				Cmds: []Cmd{
					{Op: OpSet, Key: "a", Kind: testKind, Row: row1, Ttl: time.Minute},
					{Op: OpSet, Key: "b", Kind: testKind, Row: row2},
					{Op: OpDelete, Key: "c", Kind: testKind},
				},
				KvCmds: []KvCmd{
					{Op: OpSet, Key: "kv1", Value: []byte("hello"), Ttl: 30 * time.Second},
					{Op: OpDelete, Key: "kv2"},
				},
				BizData: RawBizData([]byte{2, 0xDE, 0xAD}),
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
	if ev.Cmds[0].Ttl != time.Minute {
		t.Fatalf("cmd[0].Ttl=%v want 1m", ev.Cmds[0].Ttl)
	}
	statusFV1, _ := ev.Cmds[1].Row.GetByNum(2)
	if s, _ := statusFV1.String(); s != "closed" {
		t.Fatalf("cmd[1] num=2 = %q", s)
	}
	if ev.Cmds[2].Op != OpDelete {
		t.Fatalf("cmd[2].Op=%d want OpDelete", ev.Cmds[2].Op)
	}

	if len(ev.KvCmds) != 2 {
		t.Fatalf("kv_cmds=%d want 2", len(ev.KvCmds))
	}
	if string(ev.KvCmds[0].Value) != "hello" || ev.KvCmds[0].Ttl != 30*time.Second {
		t.Fatalf("kv[0]: %+v", ev.KvCmds[0])
	}
	if ev.KvCmds[1].Op != OpDelete {
		t.Fatalf("kv[1].Op=%d want OpDelete", ev.KvCmds[1].Op)
	}

	if ev.BizData == nil {
		t.Fatal("biz_data is nil")
	}
	if ev.BizData.Version() != 2 {
		t.Fatalf("biz_data version=%d want 2", ev.BizData.Version())
	}
	if len(ev.BizData.Bytes()) != 2 || ev.BizData.Bytes()[0] != 0xDE || ev.BizData.Bytes()[1] != 0xAD {
		t.Fatalf("biz_data bytes=%x want dead", ev.BizData.Bytes())
	}

}

// TestProtoCodec_CompactSize 验证 proto 编码比 JSON 更紧凑。
func TestProtoCodec_CompactSize(t *testing.T) {
	jsonCodec := jsonWalCodec{}
	protoCodec := protoWalCodec{}

	row := makeTestRow(12345, "partially_filled")
	w := &Wal{
		Events: []Event{
			{
				EventType: "UpdateOrder",
				Identity:  88888,
				Cmds: []Cmd{
					{Op: OpSet, Key: "order-12345", Kind: testKind, Row: row},
				},
				KvCmds: []KvCmd{
					{Op: OpSet, Key: "balance", Value: []byte("1000000.50")},
				},
			},
		},
	}

	jsonData, err := jsonCodec.Encode(w)
	if err != nil {
		t.Fatalf("json encode: %v", err)
	}

	protoData, err := protoCodec.Encode(w)
	if err != nil {
		t.Fatalf("proto encode: %v", err)
	}

	t.Logf("json size: %d bytes, proto size: %d bytes, ratio: %.1f%%",
		len(jsonData), len(protoData), float64(len(protoData))/float64(len(jsonData))*100)

	if len(protoData) >= len(jsonData) {
		t.Fatalf("proto should be more compact: proto=%d json=%d", len(protoData), len(jsonData))
	}
}
