package schema

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

// FieldValue JSON 往返——FieldDescriptor.Default 持久化后要能原样读回来，
// compat 判定才稳（值相等但序列化/反序列化损失就会 false-negative）。
func TestFieldValue_JSONRoundTrip(t *testing.T) {
	now := time.Date(2026, 4, 22, 10, 0, 0, 123456789, time.UTC)
	cases := []struct {
		name string
		in   FieldValue
	}{
		{"undefined", FieldValue{}},
		{"int32 positive", NewInt32(42)},
		{"int32 negative", NewInt32(-7)},
		{"int64 max", NewInt64(1 << 50)},
		{"string", NewString("hello world")},
		{"string empty", NewString("")},
		{"decimal", NewDecimal(decimal.RequireFromString("12.3456789"))},
		{"timestamp", NewTimestamp(now)},
		{"timestamp zero", NewTimestamp(time.Time{})},
		{"custom bytes", NewCustomFieldValue([]byte{1, 2, 3, 0, 255})},
		{"custom nil", NewCustomFieldValue(nil)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			data, err := json.Marshal(&c.in)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var got FieldValue
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if !got.Equal(c.in) {
				t.Fatalf("round-trip mismatch:\n  in  = %+v\n  got = %+v\n  raw = %s", c.in, got, data)
			}
		})
	}
}

func TestFieldValue_JSON_ReadableFormat(t *testing.T) {
	// 验证 JSON 形态对人类可读（而不是裸数字 / base64）。
	data, _ := json.Marshal(NewInt32(42))
	if !strings.Contains(string(data), `"int32"`) {
		t.Errorf("int32 JSON not readable: %s", data)
	}
	data, _ = json.Marshal(NewDecimal(decimal.RequireFromString("1.5")))
	if !strings.Contains(string(data), `"1.5"`) {
		t.Errorf("decimal JSON not readable: %s", data)
	}
	data, _ = json.Marshal(NewString("hello"))
	if !strings.Contains(string(data), `"hello"`) {
		t.Errorf("string JSON not readable: %s", data)
	}
}

// TestFieldValue_DecimalJSON_RejectsOOMVectors 防御 shopspring/decimal 接受形如
// "0E400000001" 的输入解析为巨大 exp，后续 String/Cmp 等会触发 big.Int.Exp 卡顿/OOM。
// row codec 路径有同款防御（见 row_codec.go:228-241），JSON 路径以前漏防——这里覆盖之。
func TestFieldValue_DecimalJSON_RejectsOOMVectors(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"oversize length", `{"t":4,"v":"` + strings.Repeat("1", maxDecimalStringLen+1) + `"}`},
		{"giant positive exp", `{"t":4,"v":"0E400000001"}`},
		{"giant negative exp", `{"t":4,"v":"0E-400000001"}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var v FieldValue
			err := json.Unmarshal([]byte(c.raw), &v)
			if err == nil {
				t.Fatalf("expected error for %q, got nil", c.raw)
			}
		})
	}
}
