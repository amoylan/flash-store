package schema

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

// FuzzDecodeRow 喂随机 bytes 给 DecodeRow，断言永远不 panic。
// 同时把若干合法编码作为种子，让 fuzzer 从已知有效的 wire 出发去做最小变异。
//
// 跑法：go test -run=^$ -fuzz=FuzzDecodeRow -fuzztime=30s ./store/schema
func FuzzDecodeRow(f *testing.F) {
	seeds := []Row{
		{Version: 0, Fields: nil},
		{Version: 1, Fields: []Field{{Num: 1, Value: NewInt32(0)}}},
		{Version: 2, Fields: []Field{
			{Num: 1, Value: NewInt32(-7)},
			{Num: 2, Value: NewInt64(1 << 40)},
			{Num: 3, Value: NewString("hello")},
			{Num: 4, Value: NewDecimal(decimal.RequireFromString("1.5"))},
			{Num: 5, Value: NewTimestamp(time.Date(2026, 4, 28, 0, 0, 0, 0, time.UTC))},
			{Num: 6, Value: NewCustomFieldValue([]byte{1, 2, 3})},
		}},
		{Version: 3, Fields: []Field{
			{Num: 1, Value: FieldValue{}}, // Undefined
			{Num: 2, Value: NewTimestamp(time.Time{})},
		}},
	}
	for _, s := range seeds {
		buf, err := EncodeRow(s)
		if err != nil {
			f.Fatalf("seed encode: %v", err)
		}
		f.Add(buf)
	}
	// 极端短输入也作为种子。
	f.Add([]byte{})
	f.Add([]byte{0x02})
	f.Add([]byte{0x02, 0x00})
	f.Add([]byte{0x02, 0x00, 0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		// 只断言：DecodeRow 永不 panic；任何错误都 OK。
		row, err := DecodeRow(data)
		if err != nil {
			return
		}
		// 若 decode 成功，再做一次 encode → decode round-trip，结果应稳定。
		buf, err := EncodeRow(row)
		if err != nil {
			// 已 decode 成功的 Row 重新 encode 失败是可能的（例如 num 取了 int32 边界等
			// 由 wire 直接还原但被 encoder 校验拒绝的情形）；不算 panic 类 bug。
			return
		}
		row2, err := DecodeRow(buf)
		if err != nil {
			t.Fatalf("re-decode after re-encode failed: %v\n  wire1=%x\n  wire2=%x", err, data, buf)
		}
		if row.Version != row2.Version || len(row.Fields) != len(row2.Fields) {
			t.Fatalf("round-trip mismatch:\n  v1=%d fields1=%d\n  v2=%d fields2=%d",
				row.Version, len(row.Fields), row2.Version, len(row2.Fields))
		}
		for i := range row.Fields {
			if row.Fields[i].Num != row2.Fields[i].Num {
				t.Fatalf("field[%d] num mismatch: %d vs %d", i, row.Fields[i].Num, row2.Fields[i].Num)
			}
			if !row.Fields[i].Value.Equal(row2.Fields[i].Value) {
				t.Fatalf("field[%d] value mismatch", i)
			}
		}
	})
}
