package schema

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestRow_BinaryRoundTrip(t *testing.T) {
	want := Row{
		Version: 42,
		Fields: []Field{
			{Num: 1, Value: NewFieldValue[int32](-7)},
			{Num: 2, Value: NewFieldValue[int64](1 << 40)},
			{Num: 3, Value: NewFieldValue("hello world")},
			{Num: 4, Value: NewFieldValue(decimal.RequireFromString("12.3456789012"))},
			{Num: 5, Value: NewFieldValue(time.Date(2026, 4, 20, 12, 34, 56, 789, time.UTC))},
		},
	}
	buf, err := EncodeRow(want)
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	if buf[0] != RowFormatV2 {
		t.Fatalf("format byte = 0x%02x, want 0x%02x", buf[0], RowFormatV2)
	}
	got, err := DecodeRow(buf)
	if err != nil {
		t.Fatalf("DecodeRow: %v", err)
	}
	if got.Version != want.Version {
		t.Fatalf("Version = %d, want %d", got.Version, want.Version)
	}
	if len(got.Fields) != len(want.Fields) {
		t.Fatalf("len Fields = %d, want %d", len(got.Fields), len(want.Fields))
	}
	for i := range want.Fields {
		if got.Fields[i].Num != want.Fields[i].Num {
			t.Fatalf("field[%d].Num = %d, want %d", i, got.Fields[i].Num, want.Fields[i].Num)
		}
		if !got.Fields[i].Value.Equal(want.Fields[i].Value) {
			t.Fatalf("field[%d].Value: got %+v, want %+v", i, got.Fields[i].Value, want.Fields[i].Value)
		}
	}
}

func TestRow_EmptyRoundTrip(t *testing.T) {
	want := Row{Version: 1, Fields: nil}
	buf, err := EncodeRow(want)
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	got, err := DecodeRow(buf)
	if err != nil {
		t.Fatalf("DecodeRow: %v", err)
	}
	if got.Version != 1 || len(got.Fields) != 0 {
		t.Fatalf("got %+v", got)
	}
}

func TestRow_UndefinedField(t *testing.T) {
	want := Row{
		Version: 1,
		Fields: []Field{
			{Num: 1, Value: FieldValue{}}, // FieldTypeUndefined
			{Num: 2, Value: NewFieldValue[int32](5)},
		},
	}
	buf, err := EncodeRow(want)
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	got, err := DecodeRow(buf)
	if err != nil {
		t.Fatalf("DecodeRow: %v", err)
	}
	if got.Fields[0].Value.Type() != FieldTypeUndefined {
		t.Fatalf("field[0].Type = %v, want Undefined", got.Fields[0].Value.Type())
	}
	if v, _ := got.Fields[1].Value.Int32(); v != 5 {
		t.Fatalf("field[1] = %d, want 5", v)
	}
}

// Num 身份：Fields 顺序与语义无关——不同编码顺序应解出同一份数据。
func TestRow_NumDrivenDecoupledFromFieldOrder(t *testing.T) {
	a := Row{Version: 1, Fields: []Field{
		{Num: 1, Value: NewInt64(10)},
		{Num: 2, Value: NewString("foo")},
	}}
	b := Row{Version: 1, Fields: []Field{
		{Num: 2, Value: NewString("foo")},
		{Num: 1, Value: NewInt64(10)},
	}}

	aBuf, err := EncodeRow(a)
	if err != nil {
		t.Fatalf("EncodeRow a: %v", err)
	}
	bBuf, err := EncodeRow(b)
	if err != nil {
		t.Fatalf("EncodeRow b: %v", err)
	}

	aDec, _ := DecodeRow(aBuf)
	bDec, _ := DecodeRow(bBuf)

	// 按 Num 读取应得到相同结果。
	for _, num := range []int32{1, 2} {
		av, ok := aDec.GetByNum(num)
		if !ok {
			t.Fatalf("a missing num=%d", num)
		}
		bv, ok := bDec.GetByNum(num)
		if !ok {
			t.Fatalf("b missing num=%d", num)
		}
		if !av.Equal(bv) {
			t.Fatalf("num=%d: a=%+v b=%+v", num, av, bv)
		}
	}
}

// 老格式（v1，无 format byte）应在新 Decode 下明确报错——防止 wire 格式静默混淆。
func TestRow_RejectsOldFormat(t *testing.T) {
	// 构造一个不是以 0x02 开头的 buffer：直接以 version uvarint 起手（v1 老布局）。
	old := []byte{0x01, 0x00} // version=1, nFields=0
	_, err := DecodeRow(old)
	if err == nil {
		t.Fatal("expected error for legacy v1 wire format (no format byte)")
	}
}

// num=0 在 Encode 应被拒绝——0 表示"未分配"，禁止入 wire。
func TestRow_RejectsZeroNumOnEncode(t *testing.T) {
	bad := Row{Version: 1, Fields: []Field{{Num: 0, Value: NewInt32(1)}}}
	if _, err := EncodeRow(bad); err == nil {
		t.Fatal("expected error encoding num=0 field")
	}
}

// time.Time{} 必须在 binary round-trip 后仍然 IsZero——绕过 Time() 物化的关键不变量。
func TestRow_TimestampZero_BinaryRoundTrip(t *testing.T) {
	want := Row{
		Version: 1,
		Fields: []Field{
			{Num: 1, Value: NewTimestamp(time.Time{})},
			// 同时验证 explicit time.Unix(0, 0) 也会被折叠到 zero（系统语义）。
			{Num: 2, Value: NewTimestamp(time.Unix(0, 0))},
			// 真实业务时间不应被影响。
			{Num: 3, Value: NewTimestamp(time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC))},
		},
	}
	buf, err := EncodeRow(want)
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	got, err := DecodeRow(buf)
	if err != nil {
		t.Fatalf("DecodeRow: %v", err)
	}
	t1, _ := got.Fields[0].Value.Time()
	if !t1.IsZero() {
		t.Fatalf("field[0]: expected IsZero, got %v", t1)
	}
	t2, _ := got.Fields[1].Value.Time()
	if !t2.IsZero() {
		t.Fatalf("field[1] (epoch coerced to zero): expected IsZero, got %v", t2)
	}
	t3, _ := got.Fields[2].Value.Time()
	if t3.IsZero() {
		t.Fatalf("field[2] (real time): unexpectedly IsZero")
	}
	if !t3.Equal(time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("field[2] = %v, want 2026-04-28 12:00 UTC", t3)
	}
}

// 重复 Num 必须在 encode 阶段被拒——避免读端 GetByNum 语义模糊。
func TestRow_RejectsDuplicateNumOnEncode(t *testing.T) {
	bad := Row{Version: 1, Fields: []Field{
		{Num: 7, Value: NewInt32(1)},
		{Num: 7, Value: NewInt32(2)},
	}}
	_, err := EncodeRow(bad)
	if err == nil {
		t.Fatal("expected error for duplicate num")
	}
	if !strings.Contains(err.Error(), "duplicate num") {
		t.Fatalf("error %q should mention duplicate num", err)
	}
}

// 损坏的 wire 在 Int32 槽里塞超 int32 范围的值——必须报错而不是静默截断。
func TestRow_DecodeInt32_OutOfRange(t *testing.T) {
	// 手工构造一条只含一个 Int32 字段、payload 为 int64 大值的 wire。
	var buf []byte
	buf = append(buf, RowFormatV2)
	buf = binary.AppendUvarint(buf, 1)              // version
	buf = binary.AppendUvarint(buf, 1)              // nFields
	buf = binary.AppendUvarint(buf, 1)              // num=1
	buf = append(buf, byte(FieldTypeInt32))         // tag
	buf = binary.AppendVarint(buf, math.MaxInt32+1) // out-of-range payload
	if _, err := DecodeRow(buf); err == nil {
		t.Fatal("expected error for int32 out-of-range payload")
	}
}

// 损坏的 nFields header（远超 buffer 实际容量）必须立即被拒绝，不能触发巨量分配。
func TestRow_DecodeNFields_BoundCheck(t *testing.T) {
	var buf []byte
	buf = append(buf, RowFormatV2)
	buf = binary.AppendUvarint(buf, 1) // version
	// 谎称 1<<30 个字段，但后面没有任何 payload。
	buf = binary.AppendUvarint(buf, 1<<30)
	if _, err := DecodeRow(buf); err == nil {
		t.Fatal("expected error for impossibly large nFields")
	}
}

// Decimal payload 的科学计数法 + 巨大 exponent 必须被拒绝——否则 NewFromString 解析便宜，
// 但后续 d.String() 会展开成数亿位的零字符串，触发 OOM/hang。
func TestRow_DecodeDecimal_RejectsHugeExponent(t *testing.T) {
	// 构造一个 Decimal 字段，payload = "0E1000000000"。
	payload := "0E1000000000"
	var buf []byte
	buf = append(buf, RowFormatV2)
	buf = binary.AppendUvarint(buf, 1) // version
	buf = binary.AppendUvarint(buf, 1) // nFields
	buf = binary.AppendUvarint(buf, 1) // num=1
	buf = append(buf, byte(FieldTypeDecimal))
	buf = binary.AppendUvarint(buf, uint64(len(payload)))
	buf = append(buf, payload...)
	// 这个 payload 长度 (12 字节) 在上限内，所以不会被长度门拦——
	// 不过我们其实想 cover 的是：超长 payload 会被门挡。本例展示的是长度内的真实 hang 输入，
	// 验证当前 cap=128 仍能拒（"0E" + 126 位指数串足以触发数百亿位展开）。
	// 这里改用确实超过 cap 的长度。
	hugePayload := "0E" + strings.Repeat("1", maxDecimalStringLen)
	buf = buf[:0]
	buf = append(buf, RowFormatV2)
	buf = binary.AppendUvarint(buf, 1)
	buf = binary.AppendUvarint(buf, 1)
	buf = binary.AppendUvarint(buf, 1)
	buf = append(buf, byte(FieldTypeDecimal))
	buf = binary.AppendUvarint(buf, uint64(len(hugePayload)))
	buf = append(buf, hugePayload...)
	_, err := DecodeRow(buf)
	if err == nil {
		t.Fatal("expected error for over-long decimal payload")
	}
	if !strings.Contains(err.Error(), "decimal payload length") {
		t.Fatalf("error %q should mention decimal payload length", err)
	}
}

// 短 payload 但科学计数法塞了巨大 exponent——长度门通过，但 exp 检查必须挡住，
// 否则 d.String()/Cmp 会卡死在 big.Int.Exp(10, |exp|)。
func TestRow_DecodeDecimal_RejectsHugeExponentShortPayload(t *testing.T) {
	payload := "0E400000001" // 11 字节 < cap，但解出 exp=400000001
	var buf []byte
	buf = append(buf, RowFormatV2)
	buf = binary.AppendUvarint(buf, 1)
	buf = binary.AppendUvarint(buf, 1)
	buf = binary.AppendUvarint(buf, 1)
	buf = append(buf, byte(FieldTypeDecimal))
	buf = binary.AppendUvarint(buf, uint64(len(payload)))
	buf = append(buf, payload...)
	_, err := DecodeRow(buf)
	if err == nil {
		t.Fatal("expected error for huge-exponent decimal")
	}
	if !strings.Contains(err.Error(), "decimal exponent") {
		t.Fatalf("error %q should mention decimal exponent", err)
	}
}

// 截断的 wire（uvarint 不完整、长度前缀不够 payload 等）不能 panic，必须报错。
func TestRow_DecodeTruncated(t *testing.T) {
	full := Row{Version: 1, Fields: []Field{
		{Num: 1, Value: NewString("hello")},
		{Num: 2, Value: NewInt64(42)},
	}}
	buf, err := EncodeRow(full)
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	// 从所有可能的截断位置（含 0 长度）尝试解码——任何位置都不能 panic。
	for i := 0; i < len(buf); i++ {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic on truncated[:%d]: %v", i, r)
				}
			}()
			_, _ = DecodeRow(buf[:i])
		}()
	}
}
