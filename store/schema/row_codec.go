package schema

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/shopspring/decimal"
)

// Row wire format v2 —— num-addressed、自描述。
//
//	Row     = [format_ver: 1 byte = 0x02]
//	          [schema_ver: uvarint32]
//	          [nFields: uvarint32]
//	          field * nFields
//
//	Field   = [num: uvarint32]
//	          [typeTag: 1 byte]   // FieldType iota
//	          [payload]
//
//	typeTag 值等于 FieldType 的 iota 序号：
//	  0 Undefined → 无 payload
//	  1 Int32     → [int32: svarint]
//	  2 Int64     → [int64: svarint]
//	  3 String    → [len: uvarint32] [utf8 bytes]
//	  4 Decimal   → [len: uvarint32] [decimal 规范字符串 bytes]
//	  5 Timestamp → [unixNano: svarint]
//	  6 Custom    → [len: uvarint32] [业务 CustomValuer 产出的 bytes]
//
// 设计目标：
//   - 自描述：每字段带 typeTag，slave 无 schema 也能解码成 Row。
//   - Num 驱动：字段身份是 Num，不是 Row.Fields 切片位置——业务可以任意
//     重排 Go struct 字段、rename、drop、在 schema 中 reorder，只要 Num 不变，
//     读写就兼容。
//   - 紧凑：整数/时间走 varint，字符串/Decimal 长度前缀；格式整体和 proto 风格一致。
//
// 版本演进：
//   - 0x02 是当前唯一支持的 wire format。
//   - 老 v1 格式（无 format_ver 前缀、字段无 num、按位置对齐）在 dev 期
//     已整体废弃，无向后兼容负担。

// RowFormatV2 是当前 Row wire format 的版本 byte。
const RowFormatV2 byte = 0x02

// maxDecimalStringLen 是 Decimal payload 字节长度的硬上限。
// 业务上限来自 MySQL DECIMAL(36,18)：18 整数 + '.' + 18 小数 + 可选 '-'
// = 最长 38 字节。给到 128 是宽松上限，业务精度翻几番也不会触顶。
// 任何合法 EncodeRow 永远短于此值。
const maxDecimalStringLen = 128

// maxDecimalAbsExp 是解析后 Decimal exp 的绝对值上限——单独卡长度不够：
// shopspring/decimal 的 NewFromString 接受形如 "0E400000001" 的输入，长度只有
// 11 字节但解出 Decimal{value:0, exp:400000001}，后续任何 String()/Cmp/Equal
// 调用都要 rescale，分配 10^400000001 大整数 → OOM/hang。
// 业务 (36,18) 的合法 exp 范围是 [-18, 0]；给到 ±64 留足缓冲，
// 远低于会触发 big.Int.Exp 卡顿的量级。
const maxDecimalAbsExp = 64

// EncodeRow 把 Row 序列化为自描述字节流。
func EncodeRow(r Row) ([]byte, error) {
	var buf []byte
	buf = append(buf, RowFormatV2)
	buf = binary.AppendUvarint(buf, uint64(r.Version))
	buf = binary.AppendUvarint(buf, uint64(len(r.Fields)))
	// Num 在 Row 内必须唯一——重复 Num 会让读端的 GetByNum 语义模糊
	// (first-write-wins vs last-write-wins)，在编码阶段就 fail-fast。
	var seen map[int32]struct{}
	if len(r.Fields) > 1 {
		seen = make(map[int32]struct{}, len(r.Fields))
	}
	for i, f := range r.Fields {
		if f.Num <= 0 {
			return nil, fmt.Errorf("encode row field[%d]: invalid num=%d (must be > 0)", i, f.Num)
		}
		if seen != nil {
			if _, dup := seen[f.Num]; dup {
				return nil, fmt.Errorf("encode row field[%d]: duplicate num=%d", i, f.Num)
			}
			seen[f.Num] = struct{}{}
		}
		buf = binary.AppendUvarint(buf, uint64(f.Num))
		enc, err := appendFieldValue(buf, f.Value)
		if err != nil {
			return nil, fmt.Errorf("encode row field[%d] (num=%d): %w", i, f.Num, err)
		}
		buf = enc
	}
	return buf, nil
}

// DecodeRow 从自描述字节流恢复 Row。
func DecodeRow(data []byte) (Row, error) {
	if len(data) == 0 {
		return Row{}, fmt.Errorf("decode row: empty input")
	}
	if data[0] != RowFormatV2 {
		return Row{}, fmt.Errorf("decode row: unsupported format byte 0x%02x (want 0x%02x)", data[0], RowFormatV2)
	}
	data = data[1:]

	ver, n := binary.Uvarint(data)
	if n <= 0 {
		return Row{}, fmt.Errorf("decode row: version varint")
	}
	data = data[n:]

	nFields, n := binary.Uvarint(data)
	if n <= 0 {
		return Row{}, fmt.Errorf("decode row: field count varint")
	}
	data = data[n:]

	// nFields 上界保护——防止损坏 header 触发巨量预分配。每个 field 至少
	// 2 字节（num uvarint ≥1 + typeTag 1 byte），所以宽松上界就是剩余字节数。
	if nFields > uint64(len(data)) {
		return Row{}, fmt.Errorf("decode row: nFields=%d exceeds remaining bytes=%d", nFields, len(data))
	}

	fields := make([]Field, nFields)
	for i := uint64(0); i < nFields; i++ {
		num, n := binary.Uvarint(data)
		if n <= 0 {
			return Row{}, fmt.Errorf("decode row field[%d]: num varint", i)
		}
		data = data[n:]

		v, rest, err := readFieldValue(data)
		if err != nil {
			return Row{}, fmt.Errorf("decode row field[%d] (num=%d): %w", i, num, err)
		}
		fields[i] = Field{Num: int32(num), Value: v}
		data = rest
	}
	if len(data) != 0 {
		return Row{}, fmt.Errorf("decode row: %d trailing bytes", len(data))
	}
	return Row{Version: uint32(ver), Fields: fields}, nil
}

func appendFieldValue(buf []byte, f FieldValue) ([]byte, error) {
	switch f.fieldType {
	case FieldTypeUndefined:
		return append(buf, byte(FieldTypeUndefined)), nil
	case FieldTypeInt32:
		buf = append(buf, byte(FieldTypeInt32))
		x, _ := f.Int32()
		return binary.AppendVarint(buf, int64(x)), nil
	case FieldTypeInt64:
		buf = append(buf, byte(FieldTypeInt64))
		x, _ := f.Int64()
		return binary.AppendVarint(buf, x), nil
	case FieldTypeString:
		buf = append(buf, byte(FieldTypeString))
		s, _ := f.String()
		buf = binary.AppendUvarint(buf, uint64(len(s)))
		return append(buf, s...), nil
	case FieldTypeDecimal:
		buf = append(buf, byte(FieldTypeDecimal))
		d, _ := f.Decimal()
		// Decimal 的 Canonical 字符串确保 Equal 语义下的稳定往返；
		// 不直接用 GobEncode / proto bytes 避免再引一层依赖。
		s := d.String()
		buf = binary.AppendUvarint(buf, uint64(len(s)))
		return append(buf, s...), nil
	case FieldTypeTimestamp:
		// 直接写底层 i64：避免 Time() 把 zero-time 物化成 time.Time{} 后再
		// 取 UnixNano()——zero time 的 UnixNano 是 undefined 区域，会破坏
		// 字节级 round-trip。i64=0 表示 zero time，svarint(0) 占 1 字节。
		buf = append(buf, byte(FieldTypeTimestamp))
		return binary.AppendVarint(buf, f.i64), nil
	case FieldTypeCustom:
		buf = append(buf, byte(FieldTypeCustom))
		// Custom 内部总是 []byte（Extract 时已由 CustomValuer 编码）。
		// 走无 any 版本 CustomBytes 直接拿底层切片，避免再箱一次。
		b, _ := f.CustomBytes()
		buf = binary.AppendUvarint(buf, uint64(len(b)))
		return append(buf, b...), nil
	}
	return nil, fmt.Errorf("unknown FieldType %d", f.fieldType)
}

func readFieldValue(data []byte) (FieldValue, []byte, error) {
	if len(data) == 0 {
		return FieldValue{}, nil, fmt.Errorf("empty field payload")
	}
	tag := FieldType(data[0])
	data = data[1:]
	switch tag {
	case FieldTypeUndefined:
		return FieldValue{}, data, nil
	case FieldTypeInt32:
		x, n := binary.Varint(data)
		if n <= 0 {
			return FieldValue{}, nil, fmt.Errorf("int32 varint")
		}
		// 损坏 / 伪造的 wire 可能在 Int32 槽里塞超 int32 范围的值——
		// 静默截断会让数据无声损坏，明确报错。
		if x < math.MinInt32 || x > math.MaxInt32 {
			return FieldValue{}, nil, fmt.Errorf("int32 value %d out of range", x)
		}
		return NewInt32(int32(x)), data[n:], nil
	case FieldTypeInt64:
		x, n := binary.Varint(data)
		if n <= 0 {
			return FieldValue{}, nil, fmt.Errorf("int64 varint")
		}
		return NewInt64(x), data[n:], nil
	case FieldTypeString:
		l, n := binary.Uvarint(data)
		if n <= 0 {
			return FieldValue{}, nil, fmt.Errorf("string len varint")
		}
		data = data[n:]
		if uint64(len(data)) < l {
			return FieldValue{}, nil, fmt.Errorf("string payload truncated")
		}
		return NewString(string(data[:l])), data[l:], nil
	case FieldTypeDecimal:
		l, n := binary.Uvarint(data)
		if n <= 0 {
			return FieldValue{}, nil, fmt.Errorf("decimal len varint")
		}
		data = data[n:]
		if uint64(len(data)) < l {
			return FieldValue{}, nil, fmt.Errorf("decimal payload truncated")
		}
		// 防御性长度门：任何合法 EncodeRow 输出都短于此（见 maxDecimalStringLen）。
		if l > maxDecimalStringLen {
			return FieldValue{}, nil, fmt.Errorf("decimal payload length %d exceeds max %d", l, maxDecimalStringLen)
		}
		s := string(data[:l])
		d, err := decimal.NewFromString(s)
		if err != nil {
			return FieldValue{}, nil, fmt.Errorf("decimal parse %q: %w", s, err)
		}
		// 防御科学计数法解出的极端 exp——长度短但 exp 巨大，后续 d.String()/Cmp
		// 会触发 big.Int 大整数分配 (10^|exp|) 导致 OOM/hang。
		if e := d.Exponent(); e > maxDecimalAbsExp || e < -maxDecimalAbsExp {
			return FieldValue{}, nil, fmt.Errorf("decimal exponent %d out of [-%d, %d]", e, maxDecimalAbsExp, maxDecimalAbsExp)
		}
		return NewDecimal(d), data[l:], nil
	case FieldTypeTimestamp:
		x, n := binary.Varint(data)
		if n <= 0 {
			return FieldValue{}, nil, fmt.Errorf("timestamp varint")
		}
		// 直接构造 FieldValue，绕开 NewTimestamp(time.Unix...) 这一跳——
		// i64=0 表示 zero time，由 Time() 读取时分派；非 0 才走真实时间。
		return FieldValue{fieldType: FieldTypeTimestamp, i64: x}, data[n:], nil
	case FieldTypeCustom:
		l, n := binary.Uvarint(data)
		if n <= 0 {
			return FieldValue{}, nil, fmt.Errorf("custom len varint")
		}
		data = data[n:]
		if uint64(len(data)) < l {
			return FieldValue{}, nil, fmt.Errorf("custom payload truncated")
		}
		// 复制一份 payload，避免跟外层 wire buffer 共享底层数组。
		b := make([]byte, l)
		copy(b, data[:l])
		return NewCustomFieldValue(b), data[l:], nil
	}
	return FieldValue{}, nil, fmt.Errorf("unknown typeTag %d", tag)
}
