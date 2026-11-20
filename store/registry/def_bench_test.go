package registry

import (
	"testing"
	"time"

	"github.com/amoylan/flash-store/store/schema"
	"github.com/shopspring/decimal"
)

// 覆盖典型的字段类型组合，测量 Extract/Fill/EncodeRow/DecodeRow 热路径。
type benchRec struct {
	ID     int64
	Status int32
	Price  decimal.Decimal
	Tag    string
	Ts     time.Time
}

func benchDef(b testing.TB) *Def[benchRec] {
	b.Helper()
	kind, err := NewKind("bench", "rec")
	if err != nil {
		b.Fatalf("NewKind: %v", err)
	}
	return For[benchRec](kind).
		Int64(1, "id", func(r *benchRec) *int64 { return &r.ID }).
		Int32(2, "status", func(r *benchRec) *int32 { return &r.Status }).
		Decimal(3, "price", func(r *benchRec) *decimal.Decimal { return &r.Price }).
		String(4, "tag", func(r *benchRec) *string { return &r.Tag }).
		Timestamp(5, "ts", func(r *benchRec) *time.Time { return &r.Ts }).
		Build()
}

func benchSample() *benchRec {
	return &benchRec{
		ID:     42,
		Status: 1,
		Price:  decimal.NewFromInt(1234),
		Tag:    "hello",
		Ts:     time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC),
	}
}

func BenchmarkExtract(b *testing.B) {
	def := benchDef(b)
	rec := benchSample()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = def.Extract(rec)
	}
}

func BenchmarkFill(b *testing.B) {
	def := benchDef(b)
	row := def.Extract(benchSample())
	out := &benchRec{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = def.Fill(row, out)
	}
}

func BenchmarkEncodeRow(b *testing.B) {
	def := benchDef(b)
	row := def.Extract(benchSample())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = schema.EncodeRow(row)
	}
}

func BenchmarkDecodeRow(b *testing.B) {
	def := benchDef(b)
	buf, _ := schema.EncodeRow(def.Extract(benchSample()))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = schema.DecodeRow(buf)
	}
}

// Tag-based path 的 Extract 热路径，和 Builder 路径对比：accessor 走
// unsafe.Pointer + offset，注册期一次反射，hot path 零反射。
type benchTagRec struct {
	ID     int64           `store:"id,num=1"`
	Status int32           `store:"status,num=2,index"`
	Price  decimal.Decimal `store:"price,num=3,index,prec=8"`
	Tag    string          `store:"tag,num=4"`
	Ts     time.Time       `store:"ts,num=5"`
}

func benchTagDef(b testing.TB) *Def[benchTagRec] {
	b.Helper()
	kind, err := NewKind("bench", "tagrec")
	if err != nil {
		b.Fatal(err)
	}
	def, err := BuildFromStruct[benchTagRec](kind)
	if err != nil {
		b.Fatal(err)
	}
	return def
}

func benchTagSample() *benchTagRec {
	return &benchTagRec{
		ID:     42,
		Status: 1,
		Price:  decimal.NewFromInt(1234),
		Tag:    "hello",
		Ts:     time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC),
	}
}

func BenchmarkExtractViaTag(b *testing.B) {
	def := benchTagDef(b)
	rec := benchTagSample()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = def.Extract(rec)
	}
}

func BenchmarkFillViaTag(b *testing.B) {
	def := benchTagDef(b)
	row := def.Extract(benchTagSample())
	out := &benchTagRec{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = def.Fill(row, out)
	}
}

// 只测 NewFieldValue 的原始箱开销（最本质的热点）。
func BenchmarkNewFieldValue_Scalars(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = schema.NewFieldValue[int32](7)
		_ = schema.NewFieldValue[int64](1 << 40)
		_ = schema.NewFieldValue(time.Now())
	}
}
