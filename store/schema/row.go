package schema

// Row 是内存存储原语：一条记录的 num-addressed 字段集合。
//
// 字段身份以 Num 为锚点（不是位置）。Fields 无序，Def.Fill 通过 byNum 映射
// 定位。这样 Go struct 字段顺序、schema 里声明顺序、wire 里编码顺序三者
// 完全解耦，后续 rename / drop / reorder 字段都不影响 wire 兼容性。
//
// Row 在 in-memory / WAL / snapshot 共享同一形态：
//   - shard 直接存 Row（含 Master / ReadOnlySlave / SnapshotSlave）
//   - WAL / snapshot 里 Row 走 EncodeRow/DecodeRow 的自描述 bytes
type Row struct {
	// Version 记录写入时刻的 schema 版本。Fill 用它从 store 多版本 schema
	// 缓存里查 writer schema，做 Num-驱动的映射（replay/restore 同样依赖）。
	Version uint32
	// Fields 是无序字段集合。每个 Field 自带 Num 作为身份；Extract 产出的
	// 顺序沿 Def.Fields() 声明顺序，Decode 产出的顺序沿 wire 编码顺序——
	// 调用方都不应依赖顺序，一律按 Num 访问。
	Fields []Field
}

// Field 是 Row 中的一项：Num 是稳定身份，Value 是 tagged-union FieldValue。
type Field struct {
	Num   int32
	Value FieldValue
}

// Clone 深拷贝 Row。FieldValue 的标量槽 / Decimal / Time 都是值语义；
// Custom 字段内部的 []byte 按引用透传——业务若把可变 bytes 塞进 Custom，
// 必须在自己的 codec 侧负责深拷贝。Num 是 int32 值类型，随 copy 即可。
func (r Row) Clone() Row {
	if r.Fields == nil {
		return Row{Version: r.Version}
	}
	out := make([]Field, len(r.Fields))
	copy(out, r.Fields)
	return Row{Version: r.Version, Fields: out}
}

// Len 返回 Row 实际存储的字段数（无序；可能少于 schema 的字段数——演进入口）。
func (r Row) Len() int {
	return len(r.Fields)
}

// GetByNum 按 Num 线性扫描 Row 字段，命中则返回 FieldValue + true。
// 少字段场景线性扫描比建 map 更省（Row 通常几到十几个字段）；
// 热路径 Fill 走 Def.byNum 预建表，不走这条。
func (r Row) GetByNum(num int32) (FieldValue, bool) {
	for i := range r.Fields {
		if r.Fields[i].Num == num {
			return r.Fields[i].Value, true
		}
	}
	return FieldValue{}, false
}
