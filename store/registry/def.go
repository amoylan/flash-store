package registry

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/amoylan/flash-store/store/schema"
	"github.com/shopspring/decimal"
)

// Def 是 per-Kind 的类型化 schema + accessor 绑定，
// 作为字段布局 / 索引集 / 默认值 / 约束 / *T↔Row 转换的单一真相源。
//
// version 由 store 在启动期（afterReplayStarted）自动推导：比对 registry 里
// 注册的当前 schema 与 store.schemasByKind 的缓存——结构相同则沿用老 version，
// 不同则自增。业务**不再**在 RegisterStruct / For 里手动传版本号。
// 访问用 Version()（atomic load），赋值走包内 setVersion（atomic store）。
//
// 字段身份以 Num 为锚：Extract/Fill 靠 byNum 做 wire ↔ reader 映射；Go struct
// 字段声明顺序 / schema 中声明顺序 / wire 里字段顺序三者解耦。
type Def[T any] struct {
	kind    Kind
	version atomic.Uint32 // 启动期由 store 设定；Extract 时读
	fields  []schema.FieldDescriptor
	binders []Binder[T]
	byName  map[schema.FieldName]int
	byNum   map[int32]int                 // Num → fields/binders 中的位置，Fill 的热路径
	indexed []int                         // fields 中 Indexed=true 的位置
	retired map[int32]schema.RetiredField // 业务显式声明的已退休 Num
}

// Binder 把字段级 get/set 绑定到具体的目标 Go 类型 T。
//
// Get == nil：目标 T 不携带该字段 —— Extract 用 spec.Default 代替。
// Set == nil：Fill 跳过该字段（partial Get：读入字段更少的 struct）。
// 新增字段场景由两侧共同支持：读老 Row 时 Row 里没这个位置，Fill 取 Default；
// 写新 Row 时若 T 没这个字段（getter 传 nil），Extract 取 Default。
//
// Set 按指针接收 FieldValue 以避开每字段 72B 的值拷贝；调用方保证指针在
// Set 返回前稳定。
type Binder[T any] struct {
	Get func(*T) schema.FieldValue
	Set func(*T, *schema.FieldValue) error
}

// Kind 返回注册 Kind。
func (d *Def[T]) Kind() Kind { return d.kind }

// Version 返回 schema 版本。Row.Version 在 Extract 时取自这里。
// 由 store 启动期自动推导并通过 SetVersion 设定；业务不直接管。
func (d *Def[T]) Version() uint32 { return d.version.Load() }

// SetVersion 是 store 启动期（afterReplayStarted → resolveSchemaVersions）
// 设定版本的通路。业务不应调用——产线语义由 store 控制。
// 用 atomic.Store 是防御：启动期竞争不大，但确保一致视图。
func (d *Def[T]) SetVersion(v uint32) { d.version.Store(v) }

// Fields 返回字段列表副本，供只读检视。
func (d *Def[T]) Fields() []schema.FieldDescriptor {
	out := make([]schema.FieldDescriptor, len(d.fields))
	copy(out, d.fields)
	return out
}

// IndexedFields 返回所有 Indexed=true 的 FieldDescriptor（副本）。
// shard 建索引时按 spec.Num 从 Row 里取值；相较 IndexedPositions 更 Num-driven。
func (d *Def[T]) IndexedFields() []schema.FieldDescriptor {
	out := make([]schema.FieldDescriptor, 0, len(d.indexed))
	for _, pos := range d.indexed {
		out = append(out, d.fields[pos])
	}
	return out
}

// PositionOf 按字段名查当前 Def 里的位置（供 RowView 等"按名访问"路径使用）。
// 注意：这是 reader 端 schema 位置，不是 wire 上的位置——Row.Fields 是无序的。
func (d *Def[T]) PositionOf(name schema.FieldName) (int, bool) {
	i, ok := d.byName[name]
	return i, ok
}

// FieldByNum 按 Num 返回 reader 端的 FieldDescriptor（副本）。
// shard / view 做 Num→spec 查询走这条。
func (d *Def[T]) FieldByNum(num int32) (schema.FieldDescriptor, bool) {
	pos, ok := d.byNum[num]
	if !ok {
		return schema.FieldDescriptor{}, false
	}
	return d.fields[pos], true
}

// FieldByName 按 Name 返回 reader 端的 FieldDescriptor（副本）。
// RowView / 按名查值 走这条；相较 PositionOf+Fields() 省一次 slice 拷贝。
func (d *Def[T]) FieldByName(name schema.FieldName) (schema.FieldDescriptor, bool) {
	pos, ok := d.byName[name]
	if !ok {
		return schema.FieldDescriptor{}, false
	}
	return d.fields[pos], true
}

// RetiredFields 返回业务显式声明的已退休字段列表（副本）。
// 仅用于测试 / 内省：Builder.Retire 在 init 期已经把 retired Num 注册进 Def，
// 同代码库内新字段撞 Num 会立刻 panic。RetiredField 不参与 KindDescriptor
// 的导出（DroppedFields 由 Store 运行期累积），也不参与跨进程状态传递。
func (d *Def[T]) RetiredFields() []schema.RetiredField {
	out := make([]schema.RetiredField, 0, len(d.retired))
	for _, rf := range d.retired {
		out = append(out, rf)
	}
	return out
}

// Extract 把 *T 转成 Row。Get == nil 的字段使用 spec.Default；
// 类型相关的 normalize（Decimal 精度）在此应用，存储的 Row 已是规范形态。
// Row.Version 取自 Def.Version()（store 启动期设定）。
//
// 产出的 Fields 沿 d.fields 声明顺序——但调用方一律应按 Num 访问，顺序是
// 实现细节、wire decode 回来可能完全不同。
func (d *Def[T]) Extract(obj *T) schema.Row {
	ver := d.Version()
	out := make([]schema.Field, len(d.fields))
	if obj == nil {
		for i, spec := range d.fields {
			out[i] = schema.Field{Num: spec.Num, Value: spec.Default}
		}
		return schema.Row{Version: ver, Fields: out}
	}
	for i, spec := range d.fields {
		var v schema.FieldValue
		if d.binders[i].Get != nil {
			v = d.binders[i].Get(obj)
		} else {
			v = spec.Default
		}
		out[i] = schema.Field{Num: spec.Num, Value: normalize(spec, v)}
	}
	return schema.Row{Version: ver, Fields: out}
}

// Fill 把 Row 回填到 *T。
//
// 语义（num-addressed）：
//   - 每条 row.Fields[i] 用 Num 在 d.byNum 里定位 reader 端位置；
//     找不到 → writer 有、reader 没有的字段，丢弃。
//   - Set == nil 的字段静默跳过（目标 T 不带该字段 → partial Get），但仍计
//     入"已处理"，避免随后走 Default 分支覆盖掉目标结构体的原值。
//   - 某位置未出现在 Row 中（演进新增 / writer schema 不含）且 Set != nil →
//     用 spec.Default 填。
//   - row 里显式的 FieldTypeUndefined 也走 Default 分支。
//
// 实现：源 FieldValue 以指针形态传给 Binder.Set，避免 72 字节值拷贝；
// 指针要么指向 row.Fields[i].Value，要么指向 d.fields[i].Default（两者生命
// 周期都覆盖整个 Fill 调用）。
func (d *Def[T]) Fill(row schema.Row, obj *T) error {
	// seenBitset：bit i = 1 当且仅当 row 为 reader 位置 i 提供了 Num。
	// 小 schema（≤64 字段，绝大多数业务实情）走一个 uint64 的 stack-local bitset，
	// 零分配；字段数超过 64 时降级到 heap []uint64——仍比每 Fill 一次 []bool 省。
	var seenSmall uint64 // ≤64 字段时够用
	var seenLarge []uint64
	const smallLimit = 64
	nFields := len(d.fields)
	if nFields > smallLimit {
		seenLarge = make([]uint64, (nFields+63)/64)
	}
	markSeen := func(pos int) {
		if nFields <= smallLimit {
			seenSmall |= 1 << pos
		} else {
			seenLarge[pos>>6] |= 1 << (pos & 63)
		}
	}
	isSeen := func(pos int) bool {
		if nFields <= smallLimit {
			return seenSmall&(1<<pos) != 0
		}
		return seenLarge[pos>>6]&(1<<(pos&63)) != 0
	}

	for i := range row.Fields {
		num := row.Fields[i].Num
		pos, ok := d.byNum[num]
		if !ok {
			// writer 有、reader 没有的字段——丢弃。
			continue
		}
		markSeen(pos)
		set := d.binders[pos].Set
		if set == nil {
			// 目标 struct 不带此字段（partial Get）——不 Set，也不走 Default。
			continue
		}
		src := &row.Fields[i].Value
		if src.Type() == schema.FieldTypeUndefined {
			// 显式 Undefined → 用 Default。
			defaultVal := d.fields[pos].Default
			src = &defaultVal
		}
		if err := set(obj, src); err != nil {
			return fmt.Errorf("fill %s (num=%d): %w", d.fields[pos].Name, num, err)
		}
	}
	// reader 有但 writer 没给 → Default 补。
	for i, spec := range d.fields {
		if isSeen(i) {
			continue
		}
		set := d.binders[i].Set
		if set == nil {
			continue
		}
		defaultVal := spec.Default
		if err := set(obj, &defaultVal); err != nil {
			return fmt.Errorf("fill %s (num=%d) default: %w", spec.Name, spec.Num, err)
		}
	}
	return nil
}

// Clone 通过 Row 往返深拷贝 *T。便宜且对称：标量值拷贝、Decimal/Time 不可变；
// Custom 字段按引用传递，业务若塞可变对象需自行在 codec 深拷贝。
func (d *Def[T]) Clone(obj *T) *T {
	if obj == nil {
		return nil
	}
	row := d.Extract(obj)
	out := new(T)
	if err := d.Fill(row, out); err != nil {
		panic(fmt.Sprintf("registry.Def[%s].Clone: %v", d.kind, err))
	}
	return out
}

// normalize 应用类型相关的 spec 约束。
func normalize(spec schema.FieldDescriptor, v schema.FieldValue) schema.FieldValue {
	if v.Type() == schema.FieldTypeUndefined {
		return v
	}
	switch spec.Type {
	case schema.FieldTypeDecimal:
		if spec.DecimalPrecision >= 0 {
			if dv, err := v.Decimal(); err == nil {
				return schema.NewDecimal(dv.Truncate(spec.DecimalPrecision))
			}
		}
	}
	return v
}

// ============================================================================
// Builder
// ============================================================================

// Builder 通过链式类型方法装配 *Def[T]。
// Options（schema.Indexed / schema.Default / schema.DecimalPrec 等）在字段注册期
// 校验，类型不匹配立即 panic —— 注册是启动期行为，快速失败优于延后暴露。
//
// 每个方法第一个参数是 `num int32`——字段的稳定身份，业务必须显式分配：
//
//	schema 演进（rename / drop / reorder）通过保持 Num 不变实现兼容；
//	从未使用过的 Num 用于新增字段；删除的 Num 不可再被不同语义的字段复用。
type Builder[T any] struct {
	def *Def[T]
}

// For 开始一个新的 Def[T]。版本由 store 启动期自动推导，业务不需要也无法指定。
func For[T any](kind Kind) *Builder[T] {
	return &Builder[T]{
		def: &Def[T]{
			kind:    kind,
			byName:  make(map[schema.FieldName]int),
			byNum:   make(map[int32]int),
			retired: make(map[int32]schema.RetiredField),
		},
	}
}

// Retire 声明某 Num 已被从 schema 移除。Num 永久占用，之后任何 Builder 的
// 字段方法（Int32/Int64/...）用相同 Num 都会在注册期 panic；启动期 store 会
// 和 cached tombstone 做一致性校验。
//
// 调用顺序任意——可以在字段方法之前或之后调；内部状态检查保证任一顺序下
// num 撞到 alive ↔ retired 都会 panic。
//
// 幂等：同 Num 多次 Retire 调用内容一致时允许（方便业务放在多处 init）；
// 内容冲突（Type/Default/Name 不同）→ panic。
func (b *Builder[T]) Retire(rf schema.RetiredField) *Builder[T] {
	if rf.Num <= 0 {
		panic(fmt.Sprintf("registry.Builder[%s]: Retire num must be > 0 (got %d)",
			b.def.kind, rf.Num))
	}
	if rf.Type == schema.FieldTypeUndefined {
		panic(fmt.Sprintf("registry.Builder[%s]: Retire num=%d Type must be set (tombstone needs historical type for compat check)",
			b.def.kind, rf.Num))
	}
	if pos, exists := b.def.byNum[rf.Num]; exists {
		panic(fmt.Sprintf(
			"registry.Builder[%s]: num=%d is currently alive (field %q); cannot retire an alive num—delete the field first",
			b.def.kind, rf.Num, b.def.fields[pos].Name))
	}
	if prior, exists := b.def.retired[rf.Num]; exists {
		if prior.Type != rf.Type || !prior.Default.Equal(rf.Default) || prior.Name != rf.Name {
			panic(fmt.Sprintf(
				"registry.Builder[%s]: num=%d retired twice with conflicting metadata (old=%+v new=%+v)",
				b.def.kind, rf.Num, prior, rf))
		}
	}
	b.def.retired[rf.Num] = rf
	return b
}

// addField 是内部注册入口——Num 在此处从 Builder 参数赋值到 spec，并做唯一性
// 校验（含 vs alive 的 byNum / byName，以及 vs retired）。Builder 公开的每个
// 类型方法都经过这里。
func (b *Builder[T]) addField(num int32, spec schema.FieldDescriptor, bnd Binder[T]) *Builder[T] {
	if num <= 0 {
		panic(fmt.Sprintf("registry.Builder[%s]: field %q: num must be > 0 (got %d)",
			b.def.kind, spec.Name, num))
	}
	if _, exists := b.def.byName[spec.Name]; exists {
		panic(fmt.Sprintf("registry.Builder[%s]: duplicate field name %q", b.def.kind, spec.Name))
	}
	if prior, exists := b.def.byNum[num]; exists {
		panic(fmt.Sprintf("registry.Builder[%s]: duplicate num=%d (fields %q and %q)",
			b.def.kind, num, b.def.fields[prior].Name, spec.Name))
	}
	if rf, retired := b.def.retired[num]; retired {
		panic(fmt.Sprintf(
			"registry.Builder[%s]: num=%d is retired (was %q, Type=%s); pick a new num for field %q",
			b.def.kind, num, rf.Name, rf.Type, spec.Name))
	}
	spec.Num = num
	pos := len(b.def.fields)
	b.def.fields = append(b.def.fields, spec)
	b.def.binders = append(b.def.binders, bnd)
	b.def.byName[spec.Name] = pos
	b.def.byNum[num] = pos
	if spec.Indexed {
		b.def.indexed = append(b.def.indexed, pos)
	}
	return b
}

// Int32 注册一个 int32 字段。
// getter 为 nil 时表示 T 不携带该字段（schema 演进向后兼容的旧目标类型）——
// Extract 走 spec.Default，Fill 跳过。
func (b *Builder[T]) Int32(num int32, name schema.FieldName, getter func(*T) *int32, opts ...schema.FieldOption) *Builder[T] {
	spec := schema.FieldDescriptor{Name: name, Type: schema.FieldTypeInt32, DecimalPrecision: -1}
	for _, o := range opts {
		o(&spec)
	}
	if spec.Default.Type() == schema.FieldTypeUndefined {
		spec.Default = schema.ZeroDefault(schema.FieldTypeInt32)
	}
	return b.addField(num, spec, scalarBinder(getter,
		func(v *schema.FieldValue) (int32, error) { return v.Int32() },
		schema.NewInt32,
	))
}

// Int64 注册一个 int64 字段。
func (b *Builder[T]) Int64(num int32, name schema.FieldName, getter func(*T) *int64, opts ...schema.FieldOption) *Builder[T] {
	spec := schema.FieldDescriptor{Name: name, Type: schema.FieldTypeInt64, DecimalPrecision: -1}
	for _, o := range opts {
		o(&spec)
	}
	if spec.Default.Type() == schema.FieldTypeUndefined {
		spec.Default = schema.ZeroDefault(schema.FieldTypeInt64)
	}
	return b.addField(num, spec, scalarBinder(getter,
		func(v *schema.FieldValue) (int64, error) { return v.Int64() },
		schema.NewInt64,
	))
}

// String 注册一个 string 字段。方法名与 fmt.Stringer 无冲突——Stringer 要求无参
// String() string，这里签名不同。
func (b *Builder[T]) String(num int32, name schema.FieldName, getter func(*T) *string, opts ...schema.FieldOption) *Builder[T] {
	spec := schema.FieldDescriptor{Name: name, Type: schema.FieldTypeString, DecimalPrecision: -1}
	for _, o := range opts {
		o(&spec)
	}
	if spec.Default.Type() == schema.FieldTypeUndefined {
		spec.Default = schema.ZeroDefault(schema.FieldTypeString)
	}
	return b.addField(num, spec, scalarBinder(getter,
		func(v *schema.FieldValue) (string, error) { return v.String() },
		schema.NewString,
	))
}

// Decimal 注册一个 decimal.Decimal 字段。配合 schema.DecimalPrec(p) 在 Extract
// 时 Truncate 规范化。
func (b *Builder[T]) Decimal(num int32, name schema.FieldName, getter func(*T) *decimal.Decimal, opts ...schema.FieldOption) *Builder[T] {
	spec := schema.FieldDescriptor{Name: name, Type: schema.FieldTypeDecimal, DecimalPrecision: -1}
	for _, o := range opts {
		o(&spec)
	}
	if spec.Default.Type() == schema.FieldTypeUndefined {
		spec.Default = schema.ZeroDefault(schema.FieldTypeDecimal)
	}
	return b.addField(num, spec, scalarBinder(getter,
		func(v *schema.FieldValue) (decimal.Decimal, error) { return v.Decimal() },
		schema.NewDecimal,
	))
}

// Timestamp 注册一个 time.Time 字段。
func (b *Builder[T]) Timestamp(num int32, name schema.FieldName, getter func(*T) *time.Time, opts ...schema.FieldOption) *Builder[T] {
	spec := schema.FieldDescriptor{Name: name, Type: schema.FieldTypeTimestamp, DecimalPrecision: -1}
	for _, o := range opts {
		o(&spec)
	}
	if spec.Default.Type() == schema.FieldTypeUndefined {
		spec.Default = schema.ZeroDefault(schema.FieldTypeTimestamp)
	}
	return b.addField(num, spec, scalarBinder(getter,
		func(v *schema.FieldValue) (time.Time, error) { return v.Time() },
		schema.NewTimestamp,
	))
}

// scalarBinder 把指针式 getter 转成对称的 Binder.Get/Set。
// getter 为 nil → Binder{} 空结构（Get/Set 均为 nil）。
// unbox 接收 *FieldValue（而非值），避免 Fill 热路径 72 字节值拷贝。
func scalarBinder[T any, F any](
	getter func(*T) *F,
	unbox func(*schema.FieldValue) (F, error),
	box func(F) schema.FieldValue,
) Binder[T] {
	if getter == nil {
		return Binder[T]{}
	}
	return Binder[T]{
		Get: func(o *T) schema.FieldValue { return box(*getter(o)) },
		Set: func(o *T, v *schema.FieldValue) error {
			x, err := unbox(v)
			if err != nil {
				return err
			}
			*getter(o) = x
			return nil
		},
	}
}

// Build 收尾并返回 *Def[T]。执行注册期校验：Custom 字段不允许 Indexed。
func (b *Builder[T]) Build() *Def[T] {
	for _, spec := range b.def.fields {
		if spec.Type == schema.FieldTypeCustom && spec.Indexed {
			panic(fmt.Sprintf("registry.Builder[%s]: custom field %q cannot be indexed",
				b.def.kind, spec.Name))
		}
	}
	return b.def
}

// CustomField 注册一个自定义编解码字段。
//
// 由于 Go 方法不允许独立的类型参数，Custom 走包级泛型函数，通过约束 PF 强制
// 指针接收者 *F 同时实现 schema.CustomValuer 和 schema.CustomScanner。
// 编译期即保证业务提供了成对的 Value / Scan。
//
// 内存存储语义：Row 里该字段只存 Value() 产出的 bytes——Get 时调用 Scan 复原
// 出一份新对象，业务无需额外实现 Clone。
//
// 用法：
//
//	type Extra struct { Fee decimal.Decimal }
//	func (e *Extra) Value() ([]byte, error) { return json.Marshal(e) }
//	func (e *Extra) Scan(b []byte) error    { return json.Unmarshal(b, e) }
//
//	registry.CustomField[Order, Extra](b, 5, "extra", func(o *Order) *Extra { return &o.Extra })
//
// 大多数场景 Go 能从 getter 的 *F 推断出 PF=*F，调用者只需传 [Order, Extra] 两个类型参数。
func CustomField[T any, F any, PF interface {
	*F
	schema.CustomValuer
	schema.CustomScanner
}](
	b *Builder[T],
	num int32,
	name schema.FieldName,
	getter func(*T) *F,
	opts ...schema.FieldOption,
) *Builder[T] {
	spec := schema.FieldDescriptor{Name: name, Type: schema.FieldTypeCustom, DecimalPrecision: -1}
	for _, o := range opts {
		o(&spec)
	}
	if spec.Default.Type() == schema.FieldTypeUndefined {
		spec.Default = schema.ZeroDefault(schema.FieldTypeCustom)
	}

	var bnd Binder[T]
	if getter != nil {
		bnd = Binder[T]{
			Get: func(o *T) schema.FieldValue {
				var pf PF = getter(o)
				bytes, err := pf.Value()
				if err != nil {
					// 注册期业务应保证 Value 不失败；出错就写入零长度 payload 以免整条 WAL 崩掉。
					// 不 panic 是为了让业务可以通过观察存储 bytes 大小排查。
					return schema.NewCustomFieldValue([]byte(nil))
				}
				// 防御性拷贝——同 customOffsetBinder 注释，避免 user Value() 返回内部 alias
				// 时 Commit 后业务修改 *T 穿透到存进 shard 的 Row。
				return schema.NewCustomFieldValue(copyCustomBytes(bytes))
			},
			Set: func(o *T, v *schema.FieldValue) error {
				bytes, err := v.CustomBytes()
				if err != nil {
					return err
				}
				var pf PF = getter(o)
				return pf.Scan(bytes)
			},
		}
	}
	return b.addField(num, spec, bnd)
}
