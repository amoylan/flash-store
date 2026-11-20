package registry

import (
	"fmt"
	"sync"

	"github.com/amoylan/flash-store/store/schema"
)

// TypedDef 是 *Def[T] 的擦除视图。registry 按 Kind 查找时无法携带 T，
// 因此 shard / index 等按 Kind 走的代码通过 GetTypedDef 拿到字段布局；
// 需要 Extract/Fill/Clone 的业务代码走 GetDef[T]。
//
// SetVersion 属于 store 启动期的契约——store 在 afterReplayStarted 里比对
// 当前 schema 与缓存后，调 SetVersion 赋予最终版本号。业务代码不应调用。
//
// 字段访问以 Num 为主（FieldByNum / IndexedFields）；PositionOf(name) 为
// RowView 按名查值保留。
type TypedDef interface {
	Kind() Kind
	Version() uint32
	SetVersion(uint32)
	Fields() []schema.FieldDescriptor
	IndexedFields() []schema.FieldDescriptor
	PositionOf(name schema.FieldName) (int, bool)
	FieldByNum(num int32) (schema.FieldDescriptor, bool)
	FieldByName(name schema.FieldName) (schema.FieldDescriptor, bool)
	RetiredFields() []schema.RetiredField
}

// 编译期校验：*Def[T] 必须实现 TypedDef。
var _ TypedDef = (*Def[struct{}])(nil)

// RegisterDef 注册 Def[T]。Kind 全局唯一。
// 传入 Builder.Build() 的返回值。
func RegisterDef[T any](def *Def[T]) error {
	return defaultRegistry.register(def)
}

// GetTypedDef 返回擦除 T 的 TypedDef。
func GetTypedDef(kind Kind) (TypedDef, bool) {
	return defaultRegistry.get(kind)
}

// GetDef 返回类型化 *Def[T]，要求注册时的 T 与当前调用一致。
// 不一致时 ok=false（类型断言失败）。
func GetDef[T any](kind Kind) (*Def[T], bool) {
	v, ok := defaultRegistry.get(kind)
	if !ok {
		return nil, false
	}
	d, ok := v.(*Def[T])
	return d, ok
}

// registry 是 Def[T] 的进程级注册表：每个 Kind 对应一个 TypedDef
// （擦除 T 后的接口视图）。封装锁和字典，外部只通过包级 API
// （RegisterDef / GetDef / GetTypedDef / VisitDefs / AllDescriptors）访问。
type registry struct {
	mu   sync.RWMutex
	defs map[Kind]TypedDef
}

// defaultRegistry 是业务进程共享的默认注册表。业务在 init() 里调
// RegisterDef / RegisterStruct 写入；运行期 GetDef / GetTypedDef 读。
// 故意不导出——所有跨包访问必须走 API 函数，避免再度把锁和 map 暴露出去。
var defaultRegistry = &registry{
	defs: make(map[Kind]TypedDef),
}

// ErrKindAlreadyRegistered 当同一 Kind 被重复注册时返回。
var ErrKindAlreadyRegistered = fmt.Errorf("kind already registered")

// register 注册一个 TypedDef，Kind 重复返回 ErrKindAlreadyRegistered。
func (r *registry) register(def TypedDef) error {
	if def == nil {
		return fmt.Errorf("registry.register: nil def")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	kind := def.Kind()
	if _, exists := r.defs[kind]; exists {
		return ErrKindAlreadyRegistered
	}
	r.defs[kind] = def
	return nil
}

// UnregisterKind 把指定 Kind 从全局注册表中移除。**仅供测试使用**：模拟"业务
// 重启时换了一份不同字段的 struct 注册同一个 Kind"——schema_manager 的版本
// 自动解析逻辑只在 Master 启动期对全局 registry 做一次 visit，所以集成测试
// 必须能在两次 NewStore 之间换掉 Def，否则没法走真实的 register → resolve →
// broadcast → snapshot 链路。生产代码绝不应该调它（业务的 Def 注册一锤子买卖）。
func UnregisterKind(kind Kind) {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	delete(defaultRegistry.defs, kind)
}

// get 按 Kind 取 TypedDef；不存在返回 (nil, false)。
func (r *registry) get(kind Kind) (TypedDef, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.defs[kind]
	return d, ok
}

// visit 在读锁下遍历所有注册的 TypedDef。调用方的 fn 不应再回调本 registry，
// 以免自锁——这是一个实现细节上的约束，但在当前调用模式下容易满足。
func (r *registry) visit(fn func(TypedDef)) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, def := range r.defs {
		fn(def)
	}
}
