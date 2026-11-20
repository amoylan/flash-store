package registry

import "github.com/amoylan/flash-store/store/schema"

// DescriptorOf 把 TypedDef 导出为可序列化的 KindDescriptor。
// 只导出 alive Fields——DroppedFields（tombstone）由 Store 在运行期累积，
// 不来自源码里的 RetiredField 声明。RetiredField 仅作为 Builder 期的同代码库
// Num 防撞工具，不再承担跨进程状态传递职责。
func DescriptorOf(d TypedDef) schema.KindDescriptor {
	return schema.KindDescriptor{
		Kind:    d.Kind().String(),
		Version: d.Version(),
		Fields:  d.Fields(),
	}
}

// AllDescriptors 返回当前注册表里所有 Kind 的 schema 快照。
// snapshot CreateSnapshot 时调用它写入 Header.Schemas；restore 时对比当前。
// 顺序不保证稳定（map 迭代），调用方不要依赖顺序。
func AllDescriptors() []schema.KindDescriptor {
	var out []schema.KindDescriptor
	defaultRegistry.visit(func(def TypedDef) {
		out = append(out, DescriptorOf(def))
	})
	return out
}

// VisitDefs 遍历所有已注册的 Def（擦除 T 后的 TypedDef 形态）。
// 供 store 启动期做版本解析使用。fn 在读锁下被调用——不要在 fn 内部再调
// RegisterDef / GetDef 等会取锁的 API，否则自锁。
func VisitDefs(fn func(d TypedDef)) {
	defaultRegistry.visit(fn)
}
