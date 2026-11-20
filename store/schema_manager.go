package store

import (
	"context"

	"github.com/amoylan/flash-store/store/log"
	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/schema"
	"github.com/amoylan/flash-store/store/wal"
)

// fieldsRegistry 是单个 Kind 跨版本累积的字段身份登记簿。
//
// 以 Num 为 key 分别维护 alive 和 tombstone：Num 是字段的永久身份，进 alive、
// 出 tombstone（被删除），tombstone 里的 Num 不可被不同 Type/Default 的字段再次
// 复用——compat 检查通过对比 tombstone 拦截这种不一致。
//
// 设计上不存完整历史 KindDescriptor（每版一份）——那份数据对 Fill 没用，对
// Compat 也只关心"某 Num 曾经的 Type/Default"；存摘要足够，空间从 Σ(V*F) 降
// 为 Σ(F+D)。
type fieldsRegistry struct {
	alive         map[int32]schema.FieldDescriptor
	tombstone     map[int32]schema.FieldDescriptor
	latestVersion uint32 // alive 对应的 Version
}

func newFieldsRegistry() *fieldsRegistry {
	return &fieldsRegistry{
		alive:     make(map[int32]schema.FieldDescriptor),
		tombstone: make(map[int32]schema.FieldDescriptor),
	}
}

// resolveSchemaVersions 在 afterReplayStarted 里、compat 检查之前运行（仅 Master）。
//
// 对每个已注册 Def：
//   - 没有任何历史缓存（全新 Kind）→ version=1
//   - 有历史缓存且最新版本 schema 与当前代码结构等价 → 沿用 latest.Version
//   - 有历史缓存但结构变了（兼容演进）→ latest.Version+1
//
// 以 latest（最高 Version）为对照点，是因为"等价沿用老版本号"只在最新版本
// 才有意义；如果和历史某个更老版本恰好结构等价，那也是演进链条中的中间态，
// 当前演进仍应是 latest+1。
//
// Def.SetVersion 通过 atomic store，确保之后的 Extract 都读到最终版本。
// 注意：此函数只赋版本号，不做 compat 校验——compat 在 checkSchemasCompat 里独立做。
func (s *Store) resolveSchemaVersions() {
	registry.VisitDefs(func(d registry.TypedDef) {
		latest, ok := s.LatestKnownSchema(d.Kind())
		if !ok {
			d.SetVersion(1)
			return
		}
		curDesc := registry.DescriptorOf(d)
		// 暂时把 d 的 version 同步到 latest 的——这样 DescriptorOf 对比时 version 不干扰。
		curDesc.Version = latest.Version
		if schemaStructurallyEqual(latest, curDesc) {
			d.SetVersion(latest.Version)
		} else {
			d.SetVersion(latest.Version + 1)
		}
	})
}

// schemaStructurallyEqual 判两个 KindDescriptor 除 Version 外是否完全相同。
// 按 Num 身份配对比较（而非位置）——两份 descriptor 可能一个来自 Def 声明顺序、
// 一个来自 alive map 升序导出，字段顺序不同不代表结构不同。
// 用于 auto-version：结构等价则沿用老 version，不等价则递增。
func schemaStructurallyEqual(a, b schema.KindDescriptor) bool {
	if a.Kind != b.Kind {
		return false
	}
	if len(a.Fields) != len(b.Fields) {
		return false
	}
	aByNum := make(map[int32]schema.FieldDescriptor, len(a.Fields))
	for _, f := range a.Fields {
		aByNum[f.Num] = f
	}
	for _, bf := range b.Fields {
		af, ok := aByNum[bf.Num]
		if !ok {
			return false
		}
		if af.Name != bf.Name ||
			af.Type != bf.Type ||
			af.Indexed != bf.Indexed ||
			af.DecimalPrecision != bf.DecimalPrecision ||
			af.StringMaxLen != bf.StringMaxLen ||
			!af.Default.Equal(bf.Default) {
			return false
		}
	}
	return true
}

// PublishSchemas 通过 worker 0 向 Slave 广播当前注册的所有 Kind 的 schema（仅 Master 调用）。
// 必须在 resolveSchemaVersions（赋 Version）+ checkSchemasCompat 之后调用。
//
// 为什么要广播：SnapshotSlave 不注册业务 struct，无法自己获取 schema。
// 这条 WAL 是 slave 拿到真实 schema 信息的唯一渠道，供 slave 在本地 AddSchema 到
// 多版本缓存，用于 Fill（按 writer 版本查表）和 snapshot Header 持久化。
//
// 语义：增量 add-only。master 只广播"当前代码"的版本（每个 Kind 一条），
// slave 把它 AddSchema 进自己的历史缓存——历史版本由 snapshot header 负责载入。
// 每次 master 启动广播一次即可；下次 master 重启（可能带 schema 演进）会再广播
// 一条新版本，累加到历史中。
//
// 广播只携带 alive Fields（DescriptorOf 不导 DroppedFields）。slave 的 tombstone
// 由 fieldsRegistry.upsert 里"老 alive 在新 alive 中消失"的 diff 规则增量推导；
// fresh 节点（无 prior alive）的 tombstone 由 snapshot Header.Schemas 一次性载入，
// 而不是依赖广播搬运。
func (s *Store) PublishSchemas(ctx context.Context) error {
	descs := registry.AllDescriptors()
	if len(descs) == 0 {
		return nil // 没有注册的 Kind，无需广播
	}

	// 仅通过 worker 0 广播（全局配置）。
	w := s.workers[0]
	seq := w.nextSeq()
	walItem := &wal.Wal{Schemas: descs}
	if _, err := w.appendWal(ctx, wal.WalTypeSchemaConfig, seq, walItem); err != nil {
		return err
	}

	// 同步更新 master 自己的 schemasByKind：merge alive（并把被 drop 的 Num 移到 tombstone）。
	for _, d := range descs {
		s.UpsertSchema(d)
	}
	return nil
}

// KnownSchemas 返回每个 Kind 一条 KindDescriptor（alive + tombstone 组装）。
// CreateSnapshot 用这个填 Header.Schemas——Slave 不注册业务 struct、无法自己从
// registry 拉；Master 也需要把 tombstone 持久化，下次 restore 后 compat 检查
// 仍能拦截跨版本 Num 复用。
func (s *Store) KnownSchemas() []schema.KindDescriptor {
	s.schemasByKindMu.RLock()
	defer s.schemasByKindMu.RUnlock()
	if len(s.schemasByKind) == 0 {
		return nil
	}
	out := make([]schema.KindDescriptor, 0, len(s.schemasByKind))
	for kind, reg := range s.schemasByKind {
		out = append(out, reg.toDescriptor(kind.String()))
	}
	return out
}

// LatestKnownSchema 返回 Kind 下当前 alive 的 schema 描述（不含 tombstone）。
// 用于 master 启动期 resolveSchemaVersions 对比"当前代码 vs 上一跳 alive"。
func (s *Store) LatestKnownSchema(kind registry.Kind) (schema.KindDescriptor, bool) {
	s.schemasByKindMu.RLock()
	defer s.schemasByKindMu.RUnlock()
	reg, ok := s.schemasByKind[kind]
	if !ok {
		return schema.KindDescriptor{}, false
	}
	return reg.aliveDescriptor(kind.String()), true
}

// UpsertSchema 把一条 KindDescriptor 合并到 alive + tombstone 登记簿。
//
// 合并语义：
//   - desc.Fields 里的每个字段 → 写入 alive（同 Num 存在时覆盖，前提是 compat check
//     已在上游通过）
//   - 之前 alive 里有但 desc.Fields 里没有的 Num → 移到 tombstone
//   - desc.DroppedFields 里的字段 → 直接并入 tombstone
//   - latestVersion 更新为 desc.Version
//
// desc.DroppedFields 的两条来源：
//   - **snapshot Header restore**：Header.Schemas 由 KnownSchemas() 导出，含 Store
//     当前累积的 tombstone（alive + dropped），restore 后 tombstone 完整恢复
//   - **broadcast**：当前实现里 broadcast 来自 registry.AllDescriptors()，DroppedFields
//     恒空（RetiredField 不再喂入 descriptor），slave 端 tombstone 全靠"alive 消失"的
//     diff 规则增量累积
//
// 幂等：同一份 desc 多次调用结果一致。广播和 restore 都走同一个入口。
//
// 本方法**不做** compat 检查——上游（checkSchemasCompat / validateSchemaConfig）
// 负责拦截非法演进；此处纯状态合并。
func (s *Store) UpsertSchema(desc schema.KindDescriptor) {
	kind, err := registry.ParseKindFromString(desc.Kind)
	if err != nil {
		log.Warn("UpsertSchema: invalid kind, skipped",
			log.String("kind", desc.Kind), log.NamedError("err", err))
		return
	}
	s.schemasByKindMu.Lock()
	defer s.schemasByKindMu.Unlock()
	reg, ok := s.schemasByKind[kind]
	if !ok {
		reg = newFieldsRegistry()
		s.schemasByKind[kind] = reg
	}
	// 防御性拦截：版本号倒退说明上游传了陈旧或重复的 descriptor。
	// 正常路径（KnownSchemas 每 Kind 一条 latest、broadcast 路径每 Kind 一条 latest）
	// 不会触发；但 Header 损坏 / 测试构造 / 未来加新调用方时可能踩到——
	// 让 last-wins 静默接受会把老 schema 写回 alive，覆盖更新的 fields，难以发现。
	// 等版本视为幂等更新（同 desc 多次调用结果一致），仍允许通过。
	if desc.Version < reg.latestVersion {
		log.Warn("UpsertSchema: ignoring stale descriptor",
			log.String("kind", desc.Kind),
			log.Uint32("desc_version", desc.Version),
			log.Uint32("latest_version", reg.latestVersion))
		return
	}
	reg.upsert(desc)
}

// upsert 按 latest-alive 语义把 desc 合并进 reg。见 Store.UpsertSchema 的注释。
func (r *fieldsRegistry) upsert(desc schema.KindDescriptor) {
	newAlive := make(map[int32]schema.FieldDescriptor, len(desc.Fields))
	for _, f := range desc.Fields {
		newAlive[f.Num] = f
	}
	// 老 alive 里在 newAlive 里没有的 → 迁到 tombstone（保留最后一次的 Type/Default）
	for num, f := range r.alive {
		if _, still := newAlive[num]; !still {
			r.tombstone[num] = f
		}
	}
	// 接收外部显式传的 tombstone（snapshot restore 场景）
	for _, f := range desc.DroppedFields {
		r.tombstone[f.Num] = f
	}
	// 如果某 Num 现在是 alive 了，就不应该同时出现在 tombstone——清理
	for num := range newAlive {
		delete(r.tombstone, num)
	}
	r.alive = newAlive
	r.latestVersion = desc.Version
}

// toDescriptor 把 reg 导出为 KindDescriptor（alive Fields + tombstone DroppedFields）。
// snapshot Header 用。
func (r *fieldsRegistry) toDescriptor(kindStr string) schema.KindDescriptor {
	alive := make([]schema.FieldDescriptor, 0, len(r.alive))
	for _, f := range r.alive {
		alive = append(alive, f)
	}
	// 按 Num 升序导出，便于 snapshot / diff 工具稳定读。
	sortFieldsByNum(alive)

	var dropped []schema.FieldDescriptor
	if len(r.tombstone) > 0 {
		dropped = make([]schema.FieldDescriptor, 0, len(r.tombstone))
		for _, f := range r.tombstone {
			dropped = append(dropped, f)
		}
		sortFieldsByNum(dropped)
	}
	return schema.KindDescriptor{
		Kind:          kindStr,
		Version:       r.latestVersion,
		Fields:        alive,
		DroppedFields: dropped,
	}
}

// aliveDescriptor 只含 alive（不含 tombstone）——供 resolveSchemaVersions 对比
// "当前代码 vs 上一跳 alive" 结构等价用。
func (r *fieldsRegistry) aliveDescriptor(kindStr string) schema.KindDescriptor {
	alive := make([]schema.FieldDescriptor, 0, len(r.alive))
	for _, f := range r.alive {
		alive = append(alive, f)
	}
	sortFieldsByNum(alive)
	return schema.KindDescriptor{
		Kind:    kindStr,
		Version: r.latestVersion,
		Fields:  alive,
	}
}

func sortFieldsByNum(fs []schema.FieldDescriptor) {
	for i := 1; i < len(fs); i++ {
		for j := i; j > 0 && fs[j-1].Num > fs[j].Num; j-- {
			fs[j-1], fs[j] = fs[j], fs[j-1]
		}
	}
}
