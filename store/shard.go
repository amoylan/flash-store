package store

import (
	"fmt"
	"sync"
	"time"

	storeerrors "github.com/amoylan/flash-store/store/errors"
	"github.com/amoylan/flash-store/store/index"
	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/schema"
	"github.com/amoylan/flash-store/store/snapshot"
	"github.com/amoylan/flash-store/store/wal"
)

var numShards = 64

func getShardId(identity uint64) int {
	return int(identity % uint64(numShards))
}

// object 是 shard 内存里一条对象记录。Row 形态由 registry.Def[T].Extract 产出、
// 从 WAL 解码、或从 snapshot 解码——三处来源一致，都是 schema.Row 的直接存储。
type object struct {
	identity uint64
	kind     registry.Kind
	row      schema.Row
}

// objectSlice 是 queryObjects 的中间结果：kind 统一、多个 Row 按 key 顺序。
type objectSlice struct {
	kind registry.Kind
	rows []schema.Row
	keys []string
}

type kValue struct {
	identity  uint64
	value     []byte
	expiredAt int64 // Unix 秒，0 表示不过期
}

type table struct {
	kind    registry.Kind
	objects map[string]object

	// indexes key 为索引字段名。table 已按 kind 隔离，无需 kind 前缀。
	indexes          map[schema.FieldName]*index.IndexTree
	objectKeyExpires map[string]int64
}

func newTable(kind registry.Kind) *table {
	return &table{
		kind:             kind,
		objects:          make(map[string]object),
		indexes:          make(map[schema.FieldName]*index.IndexTree),
		objectKeyExpires: make(map[string]int64),
	}
}

func (t *table) ensureIndex(field schema.FieldName) *index.IndexTree {
	if tr, ok := t.indexes[field]; ok {
		return tr
	}
	tr := index.NewIndexTree(8)
	t.indexes[field] = tr
	return tr
}

// removeRowFromIndexes 按 TypedDef 声明的 indexed 字段从索引里移除 objKey。
// def 可能为 nil（noBusinessDef 模式 / kind 未注册 Def）——此时跳过索引处理。
//
// 行为对齐 Def.Fill：若 row 里没带某个 indexed 字段的 Num（历史 writer schema
// 不包含 / 老 Row）或存的是 FieldTypeUndefined，则使用 spec.Default 参与索引。
// 这样 v2 schema 新增的带 default 的 indexed 字段能正确覆盖历史 v1 Row。
func (t *table) removeRowFromIndexes(def registry.TypedDef, row schema.Row, objKey string) {
	if def == nil {
		return
	}
	for _, spec := range def.IndexedFields() {
		fv := indexedFieldValue(row, spec)
		if !fv.Type().IsIndexable() {
			continue
		}
		if tr, ok := t.indexes[spec.Name]; ok {
			tr.Delete(fv, objKey)
		}
	}
}

func (t *table) addRowToIndexes(def registry.TypedDef, row schema.Row, objKey string) {
	if def == nil {
		return
	}
	for _, spec := range def.IndexedFields() {
		fv := indexedFieldValue(row, spec)
		if !fv.Type().IsIndexable() {
			continue
		}
		t.ensureIndex(spec.Name).Insert(fv, objKey)
	}
}

// updateRowInIndexes 处理 OpSet 路径下 row 的索引更新：对每个 indexed 字段单遍迭代，
// 用 FieldValue.Equal 判断"该字段实际变了吗"——没变直接跳过，避免 tree.Delete + tree.Insert
// 这两次 btree 走（典型情况：业务 Tx 改的是非 indexed 字段，所有 indexed 字段都不变）。
//
// 与 removeRowFromIndexes + addRowToIndexes 串行调用相比，单遍 + Equal short-circuit
// 在常见 OpSet（改非 indexed 字段）上是纯赚——索引完全不动。
// 真正变化的字段仍走"删旧插新"的两次 btree 走（btree-go 没有 atomic update API），
// 收益主要靠"全集合相等 → 全跳过"的快速路径。
func (t *table) updateRowInIndexes(def registry.TypedDef, oldRow, newRow schema.Row, objKey string) {
	if def == nil {
		return
	}
	for _, spec := range def.IndexedFields() {
		oldFv := indexedFieldValue(oldRow, spec)
		newFv := indexedFieldValue(newRow, spec)
		if oldFv.Equal(newFv) {
			continue
		}
		if oldFv.Type().IsIndexable() {
			if tr, ok := t.indexes[spec.Name]; ok {
				tr.Delete(oldFv, objKey)
			}
		}
		if newFv.Type().IsIndexable() {
			t.ensureIndex(spec.Name).Insert(newFv, objKey)
		}
	}
}

// indexedFieldValue 按 Num 在 row 里查 spec 对应字段的值；未命中（writer schema
// 没带这个 Num）或显式 FieldTypeUndefined 时回退到 spec.Default。
//
// 关键 1：add/remove 对同一条 row 必须得出相同的 FieldValue，否则删不掉或留脏值。
// 用 spec.Default 是幂等来源——schema 定了之后就不变，add 时和后来的 remove
// 看到的是同一个默认值，索引增删对称。
//
// 关键 2：schema drift 防御。如果 row 里存的 FieldType 和当前 schema 声明的
// 不同（比如代码里把字段从 int32 改成了 string，restart 之后 restoreFromSnapshot
// 或 WAL replay 拿到老 Row 去 add 索引），混合类型进同一棵 B-tree 会污染比较器
// → 索引错乱或 panic-in-tree。这里提前 panic 比让腐败蔓延到业务查询路径好得多。
// 触发场景永远是代码改错（所以 panic 而非 return error）。
func indexedFieldValue(row schema.Row, spec schema.FieldDescriptor) schema.FieldValue {
	fv, ok := row.GetByNum(spec.Num)
	if !ok || fv.Type() == schema.FieldTypeUndefined {
		return spec.Default
	}
	if fv.Type() != spec.Type {
		panic(fmt.Sprintf(
			"schema drift on index: field %q (num=%d) stored as FieldType %v, schema declares %v. "+
				"Did you change a field's type in code without proper migration?",
			spec.Name, spec.Num, fv.Type(), spec.Type))
	}
	return fv
}

// apply 处理一条 cmd。noBusinessDef=true（SnapshotSlave）时跳过 Def 查找——
// shard 仍然存完整的 schema.Row 结构体（Row 自描述），只是不建索引、不做
// Def-based 类型校验。
func (t *table) apply(shardID int, identity uint64, cmd wal.Cmd, now time.Time, noBusinessDef bool) ([]ttlEvictionKey, error) {
	var def registry.TypedDef
	if !noBusinessDef {
		d, ok := registry.GetTypedDef(t.kind)
		if !ok {
			return nil, storeerrors.ErrKindNotRegistered
		}
		def = d
	}
	var jobs []ttlEvictionKey
	switch cmd.Op {
	case wal.OpSet:
		old, hadOld := t.objects[cmd.Key]
		t.objects[cmd.Key] = object{
			identity: identity,
			kind:     cmd.Kind,
			row:      cmd.Row,
		}
		if cmd.Ttl > 0 {
			exp := now.Add(cmd.Ttl).Unix()
			t.objectKeyExpires[cmd.Key] = exp
			jobs = append(jobs, ttlEvictionKey{
				shardID:  shardID,
				isKV:     false,
				kind:     t.kind,
				key:      cmd.Key,
				expireAt: exp,
			})
		} else {
			delete(t.objectKeyExpires, cmd.Key)
		}
		if hadOld {
			t.updateRowInIndexes(def, old.row, cmd.Row, cmd.Key)
		} else {
			t.addRowToIndexes(def, cmd.Row, cmd.Key)
		}
	case wal.OpDelete:
		if old, ok := t.objects[cmd.Key]; ok {
			t.removeRowFromIndexes(def, old.row, cmd.Key)
			delete(t.objects, cmd.Key)
			delete(t.objectKeyExpires, cmd.Key)
		}
	}
	return jobs, nil
}

func (t *table) getObject(key string) (object, bool) {
	e, ok := t.objects[key]
	if !ok {
		return object{}, false
	}
	if exp, has := t.objectKeyExpires[key]; has && exp > 0 {
		if time.Now().Unix() > exp {
			return object{}, false
		}
	}
	return e, true
}

func (t *table) queryKeys(def registry.TypedDef, hints []indexHint) ([]string, error) {
	if len(hints) == 0 {
		keys := make([]string, 0, len(t.objects))
		for k := range t.objects {
			keys = append(keys, k)
		}
		return keys, nil
	}
	fields := def.Fields()
	var sets []index.IndexValueSet
	for _, h := range hints {
		idxTree, ok := t.indexes[h.IndexName]
		if !ok {
			return nil, nil
		}
		// 找到字段类型（不在 schema 里就跳过）
		var ft schema.FieldType
		for _, f := range fields {
			if f.Name == h.IndexName {
				ft = f.Type
				break
			}
		}
		if ft == schema.FieldTypeUndefined {
			continue
		}
		validated, err := h.Ranges.Validate(ft)
		if err != nil {
			return nil, err
		}
		set, err := idxTree.Lookup(validated)
		if err != nil {
			return nil, err
		}
		sets = append(sets, set)
	}
	if len(sets) == 0 {
		return nil, nil
	}
	out := sets[0]
	for i := 1; i < len(sets); i++ {
		out = out.Intersect(sets[i])
	}
	keys := make([]string, 0, len(out))
	for k := range out {
		keys = append(keys, k)
	}
	return keys, nil
}

type shardId = int

type shard struct {
	id shardId
	mu sync.RWMutex
	// noBusinessDef=true：本节点没有业务 Def 注册（SnapshotSlave 不 import 业务包）。
	// 影响两处：
	//   1. apply 路径跳过 registry.GetTypedDef 查找——找不到 Def 不算错
	//   2. 索引增删跳过——没 Def 就没 IndexedFields，无法构造索引键
	//
	// 注意：即使 noBusinessDef=true，shard 仍然存完整解码的 schema.Row 结构体
	// （Row 自描述，不依赖业务 codec）；snapshot 导出时 Re-encode 为 bytes。
	// 这个标志**不是** "raw bytes passthrough"——那种模式已经不存在了。
	noBusinessDef bool

	tables map[registry.Kind]*table
	kv     map[string]kValue
}

func newShard(id shardId) *shard {
	return &shard{
		id:     id,
		tables: make(map[registry.Kind]*table),
		kv:     make(map[string]kValue),
	}
}

func (s *shard) apply(batch []identifiedCmds) ([]ttlEvictionKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	// 一条 batch 最少派生 0 个 TTL job（无 TTL 设置），最多每条 cmd 一个；
	// len(batch) 作为容量提示对常见短 batch 已足够，更大的会走 append 自然扩容。
	jobs := make([]ttlEvictionKey, 0, len(batch))

	for _, ic := range batch {
		for _, kvCmd := range ic.kvCmds {
			switch kvCmd.Op {
			case wal.OpSet:
				var exp int64
				if kvCmd.Ttl > 0 {
					exp = now.Add(kvCmd.Ttl).Unix()
					jobs = append(jobs, ttlEvictionKey{
						shardID:  s.id,
						isKV:     true,
						key:      kvCmd.Key,
						expireAt: exp,
					})
				}
				// kvCmd.Value 上游已是 owned 副本：Master 路径在 tx.setKV 入口 deep-copy；
				// Slave 路径每次 WAL decode 重新分配。kvCmd 不会被 caller 持有/复用，
				// 此处直接引用原 slice，省掉一次 hot-path memcpy。
				s.kv[kvCmd.Key] = kValue{
					identity:  ic.identity,
					value:     kvCmd.Value,
					expiredAt: exp,
				}
			case wal.OpDelete:
				delete(s.kv, kvCmd.Key)
			}
		}

		for _, cmd := range ic.cmds {
			kind := cmd.Kind
			tbl, exists := s.tables[kind]
			if !exists {
				tbl = newTable(kind)
				s.tables[kind] = tbl
			}
			cmdJobs, err := tbl.apply(s.id, ic.identity, cmd, now, s.noBusinessDef)
			if err != nil {
				return nil, err
			}
			jobs = append(jobs, cmdJobs...)
		}
	}
	return jobs, nil
}

func (s *shard) GetObject(kind registry.Kind, key string) (object, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tbl, ok := s.tables[kind]
	if !ok {
		return object{}, false
	}
	return tbl.getObject(key)
}

func (s *shard) GetObjects(kind registry.Kind, keys []string) ([]object, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	tbl, ok := s.tables[kind]
	if !ok {
		return nil, storeerrors.ErrKeyNotFound
	}
	out := make([]object, 0, len(keys))
	for _, key := range keys {
		o, ok := tbl.getObject(key)
		if !ok {
			return nil, storeerrors.ErrKeyNotFound
		}
		if o.kind != kind {
			return nil, storeerrors.ErrTypeNotMatch
		}
		out = append(out, o)
	}
	return out, nil
}

func (s *shard) GetKV(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.kv[key]
	if !ok {
		return nil, false
	}
	if v.expiredAt > 0 && time.Now().Unix() > v.expiredAt {
		return nil, false
	}
	return v.value, true
}

func (s *shard) CheckKVExists(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.kv[key]
	if !ok {
		return false
	}
	if v.expiredAt > 0 && time.Now().Unix() > v.expiredAt {
		return false
	}
	return true
}

// queryObjects 按 hints 筛 key，再对每个 object 调用 filter。
// filter 收到一个直接挂在 Row 上的视图（rowView），避免重建业务 struct。
func (s *shard) queryObjects(kind registry.Kind, hints []indexHint, filter filterFunc) (objectSlice, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tbl, ok := s.tables[kind]
	if !ok {
		return objectSlice{}, nil
	}
	def, ok := registry.GetTypedDef(kind)
	if !ok {
		return objectSlice{}, storeerrors.ErrKindNotRegistered
	}
	keys, err := tbl.queryKeys(def, hints)
	if err != nil {
		return objectSlice{}, err
	}
	var rows []schema.Row
	var outKeys []string
	for _, key := range keys {
		o, ok := tbl.getObject(key)
		if !ok {
			continue
		}
		if o.kind != kind {
			continue
		}
		if filter != nil && !filter(def, o.row) {
			continue
		}
		rows = append(rows, o.row)
		outKeys = append(outKeys, key)
	}
	return objectSlice{kind: kind, rows: rows, keys: outKeys}, nil
}

// ======================== Snapshot 导出/恢复 ========================

// snapshotData 导出 shard 内所有数据为 ShardSnapshot（自行加 RLock）。
// Row → bytes 走 store 自管的 schema.EncodeRow，不再依赖业务 Codec。
//
// 任何一条 row 的 EncodeRow 失败都返回 error 让 CreateSnapshot 整体 abort，
// 而不是 continue 跳过——静默丢弃 row 会让该 key 在下次 restore 后永久消失，
// 比让 snapshot 失败重试糟糕得多。
func (s *shard) snapshotData() (snapshot.ShardSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	users := make(map[snapshot.Identity]snapshot.UserData)

	for _, tbl := range s.tables {
		for key, obj := range tbl.objects {
			rowBytes, err := schema.EncodeRow(obj.row)
			if err != nil {
				return snapshot.ShardSnapshot{}, fmt.Errorf("encode row shard=%d kind=%v key=%q: %w", s.id, obj.kind, key, err)
			}
			entry := snapshot.ObjectEntry{
				Kind:      obj.kind,
				Key:       key,
				Row:       rowBytes,
				ExpiredAt: tbl.objectKeyExpires[key],
			}
			ud := users[obj.identity]
			ud.Objects = append(ud.Objects, entry)
			users[obj.identity] = ud
		}
	}

	for key, kv := range s.kv {
		entry := snapshot.KVEntry{
			Key:       key,
			Value:     append([]byte(nil), kv.value...),
			ExpiredAt: kv.expiredAt,
		}
		ud := users[kv.identity]
		ud.KVs = append(ud.KVs, entry)
		users[kv.identity] = ud
	}

	return snapshot.ShardSnapshot{
		ShardID: s.id,
		Users:   users,
	}, nil
}

// restoreFromSnapshot 从 ShardSnapshot 恢复数据到 shard 内存（自行加 Lock），返回需要注册的 TTL 任务。
func (s *shard) restoreFromSnapshot(snap *snapshot.ShardSnapshot) ([]ttlEvictionKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	var ttlJobs []ttlEvictionKey

	for identity, userData := range snap.Users {
		for _, entry := range userData.Objects {
			if entry.ExpiredAt > 0 && now.Unix() > entry.ExpiredAt {
				continue
			}

			kind := entry.Kind
			tbl, exists := s.tables[kind]
			if !exists {
				tbl = newTable(kind)
				s.tables[kind] = tbl
			}

			row, err := schema.DecodeRow(entry.Row)
			if err != nil {
				return nil, fmt.Errorf("decode row key=%s: %w", entry.Key, err)
			}
			tbl.objects[entry.Key] = object{
				identity: identity,
				kind:     kind,
				row:      row,
			}

			if !s.noBusinessDef {
				if def, ok := registry.GetTypedDef(kind); ok {
					tbl.addRowToIndexes(def, row, entry.Key)
				}
			}

			if entry.ExpiredAt > 0 {
				tbl.objectKeyExpires[entry.Key] = entry.ExpiredAt
				ttlJobs = append(ttlJobs, ttlEvictionKey{
					shardID:  s.id,
					isKV:     false,
					kind:     kind,
					key:      entry.Key,
					expireAt: entry.ExpiredAt,
				})
			}
		}

		for _, entry := range userData.KVs {
			if entry.ExpiredAt > 0 && now.Unix() > entry.ExpiredAt {
				continue
			}

			s.kv[entry.Key] = kValue{
				identity:  identity,
				value:     append([]byte(nil), entry.Value...),
				expiredAt: entry.ExpiredAt,
			}

			if entry.ExpiredAt > 0 {
				ttlJobs = append(ttlJobs, ttlEvictionKey{
					shardID:  s.id,
					isKV:     true,
					key:      entry.Key,
					expireAt: entry.ExpiredAt,
				})
			}
		}
	}

	return ttlJobs, nil
}
