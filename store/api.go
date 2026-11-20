package store

import (
	"fmt"
	"time"

	"github.com/amoylan/flash-store/store/errors"
	"github.com/amoylan/flash-store/store/index"
	"github.com/amoylan/flash-store/store/metrics"
	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/schema"
)

// observeRead 在 Read API defer 中调用，统一记录耗时 / 结果 / 返回行数。
//   - resultSize < 0 表示"单对象返回"——跳过返回行数 histogram。
//   - 仅当 reader 是 *Store 时记录；测试用 mock reader 自然走过去无副作用。
func observeRead(s IStoreReader, op metrics.ReadOp, start time.Time, err error, resultSize int) {
	st, ok := s.(*Store)
	if !ok {
		return
	}
	st.metrics.ObserveRead(op, time.Since(start), err, resultSize)
}

// =========================== Write API ==============================
// 写操作仅通过 TxEvent。SetObject 调用 Def[T].Extract 得到 Row，
// 一并写进 WAL；从业务 *T → Row 的路径是唯一真相源，不再经过业务 Codec。

// SetObject 写入对象。kind 必须已通过 registry.RegisterDef 注册。
func SetObject[T any](
	tx *TxEvent,
	identity uint64,
	kind registry.Kind,
	key string,
	obj *T,
	ttl time.Duration,
) error {
	return setObject[T](tx, identity, kind, key, obj, ttl)
}

// DeleteObject 删除对象；若 key 不存在则幂等成功。
func DeleteObject(
	tx *TxEvent,
	identity uint64,
	kind registry.Kind,
	key string,
) error {
	return tx.deleteObject(identity, kind, key)
}

// SetKV 写入分片内 KV（与 object 共用 identity 选 shard）。
func SetKV(
	tx *TxEvent,
	identity uint64,
	key string,
	value []byte,
	ttl time.Duration,
) error {
	return tx.setKV(identity, key, value, ttl)
}

// DeleteKV 删除 KV。
func DeleteKV(
	tx *TxEvent,
	identity uint64,
	key string,
) error {
	return tx.deleteKV(identity, key)
}

// =========================== Read API ==============================

type indexHint struct {
	IndexName schema.FieldName
	Ranges    index.Ranges
}

// IStoreReader 存储读取接口
type IStoreReader interface {
	getObject(identity uint64, kind registry.Kind, key string) (object, error)
	getObjects(identity uint64, kind registry.Kind, keys []string) (objectSlice, error)
	queryObjectsByIdentity(identity uint64, kind registry.Kind, hints []indexHint, filter filterFunc) (objectSlice, error)
	queryObjects(kind registry.Kind, hints []indexHint, filter filterFunc) (objectSlice, error)
	getKV(identity uint64, key string) ([]byte, error)
	checkKVExists(identity uint64, key string) bool
}

// GetObject 从 store 读对象，用 Def[T].Fill 把 Row 回填到新建的 *T。
// 天然零共享：Row 是存储侧独立副本，Fill 每次产出新 *T，业务可随意修改。
func GetObject[T any](
	s IStoreReader,
	identity uint64,
	kind registry.Kind,
	key string) (out *T, err error) {
	start := time.Now()
	defer func() { observeRead(s, metrics.ReadOpGetObject, start, err, -1) }()

	def, ok := registry.GetDef[T](kind)
	if !ok {
		return nil, errors.ErrKindNotRegistered
	}
	o, err := s.getObject(identity, kind, key)
	if err != nil {
		return nil, err
	}
	out = new(T)
	if err := def.Fill(o.row, out); err != nil {
		return nil, fmt.Errorf("fill %s/%s: %w", kind, key, err)
	}
	return out, nil
}

// GetObjects 批量版本的 GetObject。
func GetObjects[T any](
	s IStoreReader,
	identity uint64,
	kind registry.Kind,
	keys []string) (out []*T, err error) {
	start := time.Now()
	defer func() { observeRead(s, metrics.ReadOpGetObjects, start, err, len(out)) }()

	def, ok := registry.GetDef[T](kind)
	if !ok {
		return nil, errors.ErrKindNotRegistered
	}
	slice, err := s.getObjects(identity, kind, keys)
	if err != nil {
		return nil, err
	}
	out = make([]*T, len(slice.rows))
	for i, row := range slice.rows {
		t := new(T)
		if err := def.Fill(row, t); err != nil {
			return nil, fmt.Errorf("fill %s/%s: %w", kind, slice.keys[i], err)
		}
		out[i] = t
	}
	return out, nil
}

// GetKV / CheckKVExists —— 字节面的 API，不走 schema 层。
func GetKV(s IStoreReader, identity uint64, key string) (out []byte, err error) {
	start := time.Now()
	defer func() { observeRead(s, metrics.ReadOpGetKV, start, err, -1) }()
	return s.getKV(identity, key)
}

func CheckKVExists(s IStoreReader, identity uint64, key string) bool {
	start := time.Now()
	exists := s.checkKVExists(identity, key)
	observeRead(s, metrics.ReadOpCheckKVExists, start, nil, -1)
	return exists
}

type IndexHint struct {
	Name   schema.FieldName
	Ranges index.Ranges
}

// Filter 是读路径上的过滤器：收到 RowView，直接按字段名取值，不重建业务 struct。
// 注意不能修改 view 背后的 Row。
type Filter[T any] func(view *registry.RowView) bool

func (f Filter[T]) toFilterFunc() filterFunc {
	if f == nil {
		return nil
	}
	return func(def registry.TypedDef, row schema.Row) bool {
		return f(registry.NewRowView(def, row))
	}
}

// QueryObjectsByIdentity 查询单个用户的对象列表（按 identity 所在 shard）。
func QueryObjectsByIdentity[T any](
	s IStoreReader,
	identity uint64,
	kind registry.Kind,
	hints []IndexHint,
	filter Filter[T]) (out []*T, err error) {
	start := time.Now()
	defer func() { observeRead(s, metrics.ReadOpQueryObjectsByIdentity, start, err, len(out)) }()

	def, ok := registry.GetDef[T](kind)
	if !ok {
		return nil, errors.ErrKindNotRegistered
	}
	slice, err := s.queryObjectsByIdentity(identity, kind, toIndexHints(hints), filter.toFilterFunc())
	if err != nil {
		return nil, err
	}
	return fillAll[T](def, slice)
}

// QueryObjects 跨 shard 全局查询。
func QueryObjects[T any](
	s IStoreReader,
	kind registry.Kind,
	hints []IndexHint,
	filter Filter[T]) (out []*T, err error) {
	start := time.Now()
	defer func() { observeRead(s, metrics.ReadOpQueryObjects, start, err, len(out)) }()

	def, ok := registry.GetDef[T](kind)
	if !ok {
		return nil, errors.ErrKindNotRegistered
	}
	slice, err := s.queryObjects(kind, toIndexHints(hints), filter.toFilterFunc())
	if err != nil {
		return nil, err
	}
	return fillAll[T](def, slice)
}

// ========================= Read API（View 路径） ============================
// View 路径跳过 Fill，直接返回 RowView 让调用方按需取字段。
// 适合只看少数几个字段（撮合/归档）或上送消息场景。

// GetView 取单个对象的 RowView。
func GetView[T any](
	s IStoreReader,
	identity uint64,
	kind registry.Kind,
	key string) (view *registry.RowView, err error) {
	start := time.Now()
	defer func() { observeRead(s, metrics.ReadOpGetView, start, err, -1) }()

	def, ok := registry.GetTypedDef(kind)
	if !ok {
		return nil, errors.ErrKindNotRegistered
	}
	o, err := s.getObject(identity, kind, key)
	if err != nil {
		return nil, err
	}
	return registry.NewRowView(def, o.row), nil
}

// GetViews 批量 RowView。
func GetViews[T any](
	s IStoreReader,
	identity uint64,
	kind registry.Kind,
	keys []string) (out []*registry.RowView, err error) {
	start := time.Now()
	defer func() { observeRead(s, metrics.ReadOpGetViews, start, err, len(out)) }()

	def, ok := registry.GetTypedDef(kind)
	if !ok {
		return nil, errors.ErrKindNotRegistered
	}
	slice, err := s.getObjects(identity, kind, keys)
	if err != nil {
		return nil, err
	}
	out = make([]*registry.RowView, len(slice.rows))
	for i, row := range slice.rows {
		out[i] = registry.NewRowView(def, row)
	}
	return out, nil
}

// QueryViewsByIdentity 按 identity 查 RowView。
func QueryViewsByIdentity[T any](
	s IStoreReader,
	identity uint64,
	kind registry.Kind,
	hints []IndexHint,
	filter Filter[T]) (out []*registry.RowView, err error) {
	start := time.Now()
	defer func() { observeRead(s, metrics.ReadOpQueryViewsByIdentity, start, err, len(out)) }()

	def, ok := registry.GetTypedDef(kind)
	if !ok {
		return nil, errors.ErrKindNotRegistered
	}
	slice, err := s.queryObjectsByIdentity(identity, kind, toIndexHints(hints), filter.toFilterFunc())
	if err != nil {
		return nil, err
	}
	return viewAll(def, slice), nil
}

// QueryViews 全局查询 RowView。
func QueryViews[T any](
	s IStoreReader,
	kind registry.Kind,
	hints []IndexHint,
	filter Filter[T]) (out []*registry.RowView, err error) {
	start := time.Now()
	defer func() { observeRead(s, metrics.ReadOpQueryViews, start, err, len(out)) }()

	def, ok := registry.GetTypedDef(kind)
	if !ok {
		return nil, errors.ErrKindNotRegistered
	}
	slice, err := s.queryObjects(kind, toIndexHints(hints), filter.toFilterFunc())
	if err != nil {
		return nil, err
	}
	return viewAll(def, slice), nil
}

// ========================= helpers ============================

func toIndexHints(hints []IndexHint) []indexHint {
	out := make([]indexHint, 0, len(hints))
	for _, h := range hints {
		out = append(out, indexHint{IndexName: h.Name, Ranges: h.Ranges})
	}
	return out
}

func fillAll[T any](def *registry.Def[T], slice objectSlice) ([]*T, error) {
	out := make([]*T, len(slice.rows))
	for i, row := range slice.rows {
		t := new(T)
		if err := def.Fill(row, t); err != nil {
			return nil, fmt.Errorf("fill %s/%s: %w", slice.kind, slice.keys[i], err)
		}
		out[i] = t
	}
	return out, nil
}

func viewAll(def registry.TypedDef, slice objectSlice) []*registry.RowView {
	out := make([]*registry.RowView, len(slice.rows))
	for i, row := range slice.rows {
		out[i] = registry.NewRowView(def, row)
	}
	return out
}
