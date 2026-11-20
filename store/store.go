/*
Package  store implements the core storage functionalities,
including sharding, object management, indexing, and transactions.

The Store struct manages multiple shards, each responsible for a subset of the data.
It provides methods to get shards, retrieve objects, query objects with filters,
and handle key-value pairs. The Store also supports transactions through the Tx struct.

Store 自身不提供公共方法，api.go中定义了package级别的公共方法供外部调用。
主要原因是Store内部存储了多种对象，但是go对象的泛型支持有限，未定义泛型的结构体无法提供泛型方法。

关于API，写方法统一通过 TxEvent 来进行操作
读方法按功能可以分成两类，一类是直接获取对象或KV，另一类是获取View（视图）
  - 读对象时，会对对象进行Clone，避免调用方修改存储中的对象
  - 读View时，通过转换函数将对象转换成View返回给调用方（View的具体实现由业务方定义）
*/
package store

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"

	storeconfig "github.com/amoylan/flash-store/store/config"
	storeerrors "github.com/amoylan/flash-store/store/errors"
	"github.com/amoylan/flash-store/store/log"
	"github.com/amoylan/flash-store/store/metrics"
	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/schema"
	"github.com/amoylan/flash-store/store/utils"
	"github.com/amoylan/flash-store/store/wal"

	_ "github.com/amoylan/flash-store/store/snapshot/source/file"
	_ "github.com/amoylan/flash-store/store/snapshot/source/noop"
	_ "github.com/amoylan/flash-store/store/snapshot/source/s3"
	_ "github.com/amoylan/flash-store/store/wal/source/file"
	_ "github.com/amoylan/flash-store/store/wal/source/kafka"
	_ "github.com/amoylan/flash-store/store/wal/source/noop"
)

type Store struct {
	config storeconfig.Config

	// walSentListeners 由 AddWalSentListener 注册，在 worker.Commit 中遍历触发 OnWalSent。
	//
	// 并发模型（freeze-before-read 契约）：
	//   - 写：AddWalSentListener 在 freeze 前持 listenersMu 追加。
	//   - freeze：freezeListeners 由 startReplay 调用，置 listenersFrozen=true 后 slice 永久不再变更。
	//     之后 AddWalSentListener 返回 ErrStoreAlreadyStarted。
	//   - 读：worker.Commit 在 freeze 之后才被触发——业务 Tx.Commit 路径只在 StartReplay
	//     返回之后开始；调用方从 setup 到业务 Commit 之间必然存在 happens-before 边
	//     （sequenced-before / goroutine spawn / channel 等用户层 sync），足以让 reader
	//     看到全部 Add 的写入。slice 在 freeze 之后是不可变对象，读路径无锁、无 atomic。
	listenersMu      sync.Mutex        // 串行化 AddWalSentListener 和 freeze 动作
	listenersFrozen  bool              // 受 listenersMu 保护
	walSentListeners []WalSentListener // 写在 listenersMu 内；freeze 之后只读，无须 atomic

	// shards 存储所有分片
	shards []*shard

	// workers 存储所有 worker
	workers []*worker

	// ttlWheel 在本地按过期时间触发 object/KV 清理（不写 WAL；主从各自根据 WAL 中的 Ttl 各自注册）。
	// 写入只发生在 NewStore 的单线程初始化阶段；之后 scheduleTTLJobs 持续读、Close 调一次 Stop——
	// closeOnce 保证 Stop 只执行一次，且 Close 的契约是"所有业务写入已停止"，与 scheduleTTLJobs 无并发，
	// 因此无需 atomic.Pointer 保护。
	ttlWheel *utils.TimeWheel[ttlEvictionKey]

	closeOnce sync.Once

	// replayApplyToStoreStarted：ApplyToStore==true 时 StartReplay 是否已成功启动过（全部分区协程已派发）；防止重复回放写内存。
	replayApplyToStoreStarted atomic.Bool

	// replayMu 保护 replay 生命周期（start/stop/snapshot 互斥）。
	replayMu sync.Mutex
	// replayCancel 用于停止当前正在运行的 replay goroutine；nil 表示 replay 未运行。
	replayCancel context.CancelFunc
	// replayDone 在所有 replay goroutine 退出后关闭。
	replayDone chan struct{}
	// replayErr 记录任一 worker goroutine 异常退出（非 ctx 取消）的首个 error。
	// 由 startReplay 在每次启动前清空；被 CreateSnapshot / WaitReplay 检查，
	// 区分"健康追到末尾 / ctx 正常停"与"中途崩溃后 partition 状态停滞"。
	// 读多写少 + 仅记录"是否曾发生过失败"，用 atomic.Pointer + CAS 保留首个 err。
	replayErr atomic.Pointer[error]

	// lastSnapshotWalOffsets 记录上一次 snapshot 的各 worker WalOffset。
	// 初始来源：restoreFromSnapshot 时从 snapshot 中读取。
	// 每次 CreateSnapshot 成功后更新为本次 snapshot 的各 worker WalOffset。
	// 用途：可观测性（flashstore_last_snapshot_wal_offset 指标）。
	lastSnapshotWalOffsets map[int]int64

	// schemasByKind 是每个 Kind 的 alive + tombstone 字段集合。
	//
	// Fill 热路径**不读**此缓存——Fill 只依赖 reader Def 的 byNum + Row 自带的
	// typeTag。此缓存的用途只有两处：
	//   1. Compat 检查（防同 Num 改 Type/Default、防跨版本 Num 复用）
	//   2. Snapshot 持久化（Slave 不注册业务 struct，得有地方知道 schema 长啥样）
	//
	// 每个 Kind 一个 fieldsRegistry：
	//   - alive：当前 latest 版本里存在的字段
	//   - tombstone：曾经存在、现已删除的字段（Num 永久占用，防复用）
	//
	// 填充来源：
	//   1. restoreFromSnapshot：header.Schemas 逐条 UpsertSchema
	//   2. replay WalTypeSchemaConfig：收到 master 广播的 latest，UpsertSchema
	//      合并（alive 里没有但 broadcast 里也没有的 → 移到 tombstone；新字段加入 alive）
	//   3. Master PublishSchemas：自己也 UpsertSchema（写完 WAL 后把自己同步到广播内容）
	schemasByKindMu sync.RWMutex
	schemasByKind   map[registry.Kind]*fieldsRegistry

	// metrics 由 NewStore 构造一次，全生命周期复用。
	// Config.Registerer 为 nil 时仍非 nil（内部用 throwaway registry），调用点无需判空。
	metrics *metrics.Metrics
}

// fieldsRegistry / schema 相关方法见 schema_manager.go。

// NewStore 构造一个 Store。
//
// reg 是 Prometheus 指标注册器，是 NewStore 的独立参数而不是 Config 字段——Config 内的字段
// 应当全部能由 Apollo 之类的配置中心下发，Registerer 是运行期对象不在此列。
// 传 nil 时内部用 throwaway registry，等价于关闭外部可见的指标，热路径无判空开销。
//
// 同进程多 Store 共享同一 Registerer 时务必先用 prometheus.WrapRegistererWith 注入
// 区分用的 const label（如 service / role），否则会因指标重复注册报错。
func NewStore(
	config storeconfig.Config,
	reg prometheus.Registerer,
) (*Store, error) {
	store := Store{
		config:        config,
		shards:        make([]*shard, numShards),
		workers:       make([]*worker, numWorkers),
		schemasByKind: make(map[registry.Kind]*fieldsRegistry),
		metrics:       metrics.New(reg),
	}
	// schemasByKind 留空 —— 三种来源按实际发生顺序填：
	//   a) restoreFromSnapshot: 从 header.Schemas 加载历史 schema
	//   b) replay: WalTypeSchemaConfig 覆盖本地缓存（全跳独立 compat 校验）
	//   c) afterReplayStarted (master): 先 compat 校验当前代码 vs 缓存，再 PublishSchemas
	//
	// 之前在 NewStore 就从 registry.AllDescriptors() 初始化的做法属于多余——
	// 很快被上述步骤覆盖，而且会让"先 seed 再 compat"的顺序变得混乱。

	for i := range numWorkers {
		worker, err := newWorker(&store, i, config.Wal)
		if err != nil {
			return nil, err
		}
		store.workers[i] = worker
	}

	for i := range numShards {
		sh := newShard(i)
		sh.noBusinessDef = store.config.Role == storeconfig.RoleSnapshotSlave
		store.shards[i] = sh
	}

	store.initTTLWheel()

	// state gauges 必须在 workers / shards 构造完之后再注册，回调访问 store 内部状态。
	// 注册失败仅打 warning——指标体系应当是降级可观测，不应阻塞 Store 启动。
	if err := store.metrics.RegisterStateGauges(storeStateProvider{s: &store}); err != nil {
		log.Warn("metrics: register state gauges failed",
			log.NamedError("err", err))
	}

	return &store, nil
}

// getWorker 越界安全的 worker 访问，返回 nil 表示越界——给 metrics scrape 等
// 不希望 panic 的 read 路径用。业务路径上不应触发越界（worker id 由内部生成）。
func (s *Store) getWorker(id int) *worker {
	if id < 0 || id >= len(s.workers) {
		return nil
	}
	return s.workers[id]
}

// AddWalSentListener 注册一个 WalSentListener。必须在 StartReplay/RestoreAndStartReplay
// 之前调用——一旦 Store 进入 started 状态，此方法返回 ErrStoreAlreadyStarted。
//
// 同一时刻可以并发调用（mutex 串行化），但所有注册必须 happens-before Start，否则注册的
// listener 可能不会收到回调。Listener 注册是 setup-time 行为，应在业务写入开始前完成。
func (s *Store) AddWalSentListener(listener WalSentListener) error {
	s.listenersMu.Lock()
	defer s.listenersMu.Unlock()
	if s.listenersFrozen {
		return storeerrors.ErrStoreAlreadyStarted
	}
	s.walSentListeners = append(s.walSentListeners, listener)
	return nil
}

// freezeListeners 在第一次 startReplay 时调用，禁止后续 AddWalSentListener。
// 多次调用安全。
func (s *Store) freezeListeners() {
	s.listenersMu.Lock()
	s.listenersFrozen = true
	s.listenersMu.Unlock()
}

// Close 优雅停机相关资源：停止 ackBatchers、关闭每个 worker 的 walWriter、停止 TTL 时间轮。
// 可安全多次调用（closeOnce 守护）。
//
// 调用契约：调用方需先停止业务写入（不再调 Tx.Commit）、停止 replay（StopReplay）。
// Close 内部按 "停止写入源 → 关闭 writer → 停止后台" 的顺序逐步释放：
//
//  1. worker.Close：关闭 walWriter（释放 kafka/file 连接、batch goroutine）
//  3. ttlWheel.Stop：停止时间轮 goroutine
//
// 注意：replayer.reader 由 replay goroutine 自己 defer 关闭——StopReplay 让 replay 退出
// 时同时释放 reader，无需 Store.Close 介入。
func (s *Store) Close() {
	s.closeOnce.Do(func() {
		// 1. 关闭 walWriter（每个 worker 独立持有）
		for _, w := range s.workers {
			if err := w.Close(); err != nil {
				log.Warn("worker.Close failed",
					log.Int("worker_id", w.id),
					log.NamedError("err", err))
			}
		}
		// 3. 停止 TTL 时间轮——closeOnce 保证只走一次，无须 atomic 防双 Stop
		if s.ttlWheel != nil {
			s.ttlWheel.Stop()
		}
	})
}

func (s *Store) getShard(identity uint64) *shard {
	id := getShardId(identity)
	if id < 0 || id >= len(s.shards) {
		return nil
	}
	return s.shards[id]
}

func (s *Store) getObject(identity uint64, kind registry.Kind, key string) (object, error) {
	sh := s.getShard(identity)
	if sh == nil {
		return object{}, storeerrors.ErrKeyNotFound
	}
	o, ok := sh.GetObject(kind, key)
	if !ok {
		return object{}, storeerrors.ErrKeyNotFound
	}
	if o.kind != kind {
		return object{}, storeerrors.ErrTypeNotMatch
	}
	return o, nil
}

func (s *Store) getObjects(identity uint64, kind registry.Kind, keys []string) (objectSlice, error) {
	if len(keys) == 0 {
		return objectSlice{}, nil
	}
	sh := s.getShard(identity)
	if sh == nil {
		return objectSlice{}, storeerrors.ErrKeyNotFound
	}
	objs, err := sh.GetObjects(kind, keys)
	if err != nil {
		return objectSlice{}, err
	}
	rows := make([]schema.Row, len(objs))
	outKeys := make([]string, len(objs))
	for i := range objs {
		rows[i] = objs[i].row
		outKeys[i] = keys[i]
	}
	return objectSlice{kind: kind, rows: rows, keys: outKeys}, nil
}

// identifiedCmds 将 identity 与该 identity 下的 cmds/kvCmds 绑定，传递到 shard 以支持按 identity 索引存储。
type identifiedCmds struct {
	identity uint64
	cmds     []wal.Cmd
	kvCmds   []wal.KvCmd
}

func (s *Store) applyEvents(events []wal.Event) error {
	// Fast path：单 event 时跳过 by-shard map 分配——
	// 单 event 是绝大多数 Tx 的形态（一个 user 一次写）。
	// 这里直接构造一条 identifiedCmds 即可，shard.apply 接收 batch 顺序遍历，
	// 单元素 batch 与多元素同等对待。
	if len(events) == 1 {
		ev := events[0]
		sid := getShardId(ev.Identity)
		return s.applyShard(sid, []identifiedCmds{{
			identity: ev.Identity,
			cmds:     ev.Cmds,
			kvCmds:   ev.KvCmds,
		}})
	}

	by := make(map[int][]identifiedCmds)
	for _, event := range events {
		sid := getShardId(event.Identity)
		by[sid] = append(by[sid], identifiedCmds{
			identity: event.Identity,
			cmds:     event.Cmds,
			kvCmds:   event.KvCmds,
		})
	}
	for sid, batch := range by {
		if err := s.applyShard(sid, batch); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) applyShard(shardId int, batch []identifiedCmds) error {
	if shardId < 0 || shardId >= len(s.shards) || s.shards[shardId] == nil {
		return storeerrors.ErrShardNotFound
	}
	jobs, err := s.shards[shardId].apply(batch)
	if err != nil {
		return err
	}
	s.scheduleTTLJobs(jobs)
	return nil
}

// filterFunc 是 queryObjects 的过滤器。参数是 TypedDef（字段布局）+ Row（存储形态）；
// 过滤不经业务 struct 重建，避免 Fill 开销。
type filterFunc func(def registry.TypedDef, row schema.Row) bool

var noMatchFilterFunc filterFunc = func(_ registry.TypedDef, _ schema.Row) bool {
	return false
}

func (s *Store) queryObjectsByIdentity(
	identity uint64,
	kind registry.Kind,
	hints []indexHint,
	filter filterFunc) (objectSlice, error) {
	sh := s.getShard(identity)
	if sh == nil {
		return objectSlice{}, nil
	}
	return sh.queryObjects(kind, hints, filter)
}

func (s *Store) queryObjects(
	kind registry.Kind,
	hints []indexHint,
	filter filterFunc) (objectSlice, error) {
	merged := objectSlice{kind: kind}
	for _, sh := range s.shards {
		if sh == nil {
			continue
		}
		part, err := sh.queryObjects(kind, hints, filter)
		if err != nil {
			return objectSlice{}, err
		}
		if len(part.rows) == 0 {
			continue
		}
		merged.rows = append(merged.rows, part.rows...)
		merged.keys = append(merged.keys, part.keys...)
	}
	return merged, nil
}

func (s *Store) getKV(identity uint64, key string) ([]byte, error) {
	sh := s.getShard(identity)
	if sh == nil {
		return nil, storeerrors.ErrKeyNotFound
	}
	v, ok := sh.GetKV(key)
	if !ok {
		return nil, storeerrors.ErrKeyNotFound
	}
	return append([]byte(nil), v...), nil
}

func (s *Store) checkKVExists(identity uint64, key string) bool {
	sh := s.getShard(identity)
	if sh == nil {
		return false
	}
	return sh.CheckKVExists(key)
}

// BeginTx 开启一个事务，ctx 会被携带到整个事务生命周期，包括 Commit 阶段的 WAL 写入。
//
// ctx 取消行为：
//   - 消息进入 Kafka 发送队列之前：ctx 取消会立即中止，不产生副作用。
//   - 消息已进入发送队列之后：ctx 取消不会中止本次 Kafka 写入（必须等到明确的成功/失败结果），
//     但会终止后续的重试循环，使 Commit 尽快返回 ctx.Err()。
//
// 调用方通常不需要在外部对 Commit 做重试：
//
//	Store 内部已实现指数退避重试（100ms → 30s），同一个 seq 会被复用，
//	配合接收端的 SeqDeduplicator 保证幂等。只有以下两种情况需要调用方介入：
//	- ctx 被主动取消（如优雅关闭、请求超时）：此时业务层应决定是否用新事务重新提交。
//	- seq 落后过多（超过 DefaultMaxSeqLag）：说明 Kafka 长时间不可用，
//	  内部重试已放弃，业务层应进入降级或告警流程。
func (s *Store) BeginTx(ctx context.Context) *Tx {
	return &Tx{
		ctx:   ctx,
		store: s,
	}
}

func (s *Store) Transaction(ctx context.Context, fn func(tx *Tx) error) (err error) {
	tx := s.BeginTx(ctx)
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	err = fn(tx)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// workerWalOffsets 返回各 worker/partition 的 maxWalOffset。
// 内部用途：restoreFromSnapshot 后由 RestoreAndStartReplay 构造 replay 起点（WalOffset+1）。
// 返回的 map key 为 partition ID，value 为该分区在 snapshot 中记录的最后 WAL offset（inclusive）；-1 表示无有效值。
func (s *Store) workerWalOffsets() map[int]wal.Offset {
	offsets := make(map[int]wal.Offset, len(s.workers))
	for _, w := range s.workers {
		offsets[w.id] = wal.Offset(w.LoadMaxWalOffset())
	}
	return offsets
}
