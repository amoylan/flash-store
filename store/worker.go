package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/amoylan/flash-store/store/log"
	"github.com/amoylan/flash-store/store/metrics"
	"github.com/amoylan/flash-store/store/snapshot"
	"github.com/amoylan/flash-store/store/wal"
	walconfig "github.com/amoylan/flash-store/store/wal/config"
	"github.com/amoylan/flash-store/store/wal/source"
)

// walWriterMetrics 是可选接口；实现的 wal.Writer（如 kafka source）会被自动注册到 store 的 Registerer。
//
// 实现方应当**不**自带 worker_id 之类的 const labels——store 端用
// prometheus.WrapRegistererWith(metrics.WorkerLabels(workerID), ...) 包过 registerer
// 后再传入，label 命名权统一在 metrics 包，跨子系统天然一致。
type walWriterMetrics interface {
	RegisterMetrics(reg prometheus.Registerer) error
}

var numWorkers = 16

// =========================== Worker ==============================
// worker 负责将 cmds 构建成 Wal，并发送到远端，然后将 cmds 应用到本地的 shard 中
// 一个 worker 对应一个发送 wal 的 kafka partition
// 一个 worker 可能会对应多个 shards （如果 shard 数量大于 worker 数量的话），但一个 shard 只能对应一个 worker
//
// worker 本身不再持有独立的 goroutine，Append 的批量聚合由底层 Writer 实现（如 kafka batchLoop）负责。
// Commit 调用方的 goroutine 直接驱动 activeUpdate，减少一次 channel 中转。
type worker struct {
	walConfig walconfig.Config

	id int // 该 worker 负责发送 wal 的 kafka partition

	store *Store

	// wal writer，负责将 wal 发送到远端
	walWriter wal.Writer

	// walCodec 启动期一次性查表得到，避免 appendWal 热路径反复走 GetWalCodec 的全局 map 查找。
	// Format 在 walConfig 上是不可变字段，构造之后无需再读。
	walCodec wal.WalCodec

	// seq 和 offset 都需要从snapshot中恢复
	seq          atomic.Int64 // wal 业务序列号，单调递增 相同seq表示同一批次的 cmds，保证原子性
	maxWalOffset atomic.Int64 // replay 期间已应用的最大 WAL offset（inclusive）；-1 表示未设置

	// seqDedup WAL 回放时的 seq 去重器，生命周期与 worker 一致（跨 replay 重启保持状态）。
	// 由 snapshot 恢复或首次 replay 时初始化。
	seqDedup *wal.SeqDeduplicator

	// closeOnce 确保 walWriter 只关闭一次（Store.Close 可能多次调用 Close）。
	closeOnce sync.Once

	// metrics 是本 worker 的 curried metric 句柄，由 newWorker 一次性创建。
	// 热路径无标签查找开销；类型化方法见 metrics.WorkerMetrics 注释。
	metrics *metrics.WorkerMetrics
}

func newWorker(
	store *Store,
	partition int,
	walConfig walconfig.Config,
) (*worker, error) {
	// partition / Dir 子目录的处理已经下沉到各 Source.NewWriter 实现内部
	// （kafka source 用 partition 当 partition 号；file source 用作 Dir 子目录
	// 后缀 partition-<n>；noop 忽略）。worker 这里只透传 partition，不再
	// 改 walConfig 自身。

	writerSource, err := source.GetSource(walConfig.Writer.Source)
	if err != nil {
		return nil, err
	}

	writer, err := writerSource.NewWriter(walConfig.Writer, partition)
	if err != nil {
		return nil, err
	}

	codec, ok := wal.GetWalCodec(walConfig.Writer.Format)
	if !ok {
		return nil, fmt.Errorf("worker: no codec registered for format %q", walConfig.Writer.Format)
	}

	worker := &worker{
		walConfig: walConfig,
		store:     store,
		id:        partition,
		walWriter: writer,
		walCodec:  codec,
		seqDedup:  wal.NewSeqDeduplicator(0),
		metrics:   store.metrics.ForWorker(partition),
	}
	worker.maxWalOffset.Store(-1)

	// 可选：让 wal.Writer 自行注册它的指标（kafka writer 用此 hook 注册 Stats() collector）。
	// 用 WrapRegistererWith 注入 worker_id label——子系统不需要知道 label 命名规则，
	// 拿到的 registerer 已经自带正确的 const labels。注册失败不阻塞 worker 启动；
	// 指标体系应当是降级可观测。
	if mr, ok := writer.(walWriterMetrics); ok {
		wrapped := prometheus.WrapRegistererWith(metrics.WorkerLabels(partition), store.metrics.Registerer())
		if err := mr.RegisterMetrics(wrapped); err != nil {
			log.Warn("worker: register wal writer metrics failed",
				log.Int("partition", partition),
				log.NamedError("err", err))
		}
	}

	return worker, nil
}

// ======================== Snapshot 相关 ========================

// LoadSeq 返回当前 seq 值（用于 snapshot 导出）。
// nextSeq 原子递增并返回下一个 WAL seq。
func (w *worker) nextSeq() int64 {
	return w.seq.Add(1)
}

func (w *worker) LoadSeq() int64 {
	return w.seq.Load()
}

// StoreSeq 从 snapshot 恢复 seq 值。
func (w *worker) StoreSeq(seq int64) {
	w.seq.Store(seq)
}

// advanceSeq 在 replay 时将 seq 推进到至少 newSeq 的值（只前进不后退）。
// replay 期间每个 worker 仅由单 goroutine 驱动，无需 CAS。
func (w *worker) advanceSeq(newSeq int64) {
	if cur := w.seq.Load(); newSeq > cur {
		w.seq.Store(newSeq)
	}
}

// LoadMaxWalOffset 返回当前 maxWalOffset（用于 snapshot 导出）。
func (w *worker) LoadMaxWalOffset() int64 {
	return w.maxWalOffset.Load()
}

// StoreMaxWalOffset 从 snapshot 恢复 maxWalOffset。
func (w *worker) StoreMaxWalOffset(offset int64) {
	w.maxWalOffset.Store(offset)
}

// advanceMaxWalOffset 在 replay 时将 maxWalOffset 推进到至少 offset 的值（只前进不后退）。
func (w *worker) advanceMaxWalOffset(offset int64) {
	if cur := w.maxWalOffset.Load(); offset > cur {
		w.maxWalOffset.Store(offset)
	}
}

// SnapshotSeqDedup 导出 SeqDeduplicator 状态（用于 snapshot）。
func (w *worker) SnapshotSeqDedup() snapshot.SeqDedupState {
	maxSeen, windowSize, seen := w.seqDedup.Snapshot()
	return snapshot.SeqDedupState{
		MaxSeen:    maxSeen,
		WindowSize: windowSize,
		Seen:       seen,
	}
}

// RestoreSeqDedup 从 snapshot 恢复 SeqDeduplicator 状态。
func (w *worker) RestoreSeqDedup(state snapshot.SeqDedupState) {
	w.seqDedup = wal.RestoreSeqDeduplicator(state.MaxSeen, state.WindowSize, state.Seen)
}

func (w *worker) Commit(ctx context.Context, events []wal.Event) error {
	commitStart := time.Now()
	// 端到端 commit 耗时：含 wal 写入 + apply + 同步 listener。activeUpdate 失败时也观测，
	// 反映"业务侧实际感知到的延迟"。listener 没有独立 metric，所以本指标是覆盖整链路的唯一窗口。
	defer func() {
		w.metrics.ObserveCommit(time.Since(commitStart))
	}()

	offset, err := w.activeUpdate(ctx, events)
	if err != nil {
		return err
	}

	// walSentListeners 在 freeze（startReplay）之后是不可变 slice。worker.Commit 只在
	// freeze 之后被触发——调用方从 setup 到业务 Commit 之间必然存在 happens-before 边
	// （sequenced-before / goroutine spawn / channel sync），足以让这里看到全部 Add 写入。
	// 不需要 atomic 也不需要锁。详见 Store 字段处的并发模型说明。
	//
	// 同步调用：listener 的阻塞会直接传导到 Tx.Commit 的 P99。WalSentListener 接口
	// 文档约定 OnWalSent 必须非阻塞（仅做本地投递，重活交后台 goroutine）。
	// 这里不做 timeout / async 包裹是有意为之——框架强制异步化反而会掩盖 listener 实现 bug，
	// 把 panic 变成静默丢消息。
	listeners := w.store.walSentListeners
	if len(listeners) == 0 {
		return nil
	}
	listenerStart := time.Now()
	for _, listener := range listeners {
		listener.OnWalSent(ctx, WalRecord{
			Partition: w.id,
			Events:    events,
			Offset:    offset,
		})
	}
	w.metrics.ObserveListenerWalSent(time.Since(listenerStart))
	return nil
}

// Close 关闭 worker 持有的资源（目前是 walWriter）。
// 调用契约：调用方应已确保不再有任何写入路径会触发 walWriter.Append——
// 即 replay 已停止（或本进程不再发起 Commit）。closeOnce 保证多次调用安全。
func (w *worker) Close() error {
	var err error
	w.closeOnce.Do(func() {
		if w.walWriter != nil {
			err = w.walWriter.Close()
		}
	})
	return err
}

func (w *worker) activeUpdate(ctx context.Context, events []wal.Event) (wal.Offset, error) {
	seq := w.nextSeq()
	walItem := &wal.Wal{
		Events: events,
	}
	offset, err := w.appendWal(ctx, wal.WalTypeBusiness, seq, walItem)
	if err != nil {
		w.metrics.IncTx(metrics.TxError)
		return 0, err
	}

	// WAL 已 durable。此时 applyEvents 失败意味着 Master 本地内存视图与 WAL 永久不一致：
	//   - Slave / 重启后 replay 都会基于 WAL 把这些 events apply 到自己的状态
	//   - 但 Master 当前进程内存里没有这些 events，对外的读会返回错误状态
	// 唯一一致的恢复路径是崩溃 + 重启 → replay 从 WAL 重建本地状态。
	// 因此把这种情况视作 fatal——panic 让进程退出，让重启来对账。
	// （多 shard 场景下 applyEvents 也无原子性：第一个 shard 已改、第二个失败时，
	//   shard 间状态也是不一致的，同样需要重启 replay 修复。）
	applyStart := time.Now()
	if err := w.store.applyEvents(events); err != nil {
		panic(fmt.Sprintf(
			"store: WAL durable at offset %d but applyEvents failed: %v "+
				"— local state divergent from WAL, crashing for replay reconciliation",
			offset, err,
		))
	}
	w.metrics.ObserveApply(time.Since(applyStart))
	w.metrics.ObserveEventsPerCommit(len(events))
	w.metrics.IncTx(metrics.TxOK)

	return offset, nil
}

// appendWal 编码 Wal payload 并连同 Header 写入底层 Writer，含指数退避重试。
// walType 和 seq 不在 Wal struct 里——它们由 Header 携带。
// 供业务 WAL、ack WAL、broadcast WAL 共用。
func (w *worker) appendWal(ctx context.Context, walType wal.WalType, seq int64, walItem *wal.Wal) (wal.Offset, error) {
	startTime := time.Now()

	data, err := w.walCodec.Encode(walItem)
	if err != nil {
		w.metrics.IncWALWriteError(metrics.WALErrEncode)
		return 0, err
	}

	// Append 使用同一个 seq 无限重试，直到写入成功或 ctx 被取消。
	//
	// 为什么复用 seq：
	//   Writer.Append 在消息入队后不再检查 ctx，因此 Append 返回错误时
	//   消息可能已写入 Kafka（客户端内部重试），也可能尚未写入。
	//   复用同一 seq 重试，无论哪种情况，replay 时 SeqDeduplicator 都能
	//   幂等地跳过重复写入，不会产生业务数据被重复执行的问题。
	//
	// 为什么不用新 seq 重试：
	//   新 seq 意味着同一批 Events 在 Kafka 中以两个不同的 seq 出现，
	//   SeqDeduplicator 无法识别，replay 时两条都会被 Apply，导致数据重复。
	record := wal.WriteRecord{
		Header: wal.Header{
			Format:  w.walConfig.Writer.Format,
			Seq:     seq,
			WalType: walType,
		},
		Data: data,
	}
	initBackoff := w.walConfig.Writer.GetAppendRetryInitBackoff()
	maxBackoff := w.walConfig.Writer.GetAppendRetryMaxBackoff()
	backoff := initBackoff
	var offset wal.Offset
	for {
		var appendErr error
		offset, appendErr = w.walWriter.Append(ctx, record)
		if appendErr == nil {
			break
		}
		// ctx 已取消（优雅关闭或上层主动放弃），停止重试
		if ctx.Err() != nil {
			w.metrics.IncWALWriteError(metrics.WALErrCtxCanceled)
			return 0, ctx.Err()
		}
		// seq 落后当前最新 seq 过多 → 放弃重试。
		if lag := w.seq.Load() - seq; lag > wal.DefaultMaxSeqLag {
			w.metrics.IncWALWriteError(metrics.WALErrSeqLag)
			return 0, fmt.Errorf("wal append: seq %d lagged %d behind current seq %d (max %d), giving up",
				seq, lag, w.seq.Load(), wal.DefaultMaxSeqLag)
		}
		w.metrics.IncWALWriteError(metrics.WALErrTransient)
		w.metrics.IncWALWriteRetry()
		log.Warn("wal append failed, retrying",
			log.Int("partition", w.id),
			log.Int64("seq", seq),
			log.Duration("backoff", backoff),
			log.NamedError("err", appendErr),
		)
		select {
		case <-ctx.Done():
			w.metrics.IncWALWriteError(metrics.WALErrCtxCanceled)
			return 0, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}

	w.metrics.ObserveWALWrite(walType, time.Since(startTime), len(data))

	return wal.Offset(offset), nil
}
