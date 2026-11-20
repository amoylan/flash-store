// Package metrics 集中定义 flash-store 的 Prometheus 指标。
//
// 设计原则：
//
//  1. flash-store 是 library，可能被多个 binary（master / pack-server / readonly-slave）集成。
//     业务侧通过 NewStore 的 Registerer 参数传入 prometheus.Registerer——库本身只关心
//     "在这个 Registerer 上注册指标"，不关心进程层面的 service / role 等常量标签。
//     业务想加 const label，自行用 prometheus.WrapRegistererWith 包一层即可。
//
//  2. 库 nil-safe：Registerer 为 nil 时回退到 prometheus.NewRegistry()（throwaway），
//     调用点不需要写 nil 判断、热路径无分支开销。
//
//  3. 标签基数严格控制：
//     - 允许：worker_id（=numWorkers，几十封顶）、wal_type（4 个）、result / trigger / phase（枚举）、op（读路径方法名）。
//     - 禁止：order_id / user_id / 任意业务主键。
//
//  4. 直方图同时启用 native histogram（稀疏自适应）+ classic buckets。
//     Prometheus 2.40+ 优先用 native；老版本回落 classic。耗时统一以 **秒** 为单位
//     （base unit；Grafana panel 设 unit=seconds 会自动按数量级渲染 ms / µs）。
//
//  5. 状态量（buffer 深度、应用 offset 等）走 GaugeFunc——scrape 时回调读取活态，
//     不在热路径上 Inc/Dec/Set，避免污染 worker.Commit 等关键路径。GaugeFunc 由
//     RegisterStateGauges(provider) 在 Store 构造完成后挂上。
//
//  6. 调用约定：热路径（Commit / appendWal / ack flush）通过 WorkerMetrics 类型化方法
//     调用，避免裸 WithLabelValues 看不出 label 含义。每个 worker 构造时 curry 一次，
//     热路径无 map lookup 开销。
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/amoylan/flash-store/store/wal"
)

// Metrics 是 store 级别的 metrics 注册中心。
// 内部持有 prometheus collector 句柄；调用方只通过类型化方法 + WorkerMetrics 访问，不直接接触 Vec。
type Metrics struct {
	reg prometheus.Registerer

	// ============== WAL 写入（per worker × wal_type） ==============
	walWriteDuration *prometheus.HistogramVec // labels: worker_id, wal_type
	walWriteBytes    *prometheus.CounterVec   // labels: worker_id, wal_type
	walSeqTotal      *prometheus.CounterVec   // labels: worker_id, wal_type
	walWriteErrors   *prometheus.CounterVec   // labels: worker_id, kind
	walWriteRetries  *prometheus.CounterVec   // labels: worker_id

	// ============== Commit / Apply / Tx（per worker） ==============
	commitDuration  *prometheus.HistogramVec // labels: worker_id —— 端到端含 listener 同步回调
	applyDuration   *prometheus.HistogramVec // labels: worker_id —— 仅 applyEvents
	txTotal         *prometheus.CounterVec   // labels: worker_id, result
	eventsPerCommit prometheus.Histogram     // store 全局共享

	// ============== Listener 回调耗时（per worker × path） ==============
	// 不带 listener name——只看聚合耗时；担心总耗时拖累 commit / replay 时这条够用。
	// 真要细分到具体 listener 时再扩 name label。
	listenerDuration *prometheus.HistogramVec // labels: worker_id, path

	// ============== Snapshot（store 级） ==============
	snapshotCreateDuration  *prometheus.HistogramVec // labels: phase
	snapshotCreateTotal     *prometheus.CounterVec   // labels: result
	snapshotSizeBytes       prometheus.Histogram
	snapshotRestoreDuration *prometheus.HistogramVec // labels: result

	// ============== Read API（store 级） ==============
	readDuration   *prometheus.HistogramVec // labels: op
	readTotal      *prometheus.CounterVec   // labels: op, result
	readResultSize *prometheus.HistogramVec // labels: op —— 批量 / 查询接口的返回行数
}

// New 用给定的 Registerer 创建并注册一组 Metrics。
// reg 为 nil 时使用 throwaway Registry——所有方法仍可正常调用、不报错、无副作用。
//
// 重复在同一个 Registerer 上调用 New 会触发 prometheus.AlreadyRegisteredError——
// 业务侧若需要同一进程内多个 Store 共享 Registerer，应当通过
// prometheus.WrapRegistererWith 给每个 Store 加一层 const label 区分。
func New(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	f := promauto.With(reg)

	latencyOpts := func(name, help string) prometheus.HistogramOpts {
		return prometheus.HistogramOpts{
			Name:                            name,
			Help:                            help,
			Buckets:                         prometheus.ExponentialBucketsRange(0.00005, 5, 14), // 50µs ~ 5s
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 0,
		}
	}
	sizeOpts := func(name, help string) prometheus.HistogramOpts {
		return prometheus.HistogramOpts{
			Name:                        name,
			Help:                        help,
			Buckets:                     prometheus.ExponentialBucketsRange(1024, 4*1024*1024*1024, 16),
			NativeHistogramBucketFactor: 1.5,
		}
	}
	countOpts := func(name, help string) prometheus.HistogramOpts {
		return prometheus.HistogramOpts{
			Name:                        name,
			Help:                        help,
			Buckets:                     prometheus.ExponentialBucketsRange(1, 10000, 12),
			NativeHistogramBucketFactor: 1.5,
		}
	}

	return &Metrics{
		reg: reg,

		walWriteDuration: f.NewHistogramVec(
			latencyOpts("flashstore_wal_write_duration_seconds",
				"appendWal 端到端耗时（含 Kafka ACK 等待与重试 backoff）"),
			[]string{labelWorkerID, labelWalType},
		),
		walWriteBytes: f.NewCounterVec(prometheus.CounterOpts{
			Name: "flashstore_wal_write_bytes_total",
			Help: "appendWal 写入字节数",
		}, []string{labelWorkerID, labelWalType}),
		walSeqTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "flashstore_wal_seq_total",
			Help: "appendWal 成功写入的 WAL 条数",
		}, []string{labelWorkerID, labelWalType}),
		walWriteErrors: f.NewCounterVec(prometheus.CounterOpts{
			Name: "flashstore_wal_write_errors_total",
			Help: "appendWal 错误次数（重试中的瞬时错误也计入）",
		}, []string{labelWorkerID, labelKind}),
		walWriteRetries: f.NewCounterVec(prometheus.CounterOpts{
			Name: "flashstore_wal_write_retries_total",
			Help: "appendWal 重试次数",
		}, []string{labelWorkerID}),

		commitDuration: f.NewHistogramVec(
			latencyOpts("flashstore_commit_duration_seconds",
				"worker.Commit 端到端耗时——含 wal_write + apply + 同步 listener 回调"),
			[]string{labelWorkerID},
		),
		applyDuration: f.NewHistogramVec(
			latencyOpts("flashstore_apply_duration_seconds",
				"applyEvents 耗时（不含 WAL 写入与 listener）"),
			[]string{labelWorkerID},
		),
		txTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "flashstore_tx_total",
			Help: "业务 Commit 计数",
		}, []string{labelWorkerID, labelResult}),
		eventsPerCommit: f.NewHistogram(
			countOpts("flashstore_tx_events_per_commit",
				"每个 Commit 包含的 event 数分布"),
		),

		listenerDuration: f.NewHistogramVec(
			latencyOpts("flashstore_listener_duration_seconds",
				"Listener 回调聚合耗时——wal_sent: worker.Commit 里所有 WalSentListener 总耗时；wal_replayed: replay 期间单条 OnWalReplayed 耗时"),
			[]string{labelWorkerID, labelPath},
		),

		snapshotCreateDuration: f.NewHistogramVec(
			latencyOpts("flashstore_snapshot_create_duration_seconds",
				"CreateSnapshot 各阶段耗时"),
			[]string{labelPhase},
		),
		snapshotCreateTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "flashstore_snapshot_create_total",
			Help: "CreateSnapshot 完成计数",
		}, []string{labelResult}),
		snapshotSizeBytes: f.NewHistogram(
			sizeOpts("flashstore_snapshot_size_bytes",
				"snapshot 总字节数分布"),
		),
		snapshotRestoreDuration: f.NewHistogramVec(
			latencyOpts("flashstore_snapshot_restore_duration_seconds",
				"restoreFromSnapshot 总耗时"),
			[]string{labelResult},
		),

		readDuration: f.NewHistogramVec(
			latencyOpts("flashstore_read_duration_seconds",
				"Read API 调用耗时"),
			[]string{labelOp},
		),
		readTotal: f.NewCounterVec(prometheus.CounterOpts{
			Name: "flashstore_read_total",
			Help: "Read API 调用计数",
		}, []string{labelOp, labelResult}),
		readResultSize: f.NewHistogramVec(
			countOpts("flashstore_read_result_size",
				"批量 / 查询 Read API 的返回行数分布"),
			[]string{labelOp},
		),
	}
}

// Registerer 返回构造时使用的 Registerer，供子系统（如 wal/source/kafka）
// 注册自定义 Collector 时取用。
func (m *Metrics) Registerer() prometheus.Registerer {
	if m == nil {
		return nil
	}
	return m.reg
}

// =================================================================
// Store-level typed methods —— snapshot / read
// =================================================================

// SnapshotPhase snapshot 创建的阶段；值用作稳定 metrics label，请勿轻改。
type SnapshotPhase string

const (
	SnapshotPhaseHeader        SnapshotPhase = "header"
	SnapshotPhaseShardsWorkers SnapshotPhase = "shards_workers"
	SnapshotPhaseFinish        SnapshotPhase = "finish"
	SnapshotPhaseTotal         SnapshotPhase = "total"
)

// SnapshotResult / RestoreResult 同上。
type SnapshotResult string

const (
	SnapshotResultOK    SnapshotResult = "ok"
	SnapshotResultError SnapshotResult = "error"
	// RestoreResultNoSnapshot 在 RestoreFromSnapshot 静默降级路径上使用。
	RestoreResultNoSnapshot SnapshotResult = "no_snapshot"
)

// ObserveSnapshotPhase 记录 CreateSnapshot 某一阶段的耗时。
func (m *Metrics) ObserveSnapshotPhase(phase SnapshotPhase, d time.Duration) {
	m.snapshotCreateDuration.WithLabelValues(string(phase)).Observe(d.Seconds())
}

// IncSnapshotCreate 记录 CreateSnapshot 完成事件。
func (m *Metrics) IncSnapshotCreate(result SnapshotResult) {
	m.snapshotCreateTotal.WithLabelValues(string(result)).Inc()
}

// ObserveSnapshotSize 记录 CreateSnapshot 写入的总字节。
func (m *Metrics) ObserveSnapshotSize(bytes int64) {
	m.snapshotSizeBytes.Observe(float64(bytes))
}

// ObserveSnapshotRestore 记录 restoreFromSnapshot 总耗时及结果分类。
func (m *Metrics) ObserveSnapshotRestore(result SnapshotResult, d time.Duration) {
	m.snapshotRestoreDuration.WithLabelValues(string(result)).Observe(d.Seconds())
}

// ReadOp Read API 的方法名；值用作稳定 metrics label。
type ReadOp string

const (
	ReadOpGetObject              ReadOp = "get_object"
	ReadOpGetObjects             ReadOp = "get_objects"
	ReadOpGetView                ReadOp = "get_view"
	ReadOpGetViews               ReadOp = "get_views"
	ReadOpQueryObjects           ReadOp = "query_objects"
	ReadOpQueryObjectsByIdentity ReadOp = "query_objects_by_identity"
	ReadOpQueryViews             ReadOp = "query_views"
	ReadOpQueryViewsByIdentity   ReadOp = "query_views_by_identity"
	ReadOpGetKV                  ReadOp = "get_kv"
	ReadOpCheckKVExists          ReadOp = "check_kv_exists"
)

// ObserveRead 记录一次 Read API 调用的耗时与结果。
//   - resultSize < 0 时跳过 readResultSize（适用于单对象返回值）。
//   - err == nil → result="ok"；否则 "error"。
func (m *Metrics) ObserveRead(op ReadOp, d time.Duration, err error, resultSize int) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	m.readDuration.WithLabelValues(string(op)).Observe(d.Seconds())
	m.readTotal.WithLabelValues(string(op), result).Inc()
	if resultSize >= 0 {
		m.readResultSize.WithLabelValues(string(op)).Observe(float64(resultSize))
	}
}

// =================================================================
// Worker-level pre-curried metrics —— ForWorker(id) 一次性 curry 后挂在 worker 上。
// 热路径调用 wm.ObserveCommit(d) / wm.IncWALWriteRetry() 等；零 map lookup。
// =================================================================

// WALErrorKind appendWal 错误分类。
type WALErrorKind string

const (
	WALErrEncode      WALErrorKind = "encode"
	WALErrTransient   WALErrorKind = "transient"
	WALErrCtxCanceled WALErrorKind = "ctx_canceled"
	WALErrSeqLag      WALErrorKind = "seq_lag_giveup"
)

// TxResult 业务 Commit 结果。
type TxResult string

const (
	TxOK    TxResult = "ok"
	TxError TxResult = "error"
)

// ListenerPath listener 回调的触发路径。
//   - wal_sent：worker.Commit 上 WalSentListener 同步触发链路（master 正常写入路径）。
//   - wal_replayed：replay 期间 WalReplayedListener 触发（immediate 路径）。
type ListenerPath string

const (
	ListenerPathWalSent     ListenerPath = "wal_sent"
	ListenerPathWalReplayed ListenerPath = "wal_replayed"
)

// WorkerMetrics 是单个 worker 的 curried metric 句柄。
// 由 Metrics.ForWorker(id) 在 worker 构造时创建一次，热路径直接调用方法、无标签查找开销。
type WorkerMetrics struct {
	parent *Metrics

	// commit / apply / tx
	commit          prometheus.Observer
	apply           prometheus.Observer
	txOK            prometheus.Counter
	txError         prometheus.Counter
	eventsPerCommit prometheus.Observer // 共享一个 store-level histogram

	// wal write —— 按 wal_type 索引；下标必须小于 walTypeMax。
	walWriteDuration [walTypeMax]prometheus.Observer
	walWriteBytes    [walTypeMax]prometheus.Counter
	walSeqTotal      [walTypeMax]prometheus.Counter

	walWriteRetries        prometheus.Counter
	walWriteErrEncode      prometheus.Counter
	walWriteErrTransient   prometheus.Counter
	walWriteErrCtxCanceled prometheus.Counter
	walWriteErrSeqLag      prometheus.Counter

	// ack batcher
	ackFlushSize     prometheus.Counter
	ackFlushInterval prometheus.Counter
	ackFlushShutdown prometheus.Counter
	ackFlushDuration prometheus.Observer
	ackBatchSize     prometheus.Observer
	ackFlushErrors   prometheus.Counter

	// listener 回调耗时（聚合）
	listenerWalSent     prometheus.Observer
	listenerWalReplayed prometheus.Observer
}

// ForWorker 返回 workerID 对应的 curried metric 集合。
// 由 worker 在 newWorker 时调用一次缓存。
func (m *Metrics) ForWorker(workerID int) *WorkerMetrics {
	wid := workerIDLabel(workerID)
	wm := &WorkerMetrics{
		parent: m,

		commit:          m.commitDuration.WithLabelValues(wid),
		apply:           m.applyDuration.WithLabelValues(wid),
		txOK:            m.txTotal.WithLabelValues(wid, string(TxOK)),
		txError:         m.txTotal.WithLabelValues(wid, string(TxError)),
		eventsPerCommit: m.eventsPerCommit,

		walWriteRetries:        m.walWriteRetries.WithLabelValues(wid),
		walWriteErrEncode:      m.walWriteErrors.WithLabelValues(wid, string(WALErrEncode)),
		walWriteErrTransient:   m.walWriteErrors.WithLabelValues(wid, string(WALErrTransient)),
		walWriteErrCtxCanceled: m.walWriteErrors.WithLabelValues(wid, string(WALErrCtxCanceled)),
		walWriteErrSeqLag:      m.walWriteErrors.WithLabelValues(wid, string(WALErrSeqLag)),

		listenerWalSent:     m.listenerDuration.WithLabelValues(wid, string(ListenerPathWalSent)),
		listenerWalReplayed: m.listenerDuration.WithLabelValues(wid, string(ListenerPathWalReplayed)),
	}
	// 按 walType 预 curry
	for t, name := range walTypeLabels {
		wm.walWriteDuration[t] = m.walWriteDuration.WithLabelValues(wid, name)
		wm.walWriteBytes[t] = m.walWriteBytes.WithLabelValues(wid, name)
		wm.walSeqTotal[t] = m.walSeqTotal.WithLabelValues(wid, name)
	}
	return wm
}

// =================================================================
// WorkerMetrics 类型化方法
// =================================================================

// ObserveCommit 记录 worker.Commit 端到端耗时（含 wal_write + apply + listener）。
func (wm *WorkerMetrics) ObserveCommit(d time.Duration) {
	wm.commit.Observe(d.Seconds())
}

// ObserveApply 记录 applyEvents 耗时（不含 WAL 写入和 listener）。
func (wm *WorkerMetrics) ObserveApply(d time.Duration) {
	wm.apply.Observe(d.Seconds())
}

// IncTx 记录一次 Commit 的结果（ok/error）。
func (wm *WorkerMetrics) IncTx(result TxResult) {
	switch result {
	case TxOK:
		wm.txOK.Inc()
	case TxError:
		wm.txError.Inc()
	}
}

// ObserveEventsPerCommit 记录单次 Commit 的 event 数。
func (wm *WorkerMetrics) ObserveEventsPerCommit(n int) {
	wm.eventsPerCommit.Observe(float64(n))
}

// ObserveWALWrite 一次成功的 appendWal：记录耗时、字节、计数。
func (wm *WorkerMetrics) ObserveWALWrite(t wal.WalType, d time.Duration, bytes int) {
	idx := walTypeIndex(t)
	if idx < 0 {
		return
	}
	wm.walWriteDuration[idx].Observe(d.Seconds())
	wm.walWriteBytes[idx].Add(float64(bytes))
	wm.walSeqTotal[idx].Inc()
}

// IncWALWriteRetry 一次重试。
func (wm *WorkerMetrics) IncWALWriteRetry() {
	wm.walWriteRetries.Inc()
}

// IncWALWriteError 记一次 appendWal 错误（kind 见 WALErr*）。
func (wm *WorkerMetrics) IncWALWriteError(kind WALErrorKind) {
	switch kind {
	case WALErrEncode:
		wm.walWriteErrEncode.Inc()
	case WALErrTransient:
		wm.walWriteErrTransient.Inc()
	case WALErrCtxCanceled:
		wm.walWriteErrCtxCanceled.Inc()
	case WALErrSeqLag:
		wm.walWriteErrSeqLag.Inc()
	}
}

// ObserveListenerWalSent 记录 worker.Commit 中**所有** WalSentListener 同步触发的总耗时。
// 没有 listener 时调用方应跳过 timing 调用以省下 time.Now() 开销。
func (wm *WorkerMetrics) ObserveListenerWalSent(d time.Duration) {
	wm.listenerWalSent.Observe(d.Seconds())
}

// ObserveListenerWalReplayed 记录 replay 期间一次 OnWalReplayed 回调耗时（immediate 路径）。
func (wm *WorkerMetrics) ObserveListenerWalReplayed(d time.Duration) {
	wm.listenerWalReplayed.Observe(d.Seconds())
}
