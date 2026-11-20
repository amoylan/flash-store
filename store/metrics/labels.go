package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/amoylan/flash-store/store/wal"
)

// label 名称常量集中维护——避免散落在各处出现拼写漂移。
// 这些值是 promQL 查询的稳定接口，请勿轻改。
const (
	labelWorkerID = "worker_id"
	labelWalType  = "wal_type"
	labelKind     = "kind"
	labelResult   = "result"
	labelTrigger  = "trigger"
	labelPhase    = "phase"
	labelOp       = "op"
	labelPath     = "path"
)

// workerIDLabel 把 partition / worker id 转成稳定的字符串 label 值。
func workerIDLabel(id int) string {
	return strconv.Itoa(id)
}

// WorkerLabels 构造 per-worker 的 const-label 集合，供子系统（kafka writer 等）
// 通过 prometheus.WrapRegistererWith 注入：
//
//	wrapped := prometheus.WrapRegistererWith(metrics.WorkerLabels(workerID), reg)
//	subsystem.RegisterMetrics(wrapped)
//
// 这样子系统不需要知道 label 名（"worker_id"）和取值规则——label 命名权完全在 metrics 包，
// 跨子系统天然一致，外部观察者用同一个 label 就能 join 所有 per-worker 指标。
//
// 注意：flash-store 设计里 kafka partition 与 worker 一对一对应（fixedPartitionBalancer），
// 所以 kafka writer collector 也用 worker_id 而非 partition——保持外部可观测视角统一。
func WorkerLabels(workerID int) prometheus.Labels {
	return prometheus.Labels{labelWorkerID: workerIDLabel(workerID)}
}

// =================================================================
// WalType ↔ label / array index 映射
// =================================================================
//
// walType 值不连续（0/1/2/3/4），用 dense array 索引可减少 map 查找。
// 新增 WalType 时：
//  1. 在 wal/wal.go 添加常量并扩 String()。
//  2. 此处 walTypeMax 加 1，walTypeLabels 加映射。
const walTypeMax = 5

var walTypeLabels = [walTypeMax]string{
	wal.WalTypeBusiness:     wal.WalTypeBusiness.String(),
	wal.WalTypeSchemaConfig: wal.WalTypeSchemaConfig.String(),
}

// walTypeIndex 返回 walType 在 dense array 中的下标；越界（未知 walType）返回 -1。
func walTypeIndex(t wal.WalType) int {
	idx := int(t)
	if idx < 0 || idx >= walTypeMax {
		return -1
	}
	return idx
}
