package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// StateProvider 暴露给 metrics 包用于读取活态指标的接口。
// 由 Store 实现并在构造完成后通过 Metrics.RegisterStateGauges 注册。
//
// 所有方法必须并发安全：scrape 来自 Prometheus 后台 goroutine。
// 返回值代表"调用瞬间"的活态——不要求与具体业务事件原子对齐。
type StateProvider interface {
	NumWorkers() int

	// ReplayAppliedOffset worker 已 apply 的最大 WAL offset（含），未应用过返回 -1。
	ReplayAppliedOffset(workerID int) int64

	// ReplayState worker 的 replay 状态：0=idle, 1=running, 2=errored。
	// store 级状态对所有 worker 取相同值；接口仍按 worker 暴露便于未来分化。
	ReplayState(workerID int) int

	// LastSnapshotWalOffset worker 上次 snapshot 的 WalOffset；无 snapshot 历史返回 -1。
	LastSnapshotWalOffset(workerID int) int64
}

// RegisterStateGauges 在 Store 完成构造后注册状态量 GaugeFunc。
// 每个 worker 一组——基数 = numWorkers，可控。
// 注册失败（重复 / Registerer 出错）会返回 error；调用方决定要不要 fail-fast。
func (m *Metrics) RegisterStateGauges(p StateProvider) error {
	if m == nil || m.reg == nil || p == nil {
		return nil
	}
	n := p.NumWorkers()
	for i := range n {
		workerID := i // capture
		labels := prometheus.Labels{"worker_id": strconv.Itoa(workerID)}

		gauges := []struct {
			name string
			help string
			fn   func() float64
		}{
			{
				"flashstore_replay_state",
				"replay 状态: 0=idle, 1=running, 2=errored",
				func() float64 { return float64(p.ReplayState(workerID)) },
			},
			{
				"flashstore_last_snapshot_wal_offset",
				"上次 snapshot 的 WalOffset，无历史 = -1",
				func() float64 { return float64(p.LastSnapshotWalOffset(workerID)) },
			},
		}

		for _, g := range gauges {
			gf := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name:        g.name,
				Help:        g.help,
				ConstLabels: labels,
			}, g.fn)
			if err := m.reg.Register(gf); err != nil {
				if _, ok := err.(prometheus.AlreadyRegisteredError); ok {
					continue
				}
				return err
			}
		}
	}
	return nil
}
