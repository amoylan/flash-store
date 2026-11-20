package store

import (
	"github.com/amoylan/flash-store/store/metrics"
)

// storeStateProvider 实现 metrics.StateProvider，把 GaugeFunc 与 Store 内部状态解耦。
//
// 每个方法都是 scrape-time 调用——保持轻量、无阻塞、并发安全。返回值代表"调用瞬间"
// 的活态，不要求与具体业务事件原子对齐（prometheus 自然容忍 scrape 误差）。
//
// 各 GaugeFunc 通过此 provider 在 NewStore 末尾一次性注册（见 metrics.RegisterStateGauges）。
//
// 编译期接口断言：缺方法或签名漂移立刻被编译器拦下。
var _ metrics.StateProvider = storeStateProvider{}

type storeStateProvider struct {
	s *Store
}

func (p storeStateProvider) NumWorkers() int { return len(p.s.workers) }

func (p storeStateProvider) ReplayAppliedOffset(workerID int) int64 {
	w := p.s.getWorker(workerID)
	if w == nil {
		return -1
	}
	return w.LoadMaxWalOffset()
}

func (p storeStateProvider) ReplayState(_ int) int {
	if p.s.ReplayErr() != nil {
		return 2
	}
	if p.s.isReplayRunning() {
		return 1
	}
	return 0
}

func (p storeStateProvider) LastSnapshotWalOffset(workerID int) int64 {
	if p.s.lastSnapshotWalOffsets == nil {
		return -1
	}
	if off, ok := p.s.lastSnapshotWalOffsets[workerID]; ok {
		return off
	}
	return -1
}
