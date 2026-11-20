package wal

import "sync"

const DefaultDedupWindowSize = 10_000

// DefaultMaxSeqLag 是发送端重试时允许的最大 seq 落后量（相对于当前最新 seq）。
// 取 DefaultDedupWindowSize 的一半，为接收端去重窗口留出充足裕量：
//   - 重试写入最多落后 5,000
//   - 去重窗口覆盖 10,000
//   - 即使重试消息延迟到达，也不会被去重器误判为"过期"
const DefaultMaxSeqLag = DefaultDedupWindowSize / 2

// SeqDeduplicator 在 WAL 回放时检测并剔除重复消息。
//
// 背景：
//   - 极端情况下（如 Broker 故障 + Kafka 客户端内部重试），同一条消息
//     （相同 seq）可能以两个不同的 Kafka offset 写入分区。
//   - seq 由 worker 内原子递增生成，多个 goroutine 并发调用 Writer.Append
//     时进入 kafka.Writer 内部 batch 的顺序无法保证，因此 offset 顺序与 seq 顺序
//     不严格一致（offset 小的消息 seq 可能更大）。
//   - 不能使用简单的"最大 seq 高水位"来判断重复，否则会错误地把乱序的
//     合法消息当成重复丢弃。
//
// 设计：滑动窗口 seen-set
//   - maxSeen：目前见过的最大 seq。
//   - floor = maxSeen - windowSize：低于 floor 的 seq 已超出窗口，
//     一律视为过期重复（正常情况下不可能出现，除非是真正的重复写入）。
//   - windowSize 应远大于单个 Kafka 批次内 seq 乱序的最大散布范围
//     （由 kafka.Writer.BatchSize 决定），建议保守地使用默认值 10,000。
type SeqDeduplicator struct {
	mu          sync.Mutex
	seen        map[int64]struct{}
	maxSeen     int64
	windowSize  int64
	initialized bool
}

func NewSeqDeduplicator(windowSize int64) *SeqDeduplicator {
	if windowSize <= 0 {
		windowSize = DefaultDedupWindowSize
	}
	return &SeqDeduplicator{
		seen:       make(map[int64]struct{}),
		windowSize: windowSize,
	}
}

// Snapshot 导出当前去重器的状态，用于持久化到 snapshot。
func (d *SeqDeduplicator) Snapshot() (maxSeen int64, windowSize int64, seen []int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	seen = make([]int64, 0, len(d.seen))
	for s := range d.seen {
		seen = append(seen, s)
	}
	return d.maxSeen, d.windowSize, seen
}

// RestoreFromSnapshot 从 snapshot 恢复去重器状态。
// 应在 replay 启动前调用，替代空白初始化。
func RestoreSeqDeduplicator(maxSeen int64, windowSize int64, seen []int64) *SeqDeduplicator {
	if windowSize <= 0 {
		windowSize = DefaultDedupWindowSize
	}
	seenMap := make(map[int64]struct{}, len(seen))
	for _, s := range seen {
		seenMap[s] = struct{}{}
	}
	return &SeqDeduplicator{
		seen:        seenMap,
		maxSeen:     maxSeen,
		windowSize:  windowSize,
		initialized: len(seen) > 0 || maxSeen > 0,
	}
}

// CheckAndMark 检查 seq 是否为重复，同时将其标记为"已见"。
//
//   - 首次见到：标记并返回 false（不是重复，应正常处理）。
//   - 已见过 / 低于窗口 floor：返回 true（重复，调用方应跳过）。
func (d *SeqDeduplicator) CheckAndMark(seq int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 第一条消息：以它作为窗口起点
	if !d.initialized {
		d.maxSeen = seq
		d.initialized = true
		d.seen[seq] = struct{}{}
		return false
	}

	// 低于窗口下界：seq 已过期，视为重复
	if seq <= d.maxSeen-d.windowSize {
		return true
	}

	// 已在窗口内见过
	if _, ok := d.seen[seq]; ok {
		return true
	}

	// 首次见到：记录并推进窗口
	d.seen[seq] = struct{}{}
	if seq > d.maxSeen {
		oldMax := d.maxSeen
		d.maxSeen = seq
		// 范围删除：只清理因 maxSeen 推进而新过期的 seq。
		// 旧 floor = oldMax - windowSize，新 floor = seq - windowSize，
		// 需要删除的范围是 (oldFloor, newFloor]，即 [oldFloor+1, newFloor]。
		// seq 基本连续（原子递增生成），不存在的 key delete 是 no-op。
		newFloor := d.maxSeen - d.windowSize
		oldFloor := oldMax - d.windowSize
		for s := oldFloor + 1; s <= newFloor; s++ {
			delete(d.seen, s)
		}
	}
	return false
}
