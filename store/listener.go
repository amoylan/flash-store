package store

import (
	"context"

	"github.com/amoylan/flash-store/store/wal"
)

// WalRecord 包含 wal 的信息以及 offset
type WalRecord struct {
	Partition int
	// wal 中的 events 顺序按照写入的顺序，wal.Event可能属于一个或多个用户
	Events []wal.Event
	Offset wal.Offset
}

// WalSentListener 正向更新时，发送wal到kafka成功后的监听器
type WalSentListener interface {
	// OnWalSent 当业务 WAL 被 sent 后调用。
	// 业务侧可以根据 WAL 中的 Events 做相应后续处理，例如构建消息并推送。
	// 注意：
	// 1. 反向回放的时候 event 中的 BizData 一定是 raw bytes（RawBizData的形式），业务侧需要额外自己解析 BizData
	// 2. 一般情况下，推送撮合要求是有序的，业务侧实现 Listener 时，需要保证顺序性
	// 3. **非阻塞契约（强约束）**：OnWalSent 在 worker.Commit 的 caller goroutine 上同步执行——
	//    Master 调 Tx.Commit 的业务路径会等所有 listener 返回后才返回。任何阻塞都会直接打穿
	//    Tx.Commit 的 P99：
	//      - 不可在回调内做 channel send 阻塞、锁争用、磁盘/网络 IO、cgo 等可能挂起的操作；
	//      - 推送/落盘/远程调用必须经一个**有界缓冲 + 后台 goroutine**完成，OnWalSent 内只做
	//        投递到本地结构（slice append + cond.Signal、ring buffer enqueue 等）；
	//      - 缓冲满时的策略由业务自定，但必须避免在 OnWalSent 里阻塞等待消费——首选丢弃 + 报警，
	//        次选 panic 让进程崩溃由运维介入，最差不要默默 wait。
	//    Store 不在框架内强制异步化，是为了让 listener 自由选择缓冲策略；代价是契约必须由业务自检。
	OnWalSent(ctx context.Context, record WalRecord)
}

// WalReplayedListener 在 replay 阶段，对每条业务 WAL 在 apply 成功后立即调用（immediate 路径）。
// 不做任何去重 / 补发判定——一致性补偿（如 outbox / record 方案）由业务侧基于本接口自行构建。
type WalReplayedListener interface {
	OnWalReplayed(ctx context.Context, record WalRecord)
}
