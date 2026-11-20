package snapshot

import "context"

// ======================== 片段标识 ========================

// Segment 标识 snapshot 中的一个独立片段，用于 Reader/Writer 的 key 寻址。
// Source 实现负责将 Segment 映射为具体路径：
//
//	file source → <baseDir>/<Name>/<Version>/shard-03.dat
//	S3 source   → s3://bucket/<Name>/<Version>/worker-07.dat
type Segment struct {
	Name    string // 逻辑名称（不变，如 "order-store"）
	Version string // 版本标识（由 Source 生成/管理，如 "v42"、"20260408-153000"）
	Kind    string // 片段类型："header" / "shard" / "worker"
	ID      int    // 片段编号（header 固定为 0，shard/worker 为各自 ID）
}

// ======================== Reader ========================

// Reader 从底层存储读取原始字节，由 source 实现。
// 只负责 I/O，不关心编解码格式。
//
// 各片段独立存储，ReadSegment 可安全并行调用。
type Reader interface {
	// LatestVersion 返回指定 name 下最新的版本标识。
	// 若该 name 下不存在任何版本，返回 ErrNoSnapshot。
	LatestVersion(ctx context.Context, name string) (string, error)

	// ReadSegment 按片段标识读取原始字节
	ReadSegment(ctx context.Context, seg Segment) ([]byte, error)

	Close() error
}

// ======================== Writer ========================

// Writer 将原始字节写入底层存储，由 source 实现。
// 只负责 I/O，不关心编解码格式。
//
// 各片段独立存储，WriteSegment 可安全并行调用。
type Writer interface {
	// BeginWrite 为指定 name 创建一个新版本，返回 Source 生成的版本标识。
	// 后续 WriteSegment / Finish / Abort 均使用该 version。
	// Source 内部决定版本命名策略（时间戳、递增序号等）。
	BeginWrite(ctx context.Context, name string) (version string, err error)

	// WriteSegment 按片段标识写入原始字节
	WriteSegment(ctx context.Context, seg Segment, data []byte) error

	// Finish 完成写入，将临时数据原子提交为正式 snapshot 版本
	Finish(ctx context.Context, name string, version string) error

	// Abort 取消写入，清理已写入的临时数据
	Abort(ctx context.Context, name string, version string) error

	Close() error
}
