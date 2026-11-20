package wal

import (
	"context"
	"fmt"
)

// ErrEndOfWAL is returned by Reader.Next when the end of the WAL is reached.
// 表示当前没有数据可读（例如分区为空，或 Seek 的位置已超过 HighWaterMark）
var ErrEndOfWAL = fmt.Errorf("end of WAL reached")

// ErrReaderClosed is returned by Reader.Next when the Reader has been closed.
var ErrReaderClosed = fmt.Errorf("reader closed")

// Reader 读取Wal的接口，提供了Seek和Next方法，Seek用于定位到指定offset，Next用于读取下一条记录。
// Seek 接收 ctx：kafka 实现里 Seek 会向 broker 发 ListOffsets 查 HWM，这是一次网络往返，
// 必须受 replay ctx 的 deadline / cancel 约束，否则 broker 卡住时会无视 replay 超时永久阻塞。
type Reader interface {
	Seek(ctx context.Context, offset Offset) error
	Next(ctx context.Context) (*ReadRecord, error)
	Close() error
}

// Writer 写入Wal的接口，提供了Append方法用于追加记录，Close方法用于关闭Writer释放资源
type Writer interface {
	Append(ctx context.Context, record WriteRecord) (Offset, error)
	Close() error
}
