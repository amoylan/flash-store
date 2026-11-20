package noop

import (
	"context"
	"sync/atomic"

	"github.com/amoylan/flash-store/store/wal"
	"github.com/amoylan/flash-store/store/wal/config"
	"github.com/amoylan/flash-store/store/wal/source"
)

type noopWriter struct {
	next atomic.Int64
}

func (w *noopWriter) Append(ctx context.Context, rec wal.WriteRecord) (wal.Offset, error) {
	return wal.Offset(w.next.Add(1)), nil
}

func (w *noopWriter) Close() error { return nil }

type noopReader struct{}

func (noopReader) Seek(_ context.Context, offset wal.Offset) error { return nil }

func (noopReader) Next(ctx context.Context) (*wal.ReadRecord, error) {
	return nil, wal.ErrEndOfWAL
}

func (noopReader) Close() error { return nil }

type noopSource struct{}

// partition 参数对 noop backend 无意义——保留以满足 Source 接口签名。
func (noopSource) NewReader(c config.Reader, partition int) (wal.Reader, error) {
	return noopReader{}, nil
}

func (noopSource) NewWriter(c config.Writer, partition int) (wal.Writer, error) {
	return &noopWriter{}, nil
}

func init() {
	source.RegisterSource(config.SourceTypeNoop, noopSource{})
}
