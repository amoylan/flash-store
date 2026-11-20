package noop

import (
	"context"

	"github.com/amoylan/flash-store/store/snapshot"
	"github.com/amoylan/flash-store/store/snapshot/config"
	"github.com/amoylan/flash-store/store/snapshot/source"
)

type noopReader struct{}

func (noopReader) LatestVersion(context.Context, string) (string, error) {
	return "", snapshot.ErrNoSnapshot
}

func (noopReader) ReadSegment(context.Context, snapshot.Segment) ([]byte, error) {
	return nil, snapshot.ErrNoSnapshot
}

func (noopReader) Close() error { return nil }

type noopWriter struct{}

func (noopWriter) BeginWrite(context.Context, string) (string, error) { return "noop", nil }

func (noopWriter) WriteSegment(context.Context, snapshot.Segment, []byte) error { return nil }

func (noopWriter) Finish(context.Context, string, string) error { return nil }

func (noopWriter) Abort(context.Context, string, string) error { return nil }

func (noopWriter) Close() error { return nil }

type noopSource struct{}

func (noopSource) NewReader(config.Reader) (snapshot.Reader, error) { return noopReader{}, nil }

func (noopSource) NewWriter(config.Writer) (snapshot.Writer, error) { return noopWriter{}, nil }

func init() {
	source.RegisterSource(config.SourceTypeNoop, noopSource{})
}
