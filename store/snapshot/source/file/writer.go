package file

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/amoylan/flash-store/store/snapshot"
)

type fileWriter struct {
	dir string
}

// BeginWrite 生成时间戳版本号并创建版本目录。
func (w *fileWriter) BeginWrite(_ context.Context, name string) (string, error) {
	version := time.Now().UTC().Format("20060102-150405")
	if err := os.MkdirAll(versionDir(w.dir, name, version), 0755); err != nil {
		return "", fmt.Errorf("snapshot file: mkdir version dir: %w", err)
	}
	return version, nil
}

// WriteSegment 将 segment 数据写入本地文件。
func (w *fileWriter) WriteSegment(_ context.Context, seg snapshot.Segment, data []byte) error {
	return os.WriteFile(segmentPath(w.dir, seg), data, 0644)
}

// Finish 写入 _SUCCESS 标记文件，标志该版本写入完成。
func (w *fileWriter) Finish(_ context.Context, name string, version string) error {
	return os.WriteFile(successMarkerPath(w.dir, name, version), []byte{}, 0644)
}

// Abort 删除该版本目录及所有已写入的文件。
func (w *fileWriter) Abort(_ context.Context, name string, version string) error {
	return os.RemoveAll(versionDir(w.dir, name, version))
}

func (w *fileWriter) Close() error {
	return nil
}
