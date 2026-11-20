package file

import (
	"context"
	"os"
	"sort"

	"github.com/amoylan/flash-store/store/snapshot"
)

type fileReader struct {
	dir string
}

// LatestVersion 扫描 <dir>/<name>/ 下所有版本子目录，返回最新的已完成版本（含 _SUCCESS 标记）。
func (r *fileReader) LatestVersion(_ context.Context, name string) (string, error) {
	entries, err := os.ReadDir(nameDir(r.dir, name))
	if err != nil {
		if os.IsNotExist(err) {
			return "", snapshot.ErrNoSnapshot
		}
		return "", err
	}

	var versions []string
	for _, e := range entries {
		if e.IsDir() {
			versions = append(versions, e.Name())
		}
	}

	if len(versions) == 0 {
		return "", snapshot.ErrNoSnapshot
	}

	// 时间戳格式保证字典序 == 时间序
	sort.Strings(versions)

	// 从最新往前找，第一个有 _SUCCESS 标记的即为最新完成版本
	for i := len(versions) - 1; i >= 0; i-- {
		ver := versions[i]
		if _, err := os.Stat(successMarkerPath(r.dir, name, ver)); err == nil {
			return ver, nil
		}
	}

	return "", snapshot.ErrNoSnapshot
}

// ReadSegment 读取指定 segment 文件的全部字节。
func (r *fileReader) ReadSegment(_ context.Context, seg snapshot.Segment) ([]byte, error) {
	return os.ReadFile(segmentPath(r.dir, seg))
}

func (r *fileReader) Close() error {
	return nil
}
