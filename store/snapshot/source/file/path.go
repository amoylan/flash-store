package file

import (
	"fmt"
	"path/filepath"

	"github.com/amoylan/flash-store/store/snapshot"
)

// segmentFileName 返回片段文件名，如 "header-00.dat"、"shard-03.dat"
func segmentFileName(seg snapshot.Segment) string {
	return fmt.Sprintf("%s-%02d.dat", seg.Kind, seg.ID)
}

// segmentPath 返回 segment 文件的完整本地路径
// 格式: <dir>/<name>/<version>/<kind>-<id>.dat
func segmentPath(dir string, seg snapshot.Segment) string {
	return filepath.Join(dir, seg.Name, seg.Version, segmentFileName(seg))
}

// versionDir 返回某个版本的目录路径
func versionDir(dir, name, version string) string {
	return filepath.Join(dir, name, version)
}

// nameDir 返回某个 name 的目录路径（其下包含各版本子目录）
func nameDir(dir, name string) string {
	return filepath.Join(dir, name)
}

// successMarkerPath 返回 _SUCCESS 标记文件路径
func successMarkerPath(dir, name, version string) string {
	return filepath.Join(dir, name, version, "_SUCCESS")
}
