package s3

import (
	"fmt"
	"strings"

	"github.com/amoylan/flash-store/store/snapshot"
)

// segmentFileName 返回片段文件名，如 "header-00.dat"、"shard-03.dat"
func segmentFileName(seg snapshot.Segment) string {
	return fmt.Sprintf("%s-%02d.dat", seg.Kind, seg.ID)
}

// segmentKey 返回正式路径的完整 S3 key
// 格式: <prefix>/<name>/<version>/<kind>-<id>.dat
func segmentKey(prefix, name, version string, seg snapshot.Segment) string {
	return joinKey(prefix, name, version, segmentFileName(seg))
}

// successMarkerKey 返回 _SUCCESS 标记文件的 S3 key
func successMarkerKey(prefix, name, version string) string {
	return joinKey(prefix, name, version, "_SUCCESS")
}

// versionPrefix 返回某个 version 的目录前缀（含尾部 /）
func versionPrefix(prefix, name, version string) string {
	return joinKey(prefix, name, version) + "/"
}

// namePrefix 返回某个 name 的目录前缀（含尾部 /），用于列出所有版本
func namePrefix(prefix, name string) string {
	return joinKey(prefix, name) + "/"
}

// joinKey 将路径各段用 "/" 连接，忽略空段
func joinKey(parts ...string) string {
	nonEmpty := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.Trim(p, "/")
		if p != "" {
			nonEmpty = append(nonEmpty, p)
		}
	}
	return strings.Join(nonEmpty, "/")
}
