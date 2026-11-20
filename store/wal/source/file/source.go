package file

import (
	"fmt"
	"path/filepath"

	"github.com/amoylan/flash-store/store/wal"
	"github.com/amoylan/flash-store/store/wal/config"
	"github.com/amoylan/flash-store/store/wal/source"
	fileconfig "github.com/amoylan/flash-store/store/wal/source/file/config"
)

type fileSource struct{}

// partition 用作 Dir 子目录后缀 partition-<n>，让每个 worker 写入独立目录，
// 避免多 partition 写同一目录冲突。此前调用方（worker.go / replayer.go）
// 自己改 config.File.Dir 拼路径——现在由 source 内部统一处理，更对称、
// 也防止调用方漏掉 partition 拼接逻辑。
func (s *fileSource) NewReader(conf config.Reader, partition int) (wal.Reader, error) {
	return newReader(withPartitionDirReader(conf.File, partition))
}

func (s *fileSource) NewWriter(conf config.Writer, partition int) (wal.Writer, error) {
	return newWriter(withPartitionDirWriter(conf.File, partition))
}

func withPartitionDirReader(c fileconfig.Reader, partition int) fileconfig.Reader {
	if c.Dir != "" {
		c.Dir = filepath.Join(c.Dir, fmt.Sprintf("partition-%d", partition))
	}
	return c
}

func withPartitionDirWriter(c fileconfig.Writer, partition int) fileconfig.Writer {
	if c.Dir != "" {
		c.Dir = filepath.Join(c.Dir, fmt.Sprintf("partition-%d", partition))
	}
	return c
}

func init() {
	source.RegisterSource(config.SourceTypeFile, &fileSource{})
}
