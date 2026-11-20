package source

import (
	"fmt"

	"github.com/amoylan/flash-store/store/wal"
	"github.com/amoylan/flash-store/store/wal/config"
)

// Source 构造 WAL Reader / Writer。partition 是 worker id —— 不属于外界配置
// （由 store 内部按 worker id 衍生），所以做成单独参数而不是 config 字段，
// 避免误以为可以从外部覆盖。
//
// 各 backend 对 partition 的使用：
//   - kafka：作为 kafka topic partition 号（writer fixedPartitionBalancer / reader Seek）
//   - file：作为 Dir 子目录后缀 partition-<n>，每个 worker 写独立文件夹避免冲突
//   - noop：忽略
type Source interface {
	NewReader(conf config.Reader, partition int) (wal.Reader, error)
	NewWriter(conf config.Writer, partition int) (wal.Writer, error)
}

var sourceRegistry = map[config.SourceType]Source{}

func RegisterSource(sourceType config.SourceType, source Source) {
	sourceRegistry[sourceType] = source
}

func GetSource(sourceType config.SourceType) (Source, error) {
	source, ok := sourceRegistry[sourceType]
	if !ok {
		return nil, fmt.Errorf("Source of type %s not registered", sourceType)
	}
	return source, nil
}
