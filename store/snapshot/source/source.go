package source

import (
	"fmt"

	"github.com/amoylan/flash-store/store/snapshot"
	"github.com/amoylan/flash-store/store/snapshot/config"
)

// Source 是 snapshot 存储后端的工厂接口（类比 wal/source.Source）
// 不同的 Source 实现对应不同的存储介质（本地文件、S3 等），只负责字节级 I/O
type Source interface {
	NewReader(conf config.Reader) (snapshot.Reader, error)
	NewWriter(conf config.Writer) (snapshot.Writer, error)
}

var sourceRegistry = map[config.SourceType]Source{}

func RegisterSource(sourceType config.SourceType, source Source) {
	sourceRegistry[sourceType] = source
}

func GetSource(sourceType config.SourceType) (Source, error) {
	source, ok := sourceRegistry[sourceType]
	if !ok {
		return nil, fmt.Errorf("snapshot source of type %s not registered", sourceType)
	}
	return source, nil
}
