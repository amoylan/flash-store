package kafka

import (
	"github.com/amoylan/flash-store/store/wal"
	"github.com/amoylan/flash-store/store/wal/config"
	"github.com/amoylan/flash-store/store/wal/source"
)

// Kafka WAL source 基于 github.com/twmb/franz-go 实现。
//
// TODO(metrics): 当前没有直接导出 franz-go 客户端内部指标。若要增加
// flashstore_kafka_writer_*，需要通过 kgo.Hook* 接口采集，并先明确各指标的
// 稳定语义和基数上限。当前调用方拿到的 *Writer 不提供后端专用指标。
type kafkaSource struct{}

func (s *kafkaSource) NewReader(conf config.Reader, partition int) (wal.Reader, error) {
	return newReader(conf.Kafka, partition)
}

func (s *kafkaSource) NewWriter(conf config.Writer, partition int) (wal.Writer, error) {
	return newWriter(conf.Kafka, partition)
}

func init() {
	source.RegisterSource(config.SourceTypeKafka, &kafkaSource{})
}
