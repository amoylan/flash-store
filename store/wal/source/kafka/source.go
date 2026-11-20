package kafka

import (
	"github.com/amoylan/flash-store/store/wal"
	"github.com/amoylan/flash-store/store/wal/config"
	"github.com/amoylan/flash-store/store/wal/source"
)

// kafka WAL source 基于 github.com/twmb/franz-go 实现。
//
// TODO(metrics): franz-go 没有等价于 kafka-go Writer.Stats() 的 read-and-reset
// 聚合接口；要补回 flashstore_kafka_writer_* 指标需要走 kgo.Hook* 接口
// （HookProduceRecordBuffered / HookProduceBatchWritten / HookBrokerThrottle 等），
// 在 RegisterMetrics 时把 hook 装到 client 上、计数 → Prometheus Counter/Gauge。
// 暂未实现：调用方拿到的 *Writer 没实现 walWriterMetrics 可选接口，worker.go
// 会优雅降级跳过注册。
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
