package config

// import (
// 	kafkaconf "github.com/amoylan/flash-store/store/wal/sender/kafka/config"
// 	noopconf "github.com/amoylan/flash-store/store/wal/sender/noop/config"
// )

// type SenderType int

// const (
// 	SenderTypeKafka SenderType = 1
// 	SenderTypeNoop  SenderType = 2
// )

// func (t SenderType) String() string {
// 	switch t {
// 	case SenderTypeKafka:
// 		return "kafka"
// 	case SenderTypeNoop:
// 		return "noop"
// 	}
// 	return ""
// }

// type SenderConfig struct {
// 	// Type 发送器类型，如 "kafka"、"noop"
// 	Type SenderType

// 	// Kafka 相关配置
// 	Kafka kafkaconf.SenderConfig

// 	// Noop 相关配置
// 	Noop noopconf.SenderConfig

// 	WalFormat WalFormat
// 	// seq过期阈值（与最新seq之间的差值）超过这个值的，发送WAL会失败
// 	// 这个值应显著小于接收端 seq cache 缓存窗口大小，这样接收端的 cache 不会有遗漏
// 	SeqOutdatedThreshold int
// }

// func (c *SenderConfig) GetFormat() WalFormat {
// 	if c.WalFormat == "" {
// 		return WalFormatProtobuf
// 	}
// 	return c.WalFormat
// }

// // 默认的seq过期阈值 5W，假定1w qps，还有5秒的时间窗口
// const DefaultSeqOutdatedThreshold = 50000

// // 默认的seqCache窗口大小 10W，显著大于 seq 过期阈值，不会漏掉重复的幂等消息
// const DefaultEventSeqCacheWindowSize = DefaultSeqOutdatedThreshold * 2

// func (c *SenderConfig) GetSeqOutdatedThreshold() int {
// 	return defaultWhenZero(c.SeqOutdatedThreshold, DefaultSeqOutdatedThreshold)
// }
