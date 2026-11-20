package config

import (
	"time"

	fileconf "github.com/amoylan/flash-store/store/wal/source/file/config"
	kafkaconf "github.com/amoylan/flash-store/store/wal/source/kafka/config"
)

// ======================== WalFormat ========================

type WalFormat string

const (
	WalFormatProtobuf WalFormat = "proto"
	WalFormatJson     WalFormat = "json"
)

func (f WalFormat) Bytes() []byte {
	return []byte(f)
}

func NewWalFormat(bytes []byte) WalFormat {
	s := WalFormat(bytes)
	if s == WalFormatJson {
		return WalFormatJson
	}
	return WalFormatProtobuf // 默认也是protobuf
}

// ======================== Config ========================

type SourceType int

const (
	SourceTypeKafka SourceType = 1
	SourceTypeFile  SourceType = 2
	SourceTypeNoop  SourceType = 3
)

func (t SourceType) String() string {
	switch t {
	case SourceTypeKafka:
		return "kafka"
	case SourceTypeFile:
		return "file"
	case SourceTypeNoop:
		return "noop"
	}
	return ""
}

type Writer struct {
	Source SourceType
	Kafka  kafkaconf.Writer
	File   fileconf.Writer
	Format WalFormat

	// Append 重试退避参数
	AppendRetryInitBackoffMs int // 初始退避时间(ms)，默认 20
	AppendRetryMaxBackoffMs  int // 最大退避时间(ms)，默认 2000
}

const (
	DefaultAppendRetryInitBackoffMs = 20
	DefaultAppendRetryMaxBackoffMs  = 2000
)

func (w *Writer) GetAppendRetryInitBackoff() time.Duration {
	return time.Duration(defaultWhenZero(w.AppendRetryInitBackoffMs, DefaultAppendRetryInitBackoffMs)) * time.Millisecond
}

func (w *Writer) GetAppendRetryMaxBackoff() time.Duration {
	return time.Duration(defaultWhenZero(w.AppendRetryMaxBackoffMs, DefaultAppendRetryMaxBackoffMs)) * time.Millisecond
}

type Reader struct {
	Source SourceType
	Kafka  kafkaconf.Reader
	File   fileconf.Reader
}

type Config struct {
	Writer Writer
	Reader Reader
}

// type SenderConfig struct {
// 	WalFormat WalFormat
// 	// seq过期阈值（与最新seq之间的差值）超过这个值的，发送WAL会失败
// 	// 这个值应显著小于接收端 seq cache 缓存窗口大小，这样接收端的 cache 不会有遗漏
// 	SeqOutdatedThreshold int
// }

func defaultWhenZero(val, def int) int {
	if val == 0 {
		return def
	}
	return val
}

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

// type ReceiverConfig struct {
// 	SubscribeChannelSize              int  // 订阅WAL的channel的缓冲区大小
// 	ValidateVersion                   bool // 是否校验版本（如果为true 当kafka header所带版本大于服务自身的store版本时，回放将被阻止）
// 	EventSeqCacheWindowSize           int  // cache 窗口
// 	EventSeqCacheCleanIntervalSeconds int  // cache 清理周期（秒）

// 	DisableBizDataReplayer bool // 只对slave生效 不使用BizDataReplayer （默认情况下是 false）（realtime-checker需要设置为 true）
// }

// // GetSubscribeChannelSize 通过函数get，添加默认值逻辑
// func (c *ReceiverConfig) GetSubscribeChannelSize() int {
// 	return defaultWhenZero(c.SubscribeChannelSize, 100)
// }

// // GetEventSeqCacheWindowSize 通过函数get，添加默认值逻辑
// func (c *ReceiverConfig) GetEventSeqCacheWindowSize() int {
// 	return defaultWhenZero(c.EventSeqCacheWindowSize, DefaultEventSeqCacheWindowSize)
// }

// // GetEventSeqCacheCleanIntervalSeconds 通过函数get，添加默认值逻辑
// func (c *ReceiverConfig) GetEventSeqCacheCleanIntervalSeconds() int {
// 	return defaultWhenZero(c.EventSeqCacheCleanIntervalSeconds, 30)
// }
