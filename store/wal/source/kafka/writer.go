package kafka

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/amoylan/flash-store/store/wal"
	"github.com/amoylan/flash-store/store/wal/source/kafka/config"
)

type appendResult struct {
	offset wal.Offset
	err    error
}

type Writer struct {
	cl        *kgo.Client
	topic     string
	partition int32
}

func newWriter(conf config.Writer, partition int) (*Writer, error) {
	if partition < 0 || partition > math.MaxInt32 {
		return nil, fmt.Errorf("kafka writer: partition %d out of int32 range", partition)
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(conf.Brokers...),
		kgo.DefaultProduceTopic(conf.Topic),
		// ManualPartitioner: Record.Partition 字段直接决定落点，
		// 保留原 fixedPartitionBalancer 的语义——每个 *Writer 固定写一个 partition。
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(mapRequiredAcks(conf.RequiredAck)),
	}

	// idempotent producer 决策：
	//   - franz-go 默认开启；开启时 broker 按 producerId+seq 重排/dedup，
	//     max in-flight 锁在 5（broker v1+），且禁止显式覆盖。
	//   - 协议层要求 acks=all——非 RequireAll 时强制关闭，否则 NewClient 报错。
	//   - 用户显式 DisableIdempotent=true 时遵从用户意图（一般为了换更大 in-flight
	//     的吞吐，业务侧自己兜底乱序）。
	idempotent := !conf.DisableIdempotent && conf.RequiredAck == config.RequireAll
	if !idempotent {
		opts = append(opts, kgo.DisableIdempotentWrite())
		// MaxInflightPerBroker 仅在 idempotent 关闭时可设——idempotent 开启时
		// franz-go 的校验会拒绝任何对此 option 的显式调用。
		if conf.MaxInflightPerBroker > 0 {
			opts = append(opts, kgo.MaxProduceRequestsInflightPerBroker(conf.MaxInflightPerBroker))
		}
	}

	if conf.BatchTimeoutMs > 0 {
		opts = append(opts, kgo.ProducerLinger(time.Duration(conf.BatchTimeoutMs)*time.Millisecond))
	}
	if conf.MaxBatchBytes > 0 {
		// franz-go 的 ProducerBatchMaxBytes 是 int32（Kafka 协议字段就是 int32），
		// 这里 clamp 一下避免悄悄截断成负数。
		bytes := conf.MaxBatchBytes
		if bytes > math.MaxInt32 {
			bytes = math.MaxInt32
		}
		opts = append(opts, kgo.ProducerBatchMaxBytes(int32(bytes)))
	}

	if opt, ok, err := saslOpt(conf.UserName, conf.Password, conf.SASLMechanism); err != nil {
		return nil, fmt.Errorf("kafka writer: %w", err)
	} else if ok {
		opts = append(opts, opt)
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka writer: new client: %w", err)
	}

	return &Writer{
		cl:        cl,
		topic:     conf.Topic,
		partition: int32(partition),
	}, nil
}

// Append 将一条 WAL 记录写入底层 kafka client。
//
// 并发与批处理：本方法是 goroutine-safe 的；多个 worker.Commit 并发调用会被 franz-go
// 内部按 partition 攒批（ProducerLinger + ProducerBatchMaxBytes），同 partition 内
// 最多 5 个 in-flight batch（idempotent producer 默认值），由 broker 按 producerId+seq
// 重排/dedup，retry 乱序到达自动恢复——见 docs/kafka-producer-concurrency.md。
func (w *Writer) Append(ctx context.Context, record wal.WriteRecord) (wal.Offset, error) {
	// 早期 ctx 检查：避免在已取消的 ctx 下仍把消息送入 producer 队列。
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	headerMap := record.Header.ToMap()
	kafkaHeaders := make([]kgo.RecordHeader, 0, len(headerMap))
	for k, v := range headerMap {
		kafkaHeaders = append(kafkaHeaders, kgo.RecordHeader{Key: k, Value: v})
	}

	rec := &kgo.Record{
		Topic:     w.topic,
		Partition: w.partition,
		Value:     record.Data,
		Headers:   kafkaHeaders,
	}

	resultCh := make(chan appendResult, 1)
	// 用 context.Background 调用 Produce：消息一旦入队，必须等到明确结果。
	// 若在此处响应 caller ctx.Done，调用方会以为写入失败并用新 seq 重试，
	// 而原消息仍在途——产生 seq 不同但业务内容相同的孤儿消息，无法被
	// SeqDeduplicator 剔除。franz-go 内部 retry 耗尽后会通过 callback 回报真实错误，
	// 不会永久阻塞。
	w.cl.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
		if err != nil {
			resultCh <- appendResult{err: err}
			return
		}
		resultCh <- appendResult{offset: r.Offset}
	})

	res := <-resultCh
	return res.offset, res.err
}

func (w *Writer) Close() error {
	w.cl.Close()
	return nil
}

// mapRequiredAcks 把 config.RequiredAcks 翻译成 franz-go 的 kgo.Acks。
//
// 配置值刻意与 Kafka 协议字段同义（-1 / 0 / 1），所以这里是字面 1-1 映射。
// 未知值兜底用 NoAck——零值语义与底层 kafka-go 时一致（kafka.RequireNone == 0）。
func mapRequiredAcks(a config.RequiredAcks) kgo.Acks {
	switch a {
	case config.RequireAll:
		return kgo.AllISRAcks()
	case config.RequireOne:
		return kgo.LeaderAck()
	default:
		return kgo.NoAck()
	}
}
