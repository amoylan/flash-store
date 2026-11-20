package kafka

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/amoylan/flash-store/store/wal"
	"github.com/amoylan/flash-store/store/wal/source/kafka/config"
)

// envKafkaDebugLog 设为 "1" 时给 kafka reader 挂上 franz-go 的 debug 级日志，
// 把 fetch / metadata / broker 连接的内部行为打到 stderr。用于诊断 PollFetches
// 卡住（如 fetch 不返回、可重试错误被静默重试、leader/SASL re-auth 问题）等
// "鉴权和 ListOffsets 正常但 Fetch 拉不动" 的场景。部署侧设 env 即可开关，无需重编译。
const envKafkaDebugLog = "KAFKA_DEBUG_LOG"

// pendingRec 把 record 跟它所属 fetch 的 HighWatermark 一起保留，
// 用于在 Next 里填写 ReadRecord.AtLatest——franz-go 把 HWM 放在
// FetchTopicPartition 上，单条 Record 不带 HWM 字段。
type pendingRec struct {
	rec *kgo.Record
	hwm int64
}

type Reader struct {
	cl        *kgo.Client
	topic     string
	partition int32

	started bool // 是否已经 AddConsumePartitions
	noData  bool // Seek 后发现 offset >= HWM，当前无数据可读
	pending []pendingRec
}

func newReader(conf config.Reader, partition int) (*Reader, error) {
	if partition < 0 || partition > math.MaxInt32 {
		return nil, fmt.Errorf("kafka reader: partition %d out of int32 range", partition)
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(conf.Brokers...),
		// 不在构造时绑定 ConsumePartitions——Seek 时再 AddConsumePartitions，
		// 这样语义和原 kafka-go Reader.SetOffset 对齐：构造 → Seek → Next。
	}
	if os.Getenv(envKafkaDebugLog) == "1" {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
	}
	if opt, ok, err := saslOpt(conf.UserName, conf.Password, conf.SASLMechanism); err != nil {
		return nil, fmt.Errorf("kafka reader: %w", err)
	} else if ok {
		opts = append(opts, opt)
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka reader: new client: %w", err)
	}

	return &Reader{
		cl:        cl,
		topic:     conf.Topic,
		partition: int32(partition),
	}, nil
}

// Seek 定位到指定 offset，同时查询当前 HWM 来判断是否有数据可读。
// ctx 透传给 readHighWaterMark 的 ListOffsets 网络往返——broker 卡住时受 replay
// 的 deadline / cancel 约束，不再无视超时永久阻塞。
func (r *Reader) Seek(ctx context.Context, offset wal.Offset) error {
	hwm, err := r.readHighWaterMark(ctx)
	if err != nil {
		return err
	}
	r.noData = offset >= hwm

	at := kgo.NewOffset().At(offset)
	if !r.started {
		r.cl.AddConsumePartitions(map[string]map[int32]kgo.Offset{
			r.topic: {r.partition: at},
		})
		r.started = true
	} else {
		// 已经在消费 → SetOffsets 重定位。Epoch=-1 让 franz-go 忽略 epoch 校验。
		r.cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{
			r.topic: {r.partition: {Epoch: -1, Offset: offset}},
		})
	}
	// Seek 重置后丢弃旧 buffer，避免返回 < offset 的陈旧 record。
	r.pending = nil
	return nil
}

// Next 读取下一条记录。
//   - 分区为空 / Seek 超过 HWM → ErrEndOfWAL
//   - Reader 被 Close → ErrReaderClosed
//   - 正常返回记录，AtLatest 表示当前是否已追上最新
func (r *Reader) Next(ctx context.Context) (*wal.ReadRecord, error) {
	if r.noData {
		r.noData = false
		return nil, wal.ErrEndOfWAL
	}

	for len(r.pending) == 0 {
		fetches := r.cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil, wal.ErrReaderClosed
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// 单 partition 消费 → 只有一个 partition 的 fetch。
		// EachPartition 拿 HWM；EachError 单独看错误。
		var fetchErr error
		fetches.EachError(func(_ string, _ int32, err error) {
			if fetchErr == nil {
				fetchErr = err
			}
		})
		if fetchErr != nil && !errors.Is(fetchErr, context.Canceled) && !errors.Is(fetchErr, context.DeadlineExceeded) {
			// kerr.OffsetOutOfRange 等可恢复错误向上抛，让调用方决定怎么处理。
			return nil, fetchErr
		}
		fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
			hwm := ftp.HighWatermark
			for _, rec := range ftp.Records {
				r.pending = append(r.pending, pendingRec{rec: rec, hwm: hwm})
			}
		})
	}

	p := r.pending[0]
	r.pending = r.pending[1:]

	headerMap := make(map[string][]byte, len(p.rec.Headers))
	for _, h := range p.rec.Headers {
		headerMap[h.Key] = h.Value
	}
	header, err := wal.HeaderFromMap(headerMap)
	if err != nil {
		return nil, err
	}

	return &wal.ReadRecord{
		Header:   header,
		Data:     p.rec.Value,
		Offset:   p.rec.Offset,
		Time:     p.rec.Timestamp,
		AtLatest: p.rec.Offset+1 >= p.hwm,
	}, nil
}

func (r *Reader) Close() error {
	r.cl.Close()
	return nil
}

// readHighWaterMark 用 kadm 查询当前 partition 的 HWM（下一条会被分配的 offset）。
// kadm 共享底层 kgo.Client 的连接和鉴权——不需要单独建 dialer。
// ctx 来自 replay：ListOffsets 是一次 broker 往返，必须受 replay deadline / cancel
// 约束；用 context.Background() 会让 broker 卡住时无视 replay 超时永久阻塞。
func (r *Reader) readHighWaterMark(ctx context.Context) (int64, error) {
	admin := kadm.NewClient(r.cl)
	offsets, err := admin.ListEndOffsets(ctx, r.topic)
	if err != nil {
		return 0, fmt.Errorf("kafka reader: list end offsets: %w", err)
	}
	o, ok := offsets.Lookup(r.topic, r.partition)
	if !ok {
		return 0, fmt.Errorf("kafka reader: no end offset for %s/%d", r.topic, r.partition)
	}
	if o.Err != nil {
		return 0, fmt.Errorf("kafka reader: end offset for %s/%d: %w", r.topic, r.partition, o.Err)
	}
	return o.Offset, nil
}
