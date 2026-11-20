package wal

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/schema"
	"github.com/amoylan/flash-store/store/wal/config"
)

type Operation int

const (
	OpSet Operation = iota
	OpDelete
)

// Cmd 是 WAL 里一条对象级写操作。
//
// OpSet：Row 字段为 schema.Row 形态（store 自管序列化，自描述 Field 类型）。
//
//	因为 Row 是自描述的，SnapshotSlave / 无业务 Def 注册的节点也能解码出 Row
//	→ 存进 shard、写进 snapshot，无需知道字段的 Go 语义。
//
// OpDelete：Row 为零值；Kind + Key 用来定位表。
type Cmd struct {
	Op   Operation     `json:"op"`
	Key  string        `json:"key"`
	Kind registry.Kind `json:"kind"`
	Row  schema.Row    `json:"-"` // JSON/proto codec 单独把 Row 打/解码为 bytes
	Ttl  time.Duration `json:"ttl"`
}

// IsSet / IsDelete 是 Cmd.Op 的语法糖，让 listener 侧 switch 更可读。
// 刻意不加涉及 registry/schema 的方法，保持 wal.Cmd"只装数据"的最小职责；
// 需要把 Row 还原成 struct / RowView 的业务走 store.DecodeCmd / store.DecodeCmdAsView。
func (c Cmd) IsSet() bool    { return c.Op == OpSet }
func (c Cmd) IsDelete() bool { return c.Op == OpDelete }

type KvCmd struct {
	Op    Operation     `json:"op"`
	Key   string        `json:"key"`
	Value []byte        `json:"value"`
	Ttl   time.Duration `json:"ttl"`
}

// BizData 业务数据
type BizData interface {
	Bytes() []byte
	Version() uint8
}

// RawBizData 是 BizData 的一个简单实现，直接使用字节切片表示业务数据
// wal 解析的时候，并不知道 BizData 的具体结构
type RawBizData []byte

func (b RawBizData) Bytes() []byte {
	return b[1:]
}

func (b RawBizData) Version() uint8 {
	return b[0]
}

type Event struct {
	EventType string  `json:"event_type"`
	Identity  uint64  `json:"identity"`
	Cmds      []Cmd   `json:"cmds"`
	KvCmds    []KvCmd `json:"kv_cmds"`
	// BizData 业务数据
	BizData BizData `json:"biz_data"`
}

// ========================= WalType =========================

// WalType 标识 WAL 记录的类型。一条 Wal 只承载一种类型的数据。
// WalType 存在于 Header 中，**不**编码进 Data payload。
type WalType int

// 命名约定：
//   - Config 后缀：store 级全局配置，**只通过 partition 0** 单通道传递。所有 Slave 实例的
//     worker-0 replay 都会消费这条 WAL，从而更新本进程的 store 级状态；其他 worker 不必感知。
//
// WalType 写入 Header 进入 Kafka，**显式赋值固定数字**而非 iota——避免新增类型时插入位置变化
// 导致历史 WAL 错乱。当前没有线上数据，但保持显式数字让线协议在代码评审里一眼可见。
const (
	// WalTypeBusiness 业务事件（Events 非空）。
	WalTypeBusiness WalType = 0
	// WalType 1 reserved：曾用于 ack 批量确认（AckEvents），机制已移除。
	// WalType 2 reserved：曾用于 AckTarget 名称广播，机制已移除。
	// WalType 3 reserved：曾用于 SnapshotAckTargets 全局配置同步，机制已移除。
	// **禁止复用**——历史 WAL 里可能仍有该值，复用会造成旧数据被新语义误解析。
	// WalTypeSchemaConfig Master 同步当前注册的所有 Kind 的 schema（Schemas 非空）。
	// store 级配置，仅通过 worker 0 单通道传递。Slave 无业务代码注册，只能靠这条 WAL 知道
	// schema 长什么样；CreateSnapshot 时 Header.Schemas 就从这里拿。整集合替换语义。
	// Master 启动后 PublishSchemas 广播。
	WalTypeSchemaConfig WalType = 4
)

// String 返回 WalType 的稳定字符串表示，供 metrics label / 日志使用。
// 字符串值是 metrics 的稳定 label——改名等价于打断历史时序，请勿轻易调整。
func (t WalType) String() string {
	switch t {
	case WalTypeBusiness:
		return "business"
	case WalTypeSchemaConfig:
		return "schema_config"
	default:
		return "unknown"
	}
}

// ========================= Wal (payload only) =========================

// Wal 是 WAL 记录的有效载荷（payload）。Seq / WalType 等元信息由 Header 携带，
// 不在 Wal 里存储也不由 codec 编解码。
//
// 以下 payload 字段与 WalType 一一对应，同一条 Wal 只填充其中一个。
type Wal struct {
	Events []Event `json:"events,omitempty"`
	// Schemas 用于 WalTypeSchemaConfig：Master 把当前注册的所有 Kind 的
	// schema 描述广播给 Slave。Slave 收到后整集合替换本地缓存。
	Schemas []schema.KindDescriptor `json:"schemas,omitempty"`
}

func (w *Wal) ForEachEvent(f func(event *Event) error) error {
	for i := range w.Events {
		if err := f(&w.Events[i]); err != nil {
			return fmt.Errorf("for each event failed: %w", err)
		}
	}
	return nil
}

// ========================= Header =========================

// Header 携带 WAL 元信息，在 transport 层（Kafka message header / File record header）
// 独立于 Data 存储，可以不 decode payload 就读到。
// 编解码统一走 map[string][]byte，不依赖 json/proto codec。
type Header struct {
	Format  config.WalFormat
	Seq     int64
	WalType WalType
}

// Header map keys — 短名，兼作 Kafka message header key 和 File record header key。
const (
	HeaderKeyFormat  = "f"
	HeaderKeySeq     = "s"
	HeaderKeyWalType = "t"
)

// ToMap 将 Header 编码为 map[string][]byte，适用于 Kafka message headers 和 File header kv 编码。
func (h Header) ToMap() map[string][]byte {
	seq := make([]byte, 8)
	binary.BigEndian.PutUint64(seq, uint64(h.Seq))
	return map[string][]byte{
		HeaderKeyFormat:  h.Format.Bytes(),
		HeaderKeySeq:     seq,
		HeaderKeyWalType: {byte(h.WalType)},
	}
}

// HeaderFromMap 从 map[string][]byte 解码出 Header。缺失字段返错。
func HeaderFromMap(m map[string][]byte) (Header, error) {
	var h Header

	f, ok := m[HeaderKeyFormat]
	if !ok || len(f) == 0 {
		return Header{}, fmt.Errorf("header missing key %q", HeaderKeyFormat)
	}
	h.Format = config.NewWalFormat(f)

	s, ok := m[HeaderKeySeq]
	if !ok || len(s) != 8 {
		return Header{}, fmt.Errorf("header key %q: want 8 bytes, got %d", HeaderKeySeq, len(s))
	}
	h.Seq = int64(binary.BigEndian.Uint64(s))

	t, ok := m[HeaderKeyWalType]
	if !ok || len(t) != 1 {
		return Header{}, fmt.Errorf("header key %q: want 1 byte, got %d", HeaderKeyWalType, len(t))
	}
	h.WalType = WalType(t[0])

	return h, nil
}

// ========================= Records =========================

// Offset 表示WAL记录在存储系统中的位置，可以是文件偏移量、Kafka的offset等，具体含义由不同的Writer实现定义
type Offset = int64

// Seq 表示WAL记录的序列号，通常是一个递增的整数，用于标识唯一的业务事件 （相同的 Seq 可以在幂等检测中跳过）
type Seq = int64

// WriteRecord 是 Writer.Append 的输入。
type WriteRecord struct {
	Header Header
	Data   []byte // codec 编码后的 Wal payload
}

// ReadRecord 是 Reader.Next 的输出。
type ReadRecord struct {
	Header   Header
	Data     []byte // 待 codec 解码的 Wal payload
	Offset   Offset
	Time     time.Time
	AtLatest bool // true 表示当前记录已经是最新的（例如 Kafka 中 Offset+1 >= HighWaterMark）
}

// ========================= Meta =========================

// Meta 由 Replayer 在调用 Applier.Apply 时携带，包含 WAL 记录的位置、时间和 Header 元信息。
type Meta struct {
	Time   time.Time
	Offset Offset
	Header Header
}
