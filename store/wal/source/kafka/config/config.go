package config

// RequiredAcks 配置 broker 等待 ack 的级别。值与 Kafka 协议字段一致：
//
//	-1 = RequireAll   等所有 ISR
//	 0 = RequireNone  不等 ack（零值；最快但不耐久）
//	 1 = RequireOne   只等 leader
//
// flash-store 的耐久性约束要求线上必须用 RequireAll。该类型刻意不直接
// alias 底层客户端类型，以避免 config 公开 API 被具体实现拴住。
type RequiredAcks int

const (
	RequireNone RequiredAcks = 0
	RequireOne  RequiredAcks = 1
	RequireAll  RequiredAcks = -1
)

// SASLMechanism 选择 SASL 握手机制。仅当 UserName 非空（即启用 SASL）时生效。
//
// 空值不等于"不鉴权"——空值由 writer/reader 解析为 DefaultSASLMechanism
// （SCRAM-SHA-512），与多数托管/类生产 Kafka 的默认开启项一致。要走明文
// 不鉴权就让 UserName 为空，SASL 整段不会被装配。
type SASLMechanism string

const (
	SASLPlain       SASLMechanism = "plain"
	SASLScramSha256 SASLMechanism = "scram-sha-256"
	SASLScramSha512 SASLMechanism = "scram-sha-512"

	// DefaultSASLMechanism 是 SASLMechanism 为空时的解析结果。历史上这里写死
	// PLAIN，但测试/生产 broker 普遍只开 SCRAM，故默认改为 SCRAM-SHA-512，
	// 避免 UNSUPPORTED_SASL_MECHANISM。
	DefaultSASLMechanism = SASLScramSha512
)

type Reader struct {
	UserName string
	Password string
	// SASLMechanism 选择 SASL 机制（plain / scram-sha-256 / scram-sha-512）。
	// 空值解析为 DefaultSASLMechanism。仅在 UserName 非空时生效。
	SASLMechanism SASLMechanism
	Topic         string
	Brokers       []string
}

type Writer struct {
	UserName string
	Password string
	// SASLMechanism 选择 SASL 机制（plain / scram-sha-256 / scram-sha-512）。
	// 空值解析为 DefaultSASLMechanism。仅在 UserName 非空时生效。
	SASLMechanism SASLMechanism
	Topic         string
	Brokers       []string
	// Producer 参数
	RequiredAck    RequiredAcks
	BatchSize      int   // 兼容字段：franz-go 按 bytes + linger 攒批，不再按条数封顶。保留供未来上限化用。
	BatchTimeoutMs int   // producer linger 时间(ms)
	MaxBatchBytes  int64 // 单 batch 最大字节数

	// DisableIdempotent 显式关闭 idempotent producer。
	//
	// 零值（false）= 跟随 RequiredAck：仅当 RequiredAck=RequireAll 时启用 idempotent，
	// 其他 ack 等级一律关闭（franz-go 协议层硬性要求 idempotent + acks=all）。
	// 显式置 true → 即使 RequiredAck=RequireAll 也强制关闭。
	//
	// 关闭 idempotent 失去 broker 端按 producerId+seq 重排/dedup 的能力；
	// 同 partition 内乱序到达不再恢复，上层须自行保证乱序无害。
	DisableIdempotent bool

	// MaxInflightPerBroker 显式设置每 broker 的 in-flight produce 请求上限。
	//
	// 仅在 idempotent **关闭**时生效（idempotent 开启时 franz-go 协议层把上限
	// 锁在 5，且拒绝显式覆盖）。
	// 零值 → franz-go 默认 1（idempotent off）/ 5（idempotent on）。
	// 想用更大值换吞吐必须配合 DisableIdempotent=true，并理解 retry 乱序的风险。
	MaxInflightPerBroker int
}
