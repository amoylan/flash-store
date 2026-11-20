package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/amoylan/flash-store/store/wal/source/kafka/config"
)

// saslOpt 把 (user, pass, mechanism) 解析成一个 kgo.Opt。
//
//   - user == "" → 不启用 SASL，返回 (nil-opt, nil)，调用方应跳过装配。
//   - mech == "" → 解析为 config.DefaultSASLMechanism（SCRAM-SHA-512）。
//   - 未知 mechanism → 返回 error，fail-fast 而不是静默退回 PLAIN。
//
// ok=false 表示无需装配 SASL（user 为空）。
func saslOpt(user, pass string, mech config.SASLMechanism) (opt kgo.Opt, ok bool, err error) {
	if user == "" {
		return nil, false, nil
	}
	if mech == "" {
		mech = config.DefaultSASLMechanism
	}
	switch mech {
	case config.SASLPlain:
		return kgo.SASL(plain.Auth{User: user, Pass: pass}.AsMechanism()), true, nil
	case config.SASLScramSha256:
		return kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha256Mechanism()), true, nil
	case config.SASLScramSha512:
		return kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha512Mechanism()), true, nil
	default:
		return nil, false, fmt.Errorf("kafka: unsupported SASL mechanism %q (valid: %s, %s, %s)",
			mech, config.SASLPlain, config.SASLScramSha256, config.SASLScramSha512)
	}
}
