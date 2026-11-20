package registry

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Kind 是 Store 中对象类型的唯一标识符。
// 组合形式为 "$domain.$name"，不带版本号——schema 版本由 Def[T].Version / Row.Version 承担。
type Kind struct {
	domain string
	name   string
}

func NewKind(domain, name string) (Kind, error) {
	if domain == "" {
		return Kind{}, fmt.Errorf("domain cannot be empty")
	}
	if name == "" {
		return Kind{}, fmt.Errorf("name cannot be empty")
	}
	if containsDot(domain) {
		return Kind{}, fmt.Errorf("domain cannot contain dots: %s", domain)
	}
	if containsDot(name) {
		return Kind{}, fmt.Errorf("name cannot contain dots: %s", name)
	}
	return Kind{
		domain: domain,
		name:   name,
	}, nil
}

func (d Kind) String() string {
	return d.domain + "." + d.name
}

func containsDot(s string) bool {
	return strings.Contains(s, ".")
}

// ParseKindFromString 解析 "domain.name" 形式的 Kind 字符串。
// 用于 WAL / snapshot 等 wire format 在 Cmd / ObjectEntry 里持久化 Kind。
func ParseKindFromString(kindStr string) (Kind, error) {
	parts := strings.SplitN(kindStr, ".", 2)
	if len(parts) != 2 {
		return Kind{}, fmt.Errorf("invalid kind format: %s", kindStr)
	}
	domain, name := parts[0], parts[1]
	if domain == "" || name == "" {
		return Kind{}, fmt.Errorf("invalid kind format: %s", kindStr)
	}
	if containsDot(name) {
		return Kind{}, fmt.Errorf("invalid kind format (name contains dot): %s", kindStr)
	}
	return Kind{domain: domain, name: name}, nil
}

// JSON wire 用字符串形式存 Kind。
func (d Kind) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (d *Kind) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	parsed, err := ParseKindFromString(s)
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}
