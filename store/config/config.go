package config

import (
	"strings"

	snapconfig "github.com/amoylan/flash-store/store/snapshot/config"
	walconfig "github.com/amoylan/flash-store/store/wal/config"
)

type Role int

// Store 角色定义。
//
// Master: 内嵌在业务代码中，负责写 WAL。
//
// Slave 分为两个子角色：
//
//	| 维度             | RoleSnapshotSlave             | RoleReadOnlySlave            |
//	|------------------|-------------------------------|------------------------------|
//	| 数据存储         | schema.Row 结构体（同下）     | schema.Row 结构体             |
//	| 业务 Def 注册    | 不需要（noBusinessDef=true）  | 需要（注册 Def[T]）           |
//	| 索引             | 不建（没 Def 就没 IndexedFields）| 建（查询路径依赖）         |
//	| Snapshot 创建    | 需要                          | 不需要                      |
//	| 对业务包的依赖   | 无（Row 自描述，不 import 业务） | 有（必须注册 Def[T]）       |
//
// 两种 Slave 在 shard 里**都**存解码后的 schema.Row 结构体——Row 自描述，
// 不依赖业务 codec 就能解码。区别只在于 SnapshotSlave 不建索引、不做 Def-
// based 类型校验（行为由 shard.noBusinessDef 标志驱动）。
const (
	RoleUndefined     Role = iota
	RoleMaster             // Master 在 AOS 内，负责写 WAL
	RoleSnapshotSlave      // Slave：仅负责打 snapshot，不 import 业务包、不建索引
	RoleReadOnlySlave      // Slave：仅负责查询/索引，需要注册业务 Def[T]，不打 snapshot
)

func (r Role) String() string {
	switch r {
	case RoleMaster:
		return "master"
	case RoleSnapshotSlave:
		return "snapshot_slave"
	case RoleReadOnlySlave:
		return "readonly_slave"
	}
	return "undefined"
}

// IsSlave 判断角色是否为任意 Slave 变体。
func (r Role) IsSlave() bool {
	return r == RoleSnapshotSlave || r == RoleReadOnlySlave
}

func RoleFromString(s string) Role {
	switch strings.ToLower(s) {
	case "master":
		return RoleMaster
	case "snapshot_slave":
		return RoleSnapshotSlave
	case "readonly_slave":
		return RoleReadOnlySlave
	}
	return RoleUndefined
}

type Config struct {
	Role Role

	Wal walconfig.Config

	// Snapshot 配置（Slave 创建快照、Master/Slave 恢复快照时使用）。
	Snapshot snapconfig.Config `json:"snapshot"`
}

func DefaultConfigForTest() Config {
	return Config{
		Role: RoleMaster,
		Wal: walconfig.Config{
			Writer: walconfig.Writer{
				Source: walconfig.SourceTypeNoop,
				Format: walconfig.WalFormatJson,
			},
			Reader: walconfig.Reader{
				Source: walconfig.SourceTypeNoop,
			},
		},
	}
}
