package snapshot

import (
	"fmt"
	"time"

	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/schema"
	"github.com/amoylan/flash-store/store/snapshot/config"
)

// ErrNoSnapshot is returned by Reader.LatestVersion when no snapshot versions exist for the given name.
var ErrNoSnapshot = fmt.Errorf("no snapshot found")

// ======================== 数据模型 ========================

type Identity = uint64

// ObjectEntry 表示 snapshot 中的一条对象记录（扁平结构，不依赖 store 内部的 table/shard）。
// Row 是 schema.EncodeRow 产出的自描述字节流——snapshot 存 Row，下游按需自解码。
type ObjectEntry struct {
	Kind      registry.Kind `json:"kind"`
	Key       string        `json:"key"`
	Row       []byte        `json:"row"`        // schema.EncodeRow 产出的自描述字节
	ExpiredAt int64         `json:"expired_at"` // Unix 秒，0 表示不过期
}

// KVEntry 表示 snapshot 中的一条 KV 记录
type KVEntry struct {
	Key       string `json:"key"`
	Value     []byte `json:"value"`
	ExpiredAt int64  `json:"expired_at"` // Unix 秒，0 表示不过期
}

// UserData 是 snapshot 中的用户数据部分
type UserData struct {
	Objects []ObjectEntry `json:"objects"` // 对象记录列表
	KVs     []KVEntry     `json:"kvs"`     // KV 记录列表
}

// ======================== ShardSnapshot 每个 shard 的快照数据 ========================
type ShardSnapshot struct {
	ShardID int                   `json:"shard_id"` // shard ID
	Users   map[Identity]UserData `json:"users"`    // 每个 identity 对应的用户数据
}

// ======================== SeqDedupState 去重器快照 ========================

// SeqDedupState 对应 wal.SeqDeduplicator 的可序列化快照
// 恢复后重建 SeqDeduplicator 的滑动窗口，避免跨 snapshot 边界的重复 seq 被误判为新消息
type SeqDedupState struct {
	MaxSeen    int64   `json:"max_seen"`    // 目前见过的最大 seq
	WindowSize int64   `json:"window_size"` // 滑动窗口大小
	Seen       []int64 `json:"seen"`        // 窗口内已见过的 seq 集合
}

// ======================== WorkerSnapshot 每个 worker 的快照数据 ========================

type WorkerSnapshot struct {
	WorkerID  int           `json:"worker_id"`  // worker ID（即 partition）
	Seq       int64         `json:"seq"`        // 当前 WAL 业务序列号（恢复后从此值继续递增）
	WalOffset int64         `json:"wal_offset"` // 快照对应的 WAL offset（恢复后从此 offset+1 开始 replay）
	SeqDedup  SeqDedupState `json:"seq_dedup"`  // 去重器状态快照
}

// ======================== Header ========================
// Header 始终以 JSON 编码存储（不走 Codec），因此 restore 时无需先知道编码格式即可解析。
type Header struct {
	// Version 当前恒写 1，restore 路径**不读**它——保留字段是为了真正进入版本化时不需要破坏性改 schema。
	// 当 snapshot wire 格式发生不兼容演进（新增非可空字段、字段语义改变等）时：
	//   1. 把这里写入的常量从 1 改为新版本号（如 2）；
	//   2. 在 restoreFromSnapshot 早期加一道 `header.Version > supportedVersion → return error`
	//      边界硬化，让老 binary 看到新 snapshot 时早 fail，而不是滑入字段解码失败的乱局。
	// 项目尚未发布，目前没有历史 snapshot 需要兼容，可以放心进入此机制。
	Version    uint32                `json:"version"`     // snapshot 格式版本（reserved；当前未消费）
	Format     config.SnapshotFormat `json:"format"`      // shard/worker 的编码格式（"json" / "proto"）
	CreatedAt  time.Time             `json:"created_at"`  // 快照创建时间
	NumShards  int                   `json:"num_shards"`  // shard 数量，恢复时用于分配 shard
	NumWorkers int                   `json:"num_workers"` // worker 数量，恢复时用于分配 worker

	// Schemas 是 snapshot 写入时刻、进程里已注册的全部 Kind 的 schema 描述。
	// restore 时 Store 对每个 KindDescriptor 调用 schema.CheckSchemaCompat 与当前
	// 代码注册的 Def 对比：发现不兼容的演进（字段类型改了 / 字段 rename / 字段缩减）
	// 就 loud-fail 拒绝启动，避免坏 schema 腐败内存和索引。
	// 老版本 snapshot 没有这字段，解码成 nil，跳过 compat 检查（向后兼容）。
	Schemas []schema.KindDescriptor `json:"schemas,omitempty"`
}

// ======================== Snapshot 全量信息 ========================
type Snapshot struct {
	Header  Header           `json:"header"`  // 快照头部元信息
	Shards  []ShardSnapshot  `json:"shards"`  // 每个 shard 的快照数据
	Workers []WorkerSnapshot `json:"workers"` // 每个 worker 的快照数据
}
