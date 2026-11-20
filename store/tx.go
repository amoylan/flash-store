package store

import (
	"context"
	"fmt"
	"time"

	storeerrors "github.com/amoylan/flash-store/store/errors"
	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/wal"
)

// ========================== TxEvent ==============================

// EventType 事件类型
type EventType string

// TxEvent 单次业务事件：累积 wal.Cmd / wal.KvCmd，提交时与 Type、BizData 一起写入一条 wal.Event。
// 同一 Tx 内所有 Event 必须落在同一 worker（Kafka 分区），可对应不同 shard（identity % numShards）。
type TxEvent struct {
	t       EventType
	bizData wal.BizData

	store   *Store
	txOwner *Tx

	cmds   []wal.Cmd
	kvCmds []wal.KvCmd

	identity uint64
	hasID    bool
}

func (ev *TxEvent) ensureIdentity(id uint64) error {
	p := workerPartition(id)
	if err := ev.txOwner.pinWorker(p); err != nil {
		return err
	}
	if !ev.hasID {
		ev.identity = id
		ev.hasID = true
		return nil
	}
	if ev.identity != id {
		return fmt.Errorf("store: tx event identity mismatch within same event")
	}
	return nil
}

// removeCmdsForKey 去掉本 Event 内同 kind+key 的待提交 cmd，便于合并为最终一条语义。
func (ev *TxEvent) removeCmdsForKey(kind registry.Kind, key string) {
	out := ev.cmds[:0]
	for _, c := range ev.cmds {
		if c.Key == key && c.Kind == kind {
			continue
		}
		out = append(out, c)
	}
	ev.cmds = out
}

func (ev *TxEvent) removeKvCmdForKey(key string) {
	out := ev.kvCmds[:0]
	for _, c := range ev.kvCmds {
		if c.Key == key {
			continue
		}
		out = append(out, c)
	}
	ev.kvCmds = out
}

func (ev *TxEvent) upsertKv(cmd wal.KvCmd) {
	ev.removeKvCmdForKey(cmd.Key)
	ev.kvCmds = append(ev.kvCmds, cmd)
}

// setObject 是泛型 SetObject 的实现：查 Def[T] → Extract 产出 Row → 累积到 TxEvent.cmds。
// 由于 Go 方法不支持类型参数，setObject 接收 any 并在内部做一次类型断言到 *T；
// 上游 SetObject[T] 保证 obj 的类型与 Def[T] 的 T 一致。
func setObject[T any](ev *TxEvent, identity uint64, kind registry.Kind, key string, obj *T, ttl time.Duration) error {
	if err := ev.ensureIdentity(identity); err != nil {
		return err
	}
	def, ok := registry.GetDef[T](kind)
	if !ok {
		return storeerrors.ErrKindNotRegistered
	}
	row := def.Extract(obj)
	ev.removeCmdsForKey(kind, key)
	ev.cmds = append(ev.cmds, wal.Cmd{
		Op:   wal.OpSet,
		Key:  key,
		Kind: kind,
		Row:  row,
		Ttl:  ttl,
	})
	return nil
}

func (ev *TxEvent) setKV(
	identity uint64,
	key string,
	value []byte,
	ttl time.Duration,
) error {
	if err := ev.ensureIdentity(identity); err != nil {
		return err
	}
	ev.upsertKv(wal.KvCmd{
		Op:    wal.OpSet,
		Key:   key,
		Value: append([]byte(nil), value...),
		Ttl:   ttl,
	})
	return nil
}

func (ev *TxEvent) deleteObject(
	identity uint64,
	kind registry.Kind,
	key string,
) error {
	if err := ev.ensureIdentity(identity); err != nil {
		return err
	}
	ev.removeCmdsForKey(kind, key)
	// OpDelete 仅需 key + kind 定位表；Row 为零值。
	ev.cmds = append(ev.cmds, wal.Cmd{
		Op:   wal.OpDelete,
		Key:  key,
		Kind: kind,
	})
	return nil
}

func (ev *TxEvent) deleteKV(identity uint64, key string) error {
	if err := ev.ensureIdentity(identity); err != nil {
		return err
	}
	ev.removeKvCmdForKey(key)
	ev.kvCmds = append(ev.kvCmds, wal.KvCmd{
		Op:  wal.OpDelete,
		Key: key,
	})
	return nil
}

func (ev *TxEvent) toWalEvent() wal.Event {
	return wal.Event{
		EventType: string(ev.t),
		Identity:  ev.identity,
		Cmds:      ev.cmds,
		KvCmds:    ev.kvCmds,
		BizData:   ev.bizData,
	}
}

// =========================== Tx ==============================

// Tx 内所有 TxEvent 必须落在同一 workerPartition（与 Kafka 分区对齐）；shard 由 identity 在 apply 时按 numShards 路由。
type Tx struct {
	ctx    context.Context
	store  *Store
	events []*TxEvent

	pinnedWorker int
	pinnedOk     bool

	// committed 一旦 Commit 成功推进到 worker.Commit 就置 true，防止同一个 *Tx 二次 Commit。
	// 若不防御，二次 Commit 会用新 seq 把同一批 events 再写一次，replay 时双 apply。
	committed bool
}

func (tx *Tx) pinWorker(p int) error {
	if !tx.pinnedOk {
		tx.pinnedWorker = p
		tx.pinnedOk = true
		return nil
	}
	if tx.pinnedWorker != p {
		return fmt.Errorf("store: tx must not span multiple worker partitions (kafka partitions)")
	}
	return nil
}

// BeginEvent 开启一个 TxEvent。跨 Event 的 worker 分区一致性由后续写路径上的 ensureIdentity → pinWorker 保证。
func (tx *Tx) BeginEvent(eventType EventType, bizData wal.BizData) *TxEvent {
	ev := &TxEvent{
		t:       eventType,
		bizData: bizData,
		store:   tx.store,
		txOwner: tx,
	}
	tx.events = append(tx.events, ev)
	return ev
}

// Commit 将本 Tx 中所有带变更的 TxEvent 打成一批 wal.Event，一次性提交到**唯一**的 worker，再 apply（各 Event 内 identity 可不同，但须属于同一 worker）。
// 同一个 *Tx 不可重复 Commit——二次调用返回 ErrTxAlreadyCommitted。
func (tx *Tx) Commit() error {
	if tx.committed {
		return storeerrors.ErrTxAlreadyCommitted
	}
	if len(tx.events) == 0 {
		tx.committed = true
		return nil
	}
	var walEvents []wal.Event
	for _, ev := range tx.events {
		if len(ev.cmds) == 0 && len(ev.kvCmds) == 0 {
			continue
		}
		if !ev.hasID {
			return fmt.Errorf("store: tx event has mutations but no identity")
		}
		p := workerPartition(ev.identity)
		if tx.pinnedOk && p != tx.pinnedWorker {
			return fmt.Errorf("store: commit: event identity maps to worker %d, tx pinned to %d", p, tx.pinnedWorker)
		}
		walEvents = append(walEvents, ev.toWalEvent())
	}
	if len(walEvents) == 0 {
		tx.committed = true
		return nil
	}
	if !tx.pinnedOk {
		return fmt.Errorf("store: commit: worker partition not pinned")
	}
	w := tx.store.workers[tx.pinnedWorker]
	if w == nil {
		return fmt.Errorf("store: worker %d unavailable", tx.pinnedWorker)
	}
	// 标记为 committed 在调用 worker.Commit 之前——即使下层失败，也阻止同一 *Tx 二次 Commit
	// 用新 seq 重写已经写入 WAL 的 events（worker.appendWal 的 seq 复用机制处理同一 seq 重试，
	// 不在这里）。失败时调用方应该新建一个 Tx 重做，或者 Rollback 后放弃。
	tx.committed = true
	return w.Commit(tx.ctx, walEvents)
}

func (tx *Tx) Rollback() error {
	tx.events = nil
	tx.pinnedOk = false
	return nil
}

func workerPartition(identity uint64) int {
	return int(identity % uint64(numWorkers))
}
