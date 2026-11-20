// Command demo 是 flash-store 的可运行最小示例：
// 注册 Kind → 事务写（WAL + 内存）→ 按 key 读 → 索引查询 → Listener 通知。
//
// 运行：go run ./demo（在 store/example 目录下；WAL 走 noop，无外部依赖）
package main

import (
	"context"
	"fmt"

	"github.com/amoylan/flash-store/store"
	"github.com/amoylan/flash-store/store/config"
	"github.com/amoylan/flash-store/store/example"
	"github.com/amoylan/flash-store/store/index"
	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/schema"
	"github.com/shopspring/decimal"
)

func main() {
	ctx := context.Background()

	// noop WAL 便于本地体验；生产用 kafka（acks=all）或 file WAL。
	s, err := store.NewStore(config.DefaultConfigForTest(), nil)
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// Listener：commit 后同步收到业务事件（非阻塞契约见 store/listener.go）。
	sent := make(chan store.WalRecord, 4)
	if err := s.AddWalSentListener(sentListener{ch: sent}); err != nil {
		panic(err)
	}

	// 1. 事务写：业务状态变更 + KV，一个 Tx 一条 WAL。
	identity := uint64(1001)
	if err := s.Transaction(ctx, func(tx *store.Tx) error {
		ev := tx.BeginEvent(example.EventTypeApprovalSubmitOrder, nil)
		if err := store.SetObject(ev, identity, example.OrderKind, "123",
			&example.Order{
				AccountId: 1001,
				OrderId:   123,
				Status:    example.StatusOpen,
				Price:     decimal.NewFromInt(50000),
				Vol:       decimal.NewFromInt(1),
			}, 0); err != nil {
			return err
		}
		return store.SetKV(ev, identity, "demo:version", []byte("v1"), 0)
	}); err != nil {
		panic(err)
	}
	rec := <-sent
	fmt.Printf("wal sent: offset=%d events=%d\n", rec.Offset, len(rec.Events))

	// 2. 按 key 读（纯内存）。
	order, err := store.GetObject[example.Order](s, identity, example.OrderKind, "123")
	if err != nil {
		panic(err)
	}
	fmt.Printf("get: order=%d status=%v price=%s\n", order.OrderId, order.Status, order.Price)

	// 3. 索引查询（status 等值 + 价格过滤）。
	orders, err := store.QueryObjectsByIdentity[example.Order](s, identity, example.OrderKind, []store.IndexHint{
		{
			Name: example.FieldNameStatus,
			Ranges: []index.Range{
				index.NewEqualRange(schema.NewInt32(int32(example.StatusOpen))),
			},
		},
	}, func(view *registry.RowView) bool {
		price, err := view.GetResult(example.FieldNamePrice).Decimal()
		if err != nil {
			return false
		}
		return price.GreaterThan(decimal.NewFromInt(1000))
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("query: status=open && price>1000 → %d order(s)\n", len(orders))

	kv, err := store.GetKV(s, identity, "demo:version")
	if err != nil {
		panic(err)
	}
	fmt.Printf("kv: demo:version=%s\n", kv)
}

// sentListener 捕获 OnWalSent 回调（生产实现须满足非阻塞契约，见 store/listener.go）。
type sentListener struct {
	ch chan store.WalRecord
}

func (l sentListener) OnWalSent(_ context.Context, record store.WalRecord) {
	l.ch <- record
}
