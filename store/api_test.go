package store

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	storecfg "github.com/amoylan/flash-store/store/config"
	storeerrors "github.com/amoylan/flash-store/store/errors"
	"github.com/amoylan/flash-store/store/index"
	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/schema"
	"github.com/amoylan/flash-store/store/wal"

	_ "github.com/amoylan/flash-store/store/wal/source/noop"
)

// 与 worker 分区对齐：identity % numWorkers == 0
const testIdentity uint64 = 0

// 同一 worker、不同 shard：0 与 16 在 numWorkers=16 时同属 partition 0，且 numShards=64 时 shard 不同
const testIdentityOtherShard uint64 = 16

type testRecord struct {
	V int64
}

var (
	testKind registry.Kind
	fieldV   schema.FieldName = "v"
)

func TestMain(m *testing.M) {
	kind, err := registry.NewKind("unittest", "apitest")
	if err != nil {
		panic(err)
	}
	testKind = kind

	def := registry.For[testRecord](testKind).
		Int64(1, fieldV, func(r *testRecord) *int64 { return &r.V }, schema.Indexed()).
		Build()
	if err := registry.RegisterDef(def); err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewStore(storecfg.DefaultConfigForTest(), nil)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

func TestAPI_SetGetObject(t *testing.T) {
	s := newTestStore(t)
	tx := s.BeginTx(context.Background())
	ev := tx.BeginEvent("order", nil)
	if err := SetObject(ev, testIdentity, testKind, "k1", &testRecord{V: 42}, 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	got, err := GetObject[testRecord](s, testIdentity, testKind, "k1")
	if err != nil {
		t.Fatal(err)
	}
	if got.V != 42 {
		t.Fatalf("V = %d, want 42", got.V)
	}
}

func TestAPI_DeleteObject(t *testing.T) {
	s := newTestStore(t)
	tx := s.BeginTx(context.Background())
	ev := tx.BeginEvent("e", nil)
	if err := SetObject(ev, testIdentity, testKind, "k-del", &testRecord{V: 1}, 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx = s.BeginTx(context.Background())
	ev = tx.BeginEvent("e2", nil)
	if err := DeleteObject(ev, testIdentity, testKind, "k-del"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	_, err := GetObject[testRecord](s, testIdentity, testKind, "k-del")
	if !errors.Is(err, storeerrors.ErrKeyNotFound) {
		t.Fatalf("after delete: err = %v, want ErrKeyNotFound", err)
	}
}

func TestAPI_DeleteObjectIdempotent(t *testing.T) {
	s := newTestStore(t)
	tx := s.BeginTx(context.Background())
	ev := tx.BeginEvent("e", nil)
	if err := DeleteObject(ev, testIdentity, testKind, "no-such-key"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestAPI_SetGetDeleteKV(t *testing.T) {
	s := newTestStore(t)
	tx := s.BeginTx(context.Background())
	ev := tx.BeginEvent("kv", nil)
	if err := SetKV(ev, testIdentity, "kv1", []byte("hello"), 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	b, err := GetKV(s, testIdentity, "kv1")
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "hello" {
		t.Fatalf("value = %q", b)
	}
	if !CheckKVExists(s, testIdentity, "kv1") {
		t.Fatal("CheckKVExists want true")
	}

	tx = s.BeginTx(context.Background())
	ev = tx.BeginEvent("kv2", nil)
	if err := DeleteKV(ev, testIdentity, "kv1"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if CheckKVExists(s, testIdentity, "kv1") {
		t.Fatal("CheckKVExists want false after delete")
	}
	_, err = GetKV(s, testIdentity, "kv1")
	if !errors.Is(err, storeerrors.ErrKeyNotFound) {
		t.Fatalf("GetKV err = %v", err)
	}
}

func TestAPI_TTLTimeWheelEviction(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	s := newTestStore(t)
	tx := s.BeginTx(context.Background())
	ev := tx.BeginEvent("ttl", nil)
	const ttl = 3 * time.Second
	if err := SetKV(ev, testIdentity, "kv-ttl", []byte("x"), ttl); err != nil {
		t.Fatal(err)
	}
	if err := SetObject(ev, testIdentity, testKind, "obj-ttl", &testRecord{V: 1}, ttl); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if !CheckKVExists(s, testIdentity, "kv-ttl") {
		t.Fatal("kv should exist before expiry")
	}
	time.Sleep(5 * time.Second)
	if CheckKVExists(s, testIdentity, "kv-ttl") {
		t.Fatal("kv should be evicted by local timewheel")
	}
	_, err := GetObject[testRecord](s, testIdentity, testKind, "obj-ttl")
	if !errors.Is(err, storeerrors.ErrKeyNotFound) {
		t.Fatalf("object should be evicted: %v", err)
	}
}

func TestAPI_TxRollback(t *testing.T) {
	s := newTestStore(t)
	tx := s.BeginTx(context.Background())
	ev := tx.BeginEvent("e", nil)
	if err := SetObject(ev, testIdentity, testKind, "k-roll", &testRecord{V: 9}, 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	_, err := GetObject[testRecord](s, testIdentity, testKind, "k-roll")
	if !errors.Is(err, storeerrors.ErrKeyNotFound) {
		t.Fatalf("after rollback: err = %v, want ErrKeyNotFound", err)
	}
}

func TestAPI_TxSecondIdentityWrongWorker(t *testing.T) {
	s := newTestStore(t)
	tx := s.BeginTx(context.Background())
	ev1 := tx.BeginEvent("e1", nil)
	if err := SetObject(ev1, testIdentity, testKind, "a", &testRecord{V: 1}, 0); err != nil {
		t.Fatal(err)
	}
	ev2 := tx.BeginEvent("e2", nil)
	err := SetObject(ev2, 1, testKind, "b", &testRecord{V: 2}, 0)
	if err == nil {
		t.Fatal("SetObject with identity on different worker: want error")
	}
}

func TestAPI_MultiEventSameWorkerDifferentShard(t *testing.T) {
	s := newTestStore(t)
	tx := s.BeginTx(context.Background())
	ev1 := tx.BeginEvent("e1", nil)
	if err := SetObject(ev1, testIdentity, testKind, "s0", &testRecord{V: 1}, 0); err != nil {
		t.Fatal(err)
	}
	ev2 := tx.BeginEvent("e2", nil)
	if err := SetObject(ev2, testIdentityOtherShard, testKind, "s16", &testRecord{V: 2}, 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	r0, err := GetObject[testRecord](s, testIdentity, testKind, "s0")
	if err != nil {
		t.Fatal(err)
	}
	r16, err := GetObject[testRecord](s, testIdentityOtherShard, testKind, "s16")
	if err != nil {
		t.Fatal(err)
	}
	if r0.V != 1 || r16.V != 2 {
		t.Fatalf("r0.V=%d r16.V=%d", r0.V, r16.V)
	}
}

func TestAPI_QueryObjectsByIdentity(t *testing.T) {
	s := newTestStore(t)
	tx := s.BeginTx(context.Background())
	ev := tx.BeginEvent("e", nil)
	if err := SetObject(ev, testIdentity, testKind, "q1", &testRecord{V: 100}, 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	hints := []IndexHint{{
		Name: fieldV,
		Ranges: index.Ranges{
			index.NewEqualRange(schema.NewFieldValue(int64(100))),
		},
	}}
	matchAll := Filter[testRecord](func(*registry.RowView) bool { return true })

	objs, err := QueryObjectsByIdentity(s, testIdentity, testKind, hints, matchAll)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 1 || objs[0].V != 100 {
		t.Fatalf("query result = %#v", objs)
	}
}

func TestAPI_GetObjectUnregisteredKind(t *testing.T) {
	s := newTestStore(t)
	other, err := registry.NewKind("unittest", "notregistered")
	if err != nil {
		t.Fatal(err)
	}
	_, err = GetObject[testRecord](s, testIdentity, other, "k")
	if !errors.Is(err, storeerrors.ErrKindNotRegistered) {
		t.Fatalf("err = %v", err)
	}
}

func TestAPI_GetView(t *testing.T) {
	s := newTestStore(t)
	tx := s.BeginTx(context.Background())
	ev := tx.BeginEvent("e", nil)
	if err := SetObject(ev, testIdentity, testKind, "gv", &testRecord{V: 7}, 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	v, err := GetView[testRecord](s, testIdentity, testKind, "gv")
	if err != nil {
		t.Fatal(err)
	}
	n, err := v.GetResult(fieldV).Int64()
	if err != nil {
		t.Fatal(err)
	}
	if n != 7 {
		t.Fatalf("view v = %d, want 7", n)
	}
}

type captureListener struct {
	ch chan WalRecord
}

func (c *captureListener) OnWalSent(ctx context.Context, record WalRecord) {
	select {
	case c.ch <- record:
	default:
	}
}

func TestAPI_WalSentWithNoopWriter(t *testing.T) {
	s := newTestStore(t)
	lis := &captureListener{ch: make(chan WalRecord, 1)}
	if err := s.AddWalSentListener(lis); err != nil {
		t.Fatal(err)
	}

	raw := wal.RawBizData(append([]byte{3}, []byte("biz")...))
	tx := s.BeginTx(context.Background())
	ev := tx.BeginEvent("with-biz", raw)
	if err := SetObject(ev, testIdentity, testKind, "w1", &testRecord{V: 1}, 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	select {
	case rec := <-lis.ch:
		if len(rec.Events) != 1 {
			t.Fatalf("Events len = %d", len(rec.Events))
		}
		if rec.Offset == 0 {
			t.Fatal("noop writer should return non-zero offset")
		}
		bd := rec.Events[0].BizData
		if bd == nil {
			t.Fatal("BizData nil")
		}
		if bd.Version() != 3 || string(bd.Bytes()) != "biz" {
			t.Fatalf("BizData version=%d bytes=%q", bd.Version(), bd.Bytes())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for OnWalSent")
	}
}

// TestAPI_TxCommitNotIdempotent 验证同一个 *Tx 不能 Commit 两次。
func TestAPI_TxCommitNotIdempotent(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.StartReplay(ctx, ReplayOptions{Mode: wal.ReplayToLatest}); err != nil {
		t.Fatalf("StartReplay: %v", err)
	}
	s.WaitReplay()

	// Tx with no events — Commit 标记 committed
	tx := s.BeginTx(context.Background())
	if err := tx.Commit(); err != nil {
		t.Fatalf("first Commit (empty tx): %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, storeerrors.ErrTxAlreadyCommitted) {
		t.Fatalf("second Commit on empty tx: got %v want ErrTxAlreadyCommitted", err)
	}

	// Tx with real event — Commit 也标记 committed
	tx2 := s.BeginTx(context.Background())
	ev := tx2.BeginEvent("e", nil)
	if err := SetObject(ev, testIdentity, testKind, "k1", &testRecord{V: 1}, 0); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("first Commit: %v", err)
	}
	if err := tx2.Commit(); !errors.Is(err, storeerrors.ErrTxAlreadyCommitted) {
		t.Fatalf("second Commit: got %v want ErrTxAlreadyCommitted", err)
	}
}

// TestAPI_AddWalSentListenerAfterStartRejected 验证 Start 之后 AddWalSentListener
// 返回 ErrStoreAlreadyStarted——避免无锁追加 + 无锁遍历的 race，且强制契约：listener
// 注册必须在业务运转开始前完成。
func TestAPI_AddWalSentListenerAfterStartRejected(t *testing.T) {
	s := newTestStore(t)

	// Start 之前可以多次注册
	for i := 0; i < 3; i++ {
		if err := s.AddWalSentListener(&captureListener{ch: make(chan WalRecord, 1)}); err != nil {
			t.Fatalf("Add #%d before Start: %v", i, err)
		}
	}

	// 触发 freeze（StartReplay 会调 freezeListeners）
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.StartReplay(ctx, ReplayOptions{Mode: wal.ReplayToLatest}); err != nil {
		t.Fatalf("StartReplay: %v", err)
	}
	s.WaitReplay()

	// Start 之后注册必须返回 ErrStoreAlreadyStarted
	err := s.AddWalSentListener(&captureListener{ch: make(chan WalRecord, 1)})
	if !errors.Is(err, storeerrors.ErrStoreAlreadyStarted) {
		t.Fatalf("Add after Start: got err=%v want ErrStoreAlreadyStarted", err)
	}

	// StopReplay 之后即使再次 StartReplay，注册依然被拒绝（freeze 是 one-way）
	s.StopReplay()
	err = s.AddWalSentListener(&captureListener{ch: make(chan WalRecord, 1)})
	if !errors.Is(err, storeerrors.ErrStoreAlreadyStarted) {
		t.Fatalf("Add after StopReplay: got err=%v want ErrStoreAlreadyStarted", err)
	}
}
