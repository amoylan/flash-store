package wal

import (
	"context"
	"testing"
	"time"

	"github.com/amoylan/flash-store/store/wal/config"
)

// testReplayFormat 是本测试专用的 WAL format，注册一个永远 decode 成空 Wal 的 codec，
// 让 replay 循环走到 apply 而不依赖 json/proto 真实编码。
const testReplayFormat config.WalFormat = "test-replay"

type emptyDecodeCodec struct{}

func (emptyDecodeCodec) Encode(*Wal) ([]byte, error) { return nil, nil }
func (emptyDecodeCodec) Decode([]byte) (*Wal, error) { return &Wal{}, nil }

func init() { RegisterWalCodec(testReplayFormat, emptyDecodeCodec{}) }

// scriptedReader 按脚本顺序返回 records；脚本耗尽后模拟 kafka reader 的行为——
// 在 HWM 处阻塞等待新消息，直到 ctx 被取消才返回 ctx.Err()。
// 这正是"未在 AtLatest 处收尾"会导致 ReplayToLatest 卡死到超时的根因路径。
type scriptedReader struct {
	recs []*ReadRecord
	i    int
}

func (r *scriptedReader) Seek(_ context.Context, _ Offset) error { return nil }

func (r *scriptedReader) Next(ctx context.Context) (*ReadRecord, error) {
	if r.i < len(r.recs) {
		rec := r.recs[r.i]
		r.i++
		return rec, nil
	}
	// 脚本耗尽：模拟 kafka reader 在 HWM 处阻塞，仅 ctx 取消时返回。
	<-ctx.Done()
	return nil, ctx.Err()
}

func (r *scriptedReader) Close() error { return nil }

type recordingApplier struct {
	applied []int64 // 成功 apply 的 offset
}

func (a *recordingApplier) Apply(_ context.Context, _ Wal, meta Meta) error {
	a.applied = append(a.applied, meta.Offset)
	return nil
}

func rec(seq, offset int64, atLatest bool) *ReadRecord {
	return &ReadRecord{
		Header:   Header{Format: testReplayFormat, Seq: seq, WalType: WalTypeBusiness},
		Offset:   offset,
		AtLatest: atLatest,
	}
}

// runWithGuard 在独立 goroutine 跑 Run，guard 时间内必须返回，否则判定卡死。
func runWithGuard(t *testing.T, r *Replayer, opts ReplayOptions, guard time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), guard)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, opts) }()
	select {
	case err := <-done:
		return err
	case <-time.After(guard + time.Second):
		t.Fatal("Run did not return within guard window — replay hung")
		return nil
	}
}

// TestRun_TailDuplicateTerminates 是核心回归用例：当分区最后一条记录是重复 seq
// （多 master 并发写产生的队尾重复），ReplayToLatest 必须照常在 AtLatest 处收尾，
// 而不是跳过判断后回到 Next 阻塞到 ctx 超时。
func TestRun_TailDuplicateTerminates(t *testing.T) {
	reader := &scriptedReader{recs: []*ReadRecord{
		rec(1, 0, false), // 首见 seq=1
		rec(1, 1, true),  // 队尾重复 seq=1，且 AtLatest
	}}
	applier := &recordingApplier{}
	r := NewReplayer(reader, applier)

	// guard 远小于"卡死才会触发"的时间窗：修复后应近乎立即返回。
	err := runWithGuard(t, r, ReplayOptions{Mode: ReplayToLatest, DedupWindowSize: 16}, 2*time.Second)
	if err != nil {
		t.Fatalf("ReplayToLatest should terminate cleanly on tail-duplicate, got err: %v", err)
	}
	if len(applier.applied) != 1 || applier.applied[0] != 0 {
		t.Fatalf("expected exactly offset 0 applied, got %v", applier.applied)
	}
}

// TestRun_NormalTailTerminates 反向兜底：正常（非重复）队尾记录依旧正常收尾。
func TestRun_NormalTailTerminates(t *testing.T) {
	reader := &scriptedReader{recs: []*ReadRecord{
		rec(1, 0, false),
		rec(2, 1, true), // 队尾非重复 + AtLatest
	}}
	applier := &recordingApplier{}
	r := NewReplayer(reader, applier)

	err := runWithGuard(t, r, ReplayOptions{Mode: ReplayToLatest, DedupWindowSize: 16}, 2*time.Second)
	if err != nil {
		t.Fatalf("ReplayToLatest should terminate on normal tail, got err: %v", err)
	}
	if len(applier.applied) != 2 {
		t.Fatalf("expected offsets 0 and 1 applied, got %v", applier.applied)
	}
}
