package store

import (
	"context"
	"errors"
	"testing"
	"time"

	storeerrors "github.com/amoylan/flash-store/store/errors"
	"github.com/amoylan/flash-store/store/wal"
)

// 模拟业务 listener 路径：从 WalSent 记录里拿到 wal.Cmd，
// 用 DecodeCmd / DecodeCmdAsView 还原成 struct 和 RowView。
func TestDecodeCmd_FromListener(t *testing.T) {
	s := newTestStore(t)
	lis := &captureListener{ch: make(chan WalRecord, 1)}
	if err := s.AddWalSentListener(lis); err != nil {
		t.Fatal(err)
	}

	tx := s.BeginTx(context.Background())
	ev := tx.BeginEvent("listener", nil)
	if err := SetObject(ev, testIdentity, testKind, "lk", &testRecord{V: 123}, 0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var rec WalRecord
	select {
	case rec = <-lis.ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for OnWalSent")
	}

	if len(rec.Events) != 1 || len(rec.Events[0].Cmds) != 1 {
		t.Fatalf("unexpected record shape: %+v", rec)
	}
	cmd := rec.Events[0].Cmds[0]

	// 路径 1：DecodeCmd 回填完整 struct。
	var got testRecord
	if err := DecodeCmd(cmd, &got); err != nil {
		t.Fatalf("DecodeCmd: %v", err)
	}
	if got.V != 123 {
		t.Fatalf("decoded V = %d, want 123", got.V)
	}

	// 路径 2：DecodeCmdAsView 只读 RowView。
	view, err := DecodeCmdAsView(cmd)
	if err != nil {
		t.Fatalf("DecodeCmdAsView: %v", err)
	}
	n, err := view.GetResult(fieldV).Int64()
	if err != nil {
		t.Fatalf("RowView.Int64: %v", err)
	}
	if n != 123 {
		t.Fatalf("view V = %d, want 123", n)
	}
}

// OpDelete 时两个 helper 都应返回 ErrCmdNotSetOp，业务按 cmd.Key 走 delete 分支。
func TestDecodeCmd_DeleteOpReturnsSentinel(t *testing.T) {
	delCmd := wal.Cmd{Op: wal.OpDelete, Key: "k", Kind: testKind}

	var out testRecord
	if err := DecodeCmd(delCmd, &out); !errors.Is(err, storeerrors.ErrCmdNotSetOp) {
		t.Fatalf("DecodeCmd on OpDelete: err = %v, want ErrCmdNotSetOp", err)
	}
	if _, err := DecodeCmdAsView(delCmd); !errors.Is(err, storeerrors.ErrCmdNotSetOp) {
		t.Fatalf("DecodeCmdAsView on OpDelete: err = %v, want ErrCmdNotSetOp", err)
	}
}

// Kind 未注册时返回 ErrKindNotRegistered（对 DecodeCmd 和 DecodeCmdAsView 都适用）。
func TestDecodeCmd_UnregisteredKind(t *testing.T) {
	setCmd := wal.Cmd{Op: wal.OpSet, Key: "k"} // zero-value Kind

	var out testRecord
	if err := DecodeCmd(setCmd, &out); !errors.Is(err, storeerrors.ErrKindNotRegistered) {
		t.Fatalf("DecodeCmd unregistered: err = %v", err)
	}
	if _, err := DecodeCmdAsView(setCmd); !errors.Is(err, storeerrors.ErrKindNotRegistered) {
		t.Fatalf("DecodeCmdAsView unregistered: err = %v", err)
	}
}
