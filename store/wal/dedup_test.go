package wal

import (
	"testing"
)

// ---------- 基础行为 ----------

func TestCheckAndMark_FirstMessage(t *testing.T) {
	d := NewSeqDeduplicator(10)
	if dup := d.CheckAndMark(100); dup {
		t.Fatal("first message should not be duplicate")
	}
	if d.maxSeen != 100 {
		t.Fatalf("maxSeen should be 100, got %d", d.maxSeen)
	}
}

func TestCheckAndMark_DuplicateSeq(t *testing.T) {
	d := NewSeqDeduplicator(10)
	d.CheckAndMark(1)
	if dup := d.CheckAndMark(1); !dup {
		t.Fatal("same seq should be duplicate")
	}
}

func TestCheckAndMark_SequentialNoDuplicate(t *testing.T) {
	d := NewSeqDeduplicator(10)
	for i := int64(1); i <= 10; i++ {
		if dup := d.CheckAndMark(i); dup {
			t.Fatalf("seq %d should not be duplicate", i)
		}
	}
}

func TestCheckAndMark_BelowFloorIsDuplicate(t *testing.T) {
	d := NewSeqDeduplicator(5)
	// 填充 seq 1..10, maxSeen=10, floor=10-5=5
	for i := int64(1); i <= 10; i++ {
		d.CheckAndMark(i)
	}
	// seq <= 5 已低于 floor，视为过期重复
	for i := int64(1); i <= 5; i++ {
		if dup := d.CheckAndMark(i); !dup {
			t.Fatalf("seq %d should be below floor and treated as duplicate", i)
		}
	}
	// seq 6..10 在窗口内，是已见过的重复
	for i := int64(6); i <= 10; i++ {
		if dup := d.CheckAndMark(i); !dup {
			t.Fatalf("seq %d should be duplicate (in window)", i)
		}
	}
}

// ---------- 范围删除裁剪 ----------

func TestCheckAndMark_TrimByRange(t *testing.T) {
	d := NewSeqDeduplicator(5)
	// 插入 seq 1..5, maxSeen=5, floor=0 → 无裁剪
	for i := int64(1); i <= 5; i++ {
		d.CheckAndMark(i)
	}
	if len(d.seen) != 5 {
		t.Fatalf("seen should have 5 entries, got %d", len(d.seen))
	}

	// 插入 seq 6 → maxSeen=6, newFloor=1 → seq 1 应被删除
	d.CheckAndMark(6)
	if _, ok := d.seen[1]; ok {
		t.Fatal("seq 1 should have been trimmed")
	}
	if len(d.seen) != 5 {
		t.Fatalf("seen should have 5 entries after trim, got %d", len(d.seen))
	}

	// 插入 seq 7 → maxSeen=7, newFloor=2 → seq 2 应被删除
	d.CheckAndMark(7)
	if _, ok := d.seen[2]; ok {
		t.Fatal("seq 2 should have been trimmed")
	}
}

func TestCheckAndMark_LargeJumpTrimsAll(t *testing.T) {
	d := NewSeqDeduplicator(5)
	// 插入 seq 1..5
	for i := int64(1); i <= 5; i++ {
		d.CheckAndMark(i)
	}
	// 大跳跃: seq 100 → maxSeen=100, newFloor=95 → seq 1..5 全部过期
	d.CheckAndMark(100)
	// seen 中应该只剩 seq 100
	if len(d.seen) != 1 {
		t.Fatalf("seen should have 1 entry after large jump, got %d", len(d.seen))
	}
	if _, ok := d.seen[100]; !ok {
		t.Fatal("seq 100 should be in seen")
	}
}

// ---------- 乱序 ----------

func TestCheckAndMark_OutOfOrder(t *testing.T) {
	d := NewSeqDeduplicator(10)
	// 乱序到达：5, 3, 7, 1, 10
	seqs := []int64{5, 3, 7, 1, 10}
	for _, s := range seqs {
		if dup := d.CheckAndMark(s); dup {
			t.Fatalf("seq %d should not be duplicate on first arrival", s)
		}
	}
	// 全部重复
	for _, s := range seqs {
		if dup := d.CheckAndMark(s); !dup {
			t.Fatalf("seq %d should be duplicate on second arrival", s)
		}
	}
}

func TestCheckAndMark_OutOfOrderWithTrim(t *testing.T) {
	d := NewSeqDeduplicator(5)
	// 先到 seq 1, 2, 3
	d.CheckAndMark(1)
	d.CheckAndMark(2)
	d.CheckAndMark(3)
	// 跳到 seq 8 → maxSeen=8, newFloor=3 → seq 1,2,3 被裁剪
	d.CheckAndMark(8)

	if _, ok := d.seen[1]; ok {
		t.Fatal("seq 1 should have been trimmed")
	}
	if _, ok := d.seen[2]; ok {
		t.Fatal("seq 2 should have been trimmed")
	}
	if _, ok := d.seen[3]; ok {
		t.Fatal("seq 3 should have been trimmed")
	}

	// seq 4 乱序到达 — 在窗口内 (4 > floor=3)，应被接受
	if dup := d.CheckAndMark(4); dup {
		t.Fatal("seq 4 should not be duplicate (within window, never seen)")
	}
	// seq 1 已低于 floor → 过期重复
	if dup := d.CheckAndMark(1); !dup {
		t.Fatal("seq 1 should be below floor")
	}
}

// ---------- 带间隙的 seq 范围删除 ----------

func TestCheckAndMark_GapsInSeq(t *testing.T) {
	d := NewSeqDeduplicator(10)
	// 只插入偶数 seq: 2, 4, 6, 8, 10
	for i := int64(2); i <= 10; i += 2 {
		d.CheckAndMark(i)
	}
	if len(d.seen) != 5 {
		t.Fatalf("expected 5, got %d", len(d.seen))
	}

	// 插入 seq 20 → maxSeen=20, newFloor=10
	// 范围删除 [oldFloor+1, newFloor] = [1, 10]
	// 偶数 2,4,6,8,10 全部被删，奇数 key 不存在 delete 是 no-op
	d.CheckAndMark(20)
	// seen 中应该只剩 seq 20
	if len(d.seen) != 1 {
		t.Fatalf("expected 1 after gap trim, got %d", len(d.seen))
	}
}

// ---------- Snapshot / Restore ----------

func TestSnapshotAndRestore(t *testing.T) {
	d := NewSeqDeduplicator(10)
	for i := int64(1); i <= 8; i++ {
		d.CheckAndMark(i)
	}

	maxSeen, windowSize, seen := d.Snapshot()

	d2 := RestoreSeqDeduplicator(maxSeen, windowSize, seen)

	// 已见过的 seq 仍然是重复
	for i := int64(1); i <= 8; i++ {
		if dup := d2.CheckAndMark(i); !dup {
			t.Fatalf("restored dedup: seq %d should be duplicate", i)
		}
	}
	// 新 seq 不是重复
	if dup := d2.CheckAndMark(9); dup {
		t.Fatal("restored dedup: seq 9 should not be duplicate")
	}
}

func TestRestoreAndTrim(t *testing.T) {
	// 模拟从 snapshot 恢复后继续推进并裁剪
	d := RestoreSeqDeduplicator(10, 5, []int64{6, 7, 8, 9, 10})

	// 插入 seq 13 → maxSeen=13, newFloor=8
	// 范围删除 (oldFloor=5, newFloor=8] → 删除 6, 7, 8
	d.CheckAndMark(13)

	if _, ok := d.seen[6]; ok {
		t.Fatal("seq 6 should have been trimmed after restore+advance")
	}
	if _, ok := d.seen[7]; ok {
		t.Fatal("seq 7 should have been trimmed after restore+advance")
	}
	if _, ok := d.seen[8]; ok {
		t.Fatal("seq 8 should have been trimmed after restore+advance")
	}
	// seq 9, 10, 13 应该还在
	for _, s := range []int64{9, 10, 13} {
		if _, ok := d.seen[s]; !ok {
			t.Fatalf("seq %d should still be in seen", s)
		}
	}
}

// ---------- 边界情况 ----------

func TestCheckAndMark_WindowSizeOne(t *testing.T) {
	d := NewSeqDeduplicator(1)
	d.CheckAndMark(1)
	d.CheckAndMark(2) // maxSeen=2, floor=1 → seq 1 过期
	if dup := d.CheckAndMark(1); !dup {
		t.Fatal("seq 1 should be below floor with windowSize=1")
	}
	if dup := d.CheckAndMark(2); !dup {
		t.Fatal("seq 2 should be duplicate")
	}
	if dup := d.CheckAndMark(3); dup {
		t.Fatal("seq 3 should not be duplicate")
	}
}

func TestCheckAndMark_DefaultWindowSize(t *testing.T) {
	d := NewSeqDeduplicator(0) // 应使用默认值
	if d.windowSize != DefaultDedupWindowSize {
		t.Fatalf("expected default window size %d, got %d", DefaultDedupWindowSize, d.windowSize)
	}
}

func TestCheckAndMark_SeenSizeStable(t *testing.T) {
	// 连续插入远超 windowSize 的 seq，seen 大小应稳定在 windowSize 左右
	ws := int64(100)
	d := NewSeqDeduplicator(ws)
	for i := int64(1); i <= 500; i++ {
		d.CheckAndMark(i)
	}
	if int64(len(d.seen)) > ws+1 {
		t.Fatalf("seen size %d exceeds windowSize %d + 1", len(d.seen), ws)
	}
}
