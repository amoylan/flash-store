package file

import (
	"context"
	"fmt"
	"testing"

	"github.com/amoylan/flash-store/store/wal"
	walconfig "github.com/amoylan/flash-store/store/wal/config"
	"github.com/amoylan/flash-store/store/wal/source/file/config"
)

func writeRecords(t *testing.T, w wal.Writer, count int, startSeq int64) []wal.Offset {
	t.Helper()
	offsets := make([]wal.Offset, count)
	for i := 0; i < count; i++ {
		off, err := w.Append(context.Background(), wal.WriteRecord{
			Header: wal.Header{Format: walconfig.WalFormatProtobuf, Seq: startSeq + int64(i), WalType: wal.WalTypeBusiness},
			Data:   []byte(fmt.Sprintf("data-%d", startSeq+int64(i))),
		})
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
		offsets[i] = off
	}
	return offsets
}

func TestBasicWriteAndRead(t *testing.T) {
	dir := t.TempDir()

	w, err := newWriter(config.Writer{Dir: dir})
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	records := []wal.WriteRecord{
		{Header: wal.Header{Format: walconfig.WalFormatProtobuf, Seq: 1, WalType: wal.WalTypeBusiness}, Data: []byte("hello")},
		{Header: wal.Header{Format: walconfig.WalFormatJson, Seq: 2, WalType: wal.WalTypeSchemaConfig}, Data: []byte(`{"schemas":[]}`)},
		{Header: wal.Header{Format: walconfig.WalFormatProtobuf, Seq: 3, WalType: wal.WalTypeBusiness}, Data: []byte("world")},
	}

	offsets := make([]wal.Offset, len(records))
	for i, rec := range records {
		off, err := w.Append(context.Background(), rec)
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
		offsets[i] = off
	}
	w.Close()

	// offset 应该是连续的 0, 1, 2
	for i, off := range offsets {
		if off != int64(i) {
			t.Fatalf("offset[%d]: want %d, got %d", i, i, off)
		}
	}

	// 从头读
	r, err := newReader(config.Reader{Dir: dir})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	if err := r.Seek(context.Background(), 0); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	for i, rec := range records {
		got, err := r.Next(context.Background())
		if err != nil {
			t.Fatalf("Next[%d]: %v", i, err)
		}
		if got.Offset != int64(i) {
			t.Errorf("record[%d] offset: want %d, got %d", i, i, got.Offset)
		}
		if got.Header.Seq != rec.Header.Seq {
			t.Errorf("record[%d] seq: want %d, got %d", i, rec.Header.Seq, got.Header.Seq)
		}
		if got.Header.Format != rec.Header.Format {
			t.Errorf("record[%d] format: want %s, got %s", i, rec.Header.Format, got.Header.Format)
		}
		if got.Header.WalType != rec.Header.WalType {
			t.Errorf("record[%d] walType: want %d, got %d", i, rec.Header.WalType, got.Header.WalType)
		}
		if string(got.Data) != string(rec.Data) {
			t.Errorf("record[%d] data: want %q, got %q", i, rec.Data, got.Data)
		}
	}

	// 读到结尾
	_, err = r.Next(context.Background())
	if err != wal.ErrEndOfWAL {
		t.Fatalf("expected ErrEndOfWAL, got %v", err)
	}

	r.Close()
}

func TestSeekToMiddle(t *testing.T) {
	dir := t.TempDir()

	w, err := newWriter(config.Writer{Dir: dir})
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	writeRecords(t, w, 5, 10)
	w.Close()

	r, err := newReader(config.Reader{Dir: dir})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer r.Close()

	// Seek 到 offset 2（第 3 条记录，seq=12）
	if err := r.Seek(context.Background(), 2); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	rec, err := r.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if rec.Offset != 2 {
		t.Errorf("offset: want 2, got %d", rec.Offset)
	}
	if rec.Header.Seq != 12 {
		t.Errorf("seq: want 12, got %d", rec.Header.Seq)
	}
}

func TestAtLatest(t *testing.T) {
	dir := t.TempDir()

	w, err := newWriter(config.Writer{Dir: dir})
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}
	writeRecords(t, w, 3, 1)
	w.Close()

	r, err := newReader(config.Reader{Dir: dir})
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer r.Close()

	if err := r.Seek(context.Background(), 0); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	for i := 0; i < 2; i++ {
		rec, _ := r.Next(context.Background())
		if rec.AtLatest {
			t.Errorf("record[%d] should not be AtLatest", i)
		}
	}

	rec, _ := r.Next(context.Background())
	if !rec.AtLatest {
		t.Error("last record should be AtLatest")
	}
}

func TestAppendToExisting(t *testing.T) {
	dir := t.TempDir()

	// 第一轮写入
	w1, _ := newWriter(config.Writer{Dir: dir})
	writeRecords(t, w1, 3, 1)
	w1.Close()

	// 第二轮写入（重新打开）
	w2, _ := newWriter(config.Writer{Dir: dir})
	offsets := writeRecords(t, w2, 2, 100)
	w2.Close()

	// 新记录的 offset 应该从 3 开始
	if offsets[0] != 3 || offsets[1] != 4 {
		t.Fatalf("offsets: want [3, 4], got %v", offsets)
	}

	// 读全部 5 条
	r, _ := newReader(config.Reader{Dir: dir})
	defer r.Close()
	r.Seek(context.Background(), 0)

	for i := int64(0); i < 5; i++ {
		rec, err := r.Next(context.Background())
		if err != nil {
			t.Fatalf("Next[%d]: %v", i, err)
		}
		if rec.Offset != i {
			t.Errorf("record[%d] offset: want %d, got %d", i, i, rec.Offset)
		}
	}
}

func TestSegmentRotation(t *testing.T) {
	dir := t.TempDir()

	// 使用很小的 MaxSegmentSize 触发滚动
	w, err := newWriter(config.Writer{Dir: dir, MaxSegmentSize: 50})
	if err != nil {
		t.Fatalf("newWriter: %v", err)
	}

	// 写入足够多的记录触发滚动
	offsets := writeRecords(t, w, 10, 1)
	w.Close()

	// 验证 offset 仍然连续
	for i, off := range offsets {
		if off != int64(i) {
			t.Fatalf("offset[%d]: want %d, got %d", i, i, off)
		}
	}

	// 验证 meta 有多个 segment
	m, err := readMeta(dir)
	if err != nil {
		t.Fatalf("readMeta: %v", err)
	}
	if len(m.Segments) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(m.Segments))
	}

	// 顺序读全部记录
	r, _ := newReader(config.Reader{Dir: dir})
	defer r.Close()
	r.Seek(context.Background(), 0)

	for i := 0; i < 10; i++ {
		rec, err := r.Next(context.Background())
		if err != nil {
			t.Fatalf("Next[%d]: %v", i, err)
		}
		if rec.Offset != int64(i) {
			t.Errorf("record[%d] offset: want %d, got %d", i, i, rec.Offset)
		}
		if rec.Header.Seq != int64(i+1) {
			t.Errorf("record[%d] seq: want %d, got %d", i, i+1, rec.Header.Seq)
		}
	}
}

func TestSeekAcrossSegments(t *testing.T) {
	dir := t.TempDir()

	w, _ := newWriter(config.Writer{Dir: dir, MaxSegmentSize: 50})
	writeRecords(t, w, 10, 1)
	w.Close()

	m, _ := readMeta(dir)
	if len(m.Segments) < 2 {
		t.Fatal("need multiple segments for this test")
	}

	// 找到一个非空的 segment（非第一个）来 Seek
	var seekOffset int64
	found := false
	for i := len(m.Segments) - 1; i >= 1; i-- {
		if m.Segments[i].RecordCount > 0 {
			seekOffset = m.Segments[i].StartOffset
			found = true
			break
		}
	}
	if !found {
		// 如果倒数第二个有记录，seek 到它
		seekOffset = m.Segments[1].StartOffset
	}

	r, _ := newReader(config.Reader{Dir: dir})
	defer r.Close()

	if err := r.Seek(context.Background(), seekOffset); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	rec, err := r.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if rec.Offset != seekOffset {
		t.Errorf("offset: want %d, got %d", seekOffset, rec.Offset)
	}
	// seq should be seekOffset+1 (since seq starts from 1)
	if rec.Header.Seq != seekOffset+1 {
		t.Errorf("seq: want %d, got %d", seekOffset+1, rec.Header.Seq)
	}
}

func TestSlaveFollowMode(t *testing.T) {
	dir := t.TempDir()

	// Writer 先写 3 条
	w, _ := newWriter(config.Writer{Dir: dir})
	writeRecords(t, w, 3, 1)

	// Reader 从头读
	r, _ := newReader(config.Reader{Dir: dir})
	defer r.Close()
	r.Seek(context.Background(), 0)

	for i := 0; i < 3; i++ {
		_, err := r.Next(context.Background())
		if err != nil {
			t.Fatalf("Next[%d]: %v", i, err)
		}
	}

	// 读到末尾，返回 ErrEndOfWAL
	_, err := r.Next(context.Background())
	if err != wal.ErrEndOfWAL {
		t.Fatalf("expected ErrEndOfWAL, got %v", err)
	}

	// Writer 继续写入 2 条
	writeRecords(t, w, 2, 100)

	// Reader 能读到新数据
	rec, err := r.Next(context.Background())
	if err != nil {
		t.Fatalf("Next after new write: %v", err)
	}
	if rec.Offset != 3 {
		t.Errorf("offset: want 3, got %d", rec.Offset)
	}
	if rec.Header.Seq != 100 {
		t.Errorf("seq: want 100, got %d", rec.Header.Seq)
	}

	w.Close()
}

func TestSlaveFollowAcrossRotation(t *testing.T) {
	dir := t.TempDir()

	// 用小 segment size，writer 写到接近满
	w, _ := newWriter(config.Writer{Dir: dir, MaxSegmentSize: 100})

	// 写入直到触发第一次滚动
	var totalWritten int
	for {
		writeRecords(t, w, 1, int64(totalWritten+1))
		totalWritten++
		m, _ := readMeta(dir)
		if len(m.Segments) >= 2 {
			break
		}
		if totalWritten > 100 {
			t.Fatal("rotation not triggered")
		}
	}

	// Reader 从 0 读全部
	r, _ := newReader(config.Reader{Dir: dir})
	defer r.Close()
	r.Seek(context.Background(), 0)

	for i := 0; i < totalWritten; i++ {
		rec, err := r.Next(context.Background())
		if err != nil {
			t.Fatalf("Next[%d]: %v", i, err)
		}
		if rec.Offset != int64(i) {
			t.Errorf("record[%d] offset: want %d, got %d", i, i, rec.Offset)
		}
	}

	// 此时读到末尾
	_, err := r.Next(context.Background())
	if err != wal.ErrEndOfWAL {
		t.Fatalf("expected ErrEndOfWAL, got %v", err)
	}

	// Writer 继续写（可能在新 segment）
	writeRecords(t, w, 2, int64(totalWritten+1))

	// Reader 继续读（跨 segment 自动切换）
	for i := 0; i < 2; i++ {
		rec, err := r.Next(context.Background())
		if err != nil {
			t.Fatalf("Next after rotation[%d]: %v", i, err)
		}
		if rec.Offset != int64(totalWritten+i) {
			t.Errorf("offset: want %d, got %d", totalWritten+i, rec.Offset)
		}
	}

	w.Close()
}

func TestReaderClosed(t *testing.T) {
	dir := t.TempDir()

	w, _ := newWriter(config.Writer{Dir: dir})
	writeRecords(t, w, 1, 1)
	w.Close()

	r, _ := newReader(config.Reader{Dir: dir})
	r.Seek(context.Background(), 0)
	r.Close()

	_, err := r.Next(context.Background())
	if err != wal.ErrReaderClosed {
		t.Fatalf("expected ErrReaderClosed, got %v", err)
	}
}

func TestMetaAtomicWrite(t *testing.T) {
	dir := t.TempDir()

	m := &meta{
		Segments: []segmentMeta{
			{ID: 0, StartOffset: 0, RecordCount: 100},
			{ID: 1, StartOffset: 100, RecordCount: 0},
		},
	}

	if err := writeMeta(dir, m); err != nil {
		t.Fatalf("writeMeta: %v", err)
	}

	m2, err := readMeta(dir)
	if err != nil {
		t.Fatalf("readMeta: %v", err)
	}
	if len(m2.Segments) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(m2.Segments))
	}
	if m2.Segments[0].RecordCount != 100 {
		t.Errorf("recordCount: want 100, got %d", m2.Segments[0].RecordCount)
	}
	if m2.Segments[1].StartOffset != 100 {
		t.Errorf("startOffset: want 100, got %d", m2.Segments[1].StartOffset)
	}
}
