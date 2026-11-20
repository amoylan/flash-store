package file

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/amoylan/flash-store/store/wal"
	"github.com/amoylan/flash-store/store/wal/source/file/config"
)

type fileWriter struct {
	mu  sync.Mutex
	dir string

	m              *meta
	maxSegmentSize int64

	// 活跃 segment
	segID      uint32
	datFile    *os.File
	idxFile    *os.File
	datSize    int64 // 当前 .dat 文件大小
	localCount int64 // 当前 segment 已写入记录数
}

func newWriter(conf config.Writer) (*fileWriter, error) {
	dir := conf.Dir
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("file writer mkdir: %w", err)
	}

	m, err := readMeta(dir)
	if err != nil {
		return nil, err
	}

	w := &fileWriter{
		dir:            dir,
		m:              m,
		maxSegmentSize: conf.GetMaxSegmentSize(),
	}

	if len(m.Segments) == 0 {
		// 首次写入，创建第一个 segment
		m.Segments = append(m.Segments, segmentMeta{ID: 0, StartOffset: 0, RecordCount: 0})
		if err := writeMeta(dir, m); err != nil {
			return nil, err
		}
	}

	// 打开活跃 segment（最后一个）
	active := m.activeSegment()
	w.segID = active.ID

	if err := w.openSegment(active.ID); err != nil {
		return nil, err
	}

	// 恢复已有记录数
	w.localCount, err = idxRecordCount(dir, active.ID)
	if err != nil {
		w.closeFiles()
		return nil, err
	}

	return w, nil
}

func (w *fileWriter) openSegment(id uint32) error {
	datPath := filepath.Join(w.dir, segDatName(id))
	datFile, err := os.OpenFile(datPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open segment dat: %w", err)
	}

	info, err := datFile.Stat()
	if err != nil {
		datFile.Close()
		return fmt.Errorf("stat segment dat: %w", err)
	}

	idxPath := filepath.Join(w.dir, segIdxName(id))
	idxFile, err := os.OpenFile(idxPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		datFile.Close()
		return fmt.Errorf("open segment idx: %w", err)
	}

	w.datFile = datFile
	w.idxFile = idxFile
	w.datSize = info.Size()
	return nil
}

func (w *fileWriter) Append(_ context.Context, record wal.WriteRecord) (wal.Offset, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	active := w.m.activeSegment()
	globalOffset := active.StartOffset + w.localCount

	// 记录在 .dat 中的起始位置
	bytePos := w.datSize

	// 构建 header: Header 的 map kv 二进制编码
	header, encErr := wal.EncodeHeaderMap(record.Header.ToMap())
	if encErr != nil {
		return 0, fmt.Errorf("encode header map: %w", encErr)
	}

	// 写 header length + header + data length + data 到 .dat
	if err := w.writeUint32(w.datFile, uint32(len(header))); err != nil {
		return 0, err
	}
	if _, err := w.datFile.Write(header); err != nil {
		return 0, fmt.Errorf("write header: %w", err)
	}
	if err := w.writeUint32(w.datFile, uint32(len(record.Data))); err != nil {
		return 0, err
	}
	if _, err := w.datFile.Write(record.Data); err != nil {
		return 0, fmt.Errorf("write data: %w", err)
	}

	// 写 bytePos 到 .idx
	idxEntry := make([]byte, indexEntrySize)
	binary.BigEndian.PutUint64(idxEntry, uint64(bytePos))
	if _, err := w.idxFile.Write(idxEntry); err != nil {
		return 0, fmt.Errorf("write idx: %w", err)
	}

	// sync
	if err := w.datFile.Sync(); err != nil {
		return 0, fmt.Errorf("sync dat: %w", err)
	}
	if err := w.idxFile.Sync(); err != nil {
		return 0, fmt.Errorf("sync idx: %w", err)
	}

	recordSize := int64(4 + len(header) + 4 + len(record.Data))
	w.datSize += recordSize
	w.localCount++

	// 检查是否需要滚动 segment
	if w.datSize >= w.maxSegmentSize {
		if err := w.rotate(); err != nil {
			return 0, fmt.Errorf("rotate: %w", err)
		}
	}

	return globalOffset, nil
}

func (w *fileWriter) rotate() error {
	// 更新当前 segment 的 recordCount
	active := w.m.activeSegment()
	active.RecordCount = w.localCount

	// 新 segment
	newID := w.segID + 1
	newStart := active.StartOffset + active.RecordCount
	w.m.Segments = append(w.m.Segments, segmentMeta{
		ID:          newID,
		StartOffset: newStart,
		RecordCount: 0,
	})

	// 原子写 meta
	if err := writeMeta(w.dir, w.m); err != nil {
		return err
	}

	// 关闭旧文件
	w.datFile.Close()
	w.idxFile.Close()

	// 打开新 segment
	w.segID = newID
	w.localCount = 0
	return w.openSegment(newID)
}

func (w *fileWriter) writeUint32(f *os.File, v uint32) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, v)
	if _, err := f.Write(buf); err != nil {
		return fmt.Errorf("write uint32: %w", err)
	}
	return nil
}

func (w *fileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 更新活跃 segment 的 recordCount 并写入 meta
	if active := w.m.activeSegment(); active != nil && w.localCount > 0 {
		active.RecordCount = w.localCount
		_ = writeMeta(w.dir, w.m)
	}

	return w.closeFiles()
}

func (w *fileWriter) closeFiles() error {
	var firstErr error
	if w.datFile != nil {
		if err := w.datFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if w.idxFile != nil {
		if err := w.idxFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
