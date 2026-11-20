package file

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/amoylan/flash-store/store/wal"
	"github.com/amoylan/flash-store/store/wal/source/file/config"
)

type fileReader struct {
	mu     sync.Mutex
	dir    string
	closed bool

	// 当前读取位置
	curSegIdx   int      // 当前 segment 在 meta.Segments 中的下标
	curSegID    uint32   // 当前 segment ID
	curDatFile  *os.File // 当前 segment .dat 文件
	curOffset   int64    // 当前全局 offset（下一条要读的记录序号）
	localOffset int64    // 当前 segment 内的本地 offset
}

func newReader(conf config.Reader) (*fileReader, error) {
	return &fileReader{
		dir: conf.Dir,
	}, nil
}

// Seek 的 ctx 入参为满足 wal.Reader 接口；文件 reader 是本地 I/O，无网络往返，
// 不需要受 ctx 约束，故忽略。
func (r *fileReader) Seek(_ context.Context, offset wal.Offset) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return wal.ErrReaderClosed
	}

	// 关闭当前打开的文件
	r.closeDatFile()

	m, err := readMeta(r.dir)
	if err != nil {
		return fmt.Errorf("file reader seek read meta: %w", err)
	}

	if len(m.Segments) == 0 {
		// 空 WAL
		r.curOffset = offset
		return nil
	}

	// 查找 segment
	segIdx, err := m.segmentForOffset(offset)
	if err != nil {
		return fmt.Errorf("file reader seek: %w", err)
	}

	seg := m.Segments[segIdx]
	localOffset := offset - seg.StartOffset

	// 读 idx 文件获取 byte position
	bytePos, err := r.readIdxEntry(seg.ID, localOffset)
	if err != nil {
		return fmt.Errorf("file reader seek read idx: %w", err)
	}

	// 打开 .dat 文件并 seek
	datPath := filepath.Join(r.dir, segDatName(seg.ID))
	f, err := os.Open(datPath)
	if err != nil {
		return fmt.Errorf("file reader seek open dat: %w", err)
	}

	if _, err := f.Seek(bytePos, io.SeekStart); err != nil {
		f.Close()
		return fmt.Errorf("file reader seek dat: %w", err)
	}

	r.curSegIdx = segIdx
	r.curSegID = seg.ID
	r.curDatFile = f
	r.curOffset = offset
	r.localOffset = localOffset

	return nil
}

func (r *fileReader) Next(_ context.Context) (*wal.ReadRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, wal.ErrReaderClosed
	}

	// 如果还没有打开任何文件（Seek(0) 或首次调用）
	if r.curDatFile == nil {
		if err := r.openInitialSegment(); err != nil {
			return nil, err
		}
		if r.curDatFile == nil {
			return nil, wal.ErrEndOfWAL
		}
	}

	for {
		// 尝试从当前 segment 读取
		rec, err := r.tryReadRecord()
		if err == nil {
			// 判断是否 AtLatest
			totalRecords, _ := r.totalRecords()
			rec.Offset = r.curOffset
			rec.AtLatest = r.curOffset+1 >= totalRecords
			r.curOffset++
			r.localOffset++
			return rec, nil
		}

		if err != io.EOF && err != io.ErrUnexpectedEOF {
			return nil, err
		}

		// 当前 segment 读完，尝试切换到下一个 segment
		switched, switchErr := r.tryNextSegment()
		if switchErr != nil {
			return nil, switchErr
		}
		if switched {
			continue // 重试读取
		}

		// 没有下一个 segment，检查活跃 segment 是否有新数据
		hasNew, checkErr := r.checkNewRecords()
		if checkErr != nil {
			return nil, checkErr
		}
		if hasNew {
			continue // 重试读取
		}

		return nil, wal.ErrEndOfWAL
	}
}

func (r *fileReader) tryReadRecord() (*wal.ReadRecord, error) {
	// 读 header length
	headerLen, err := r.readUint32()
	if err != nil {
		return nil, err
	}

	// 读 header（map kv 二进制编码）
	headerBuf := make([]byte, headerLen)
	if _, err := io.ReadFull(r.curDatFile, headerBuf); err != nil {
		return nil, err
	}

	// 解析 header map → Header struct
	headerMap, err := wal.DecodeHeaderMap(headerBuf)
	if err != nil {
		return nil, fmt.Errorf("decode header map: %w", err)
	}
	header, err := wal.HeaderFromMap(headerMap)
	if err != nil {
		return nil, fmt.Errorf("header from map: %w", err)
	}

	// 读 data length
	dataLen, err := r.readUint32()
	if err != nil {
		return nil, err
	}

	// 读 data
	data := make([]byte, dataLen)
	if _, err := io.ReadFull(r.curDatFile, data); err != nil {
		return nil, err
	}

	return &wal.ReadRecord{
		Header: header,
		Data:   data,
		Time:   time.Now(),
	}, nil
}

func (r *fileReader) tryNextSegment() (bool, error) {
	m, err := readMeta(r.dir)
	if err != nil {
		return false, err
	}

	nextIdx := r.curSegIdx + 1
	if nextIdx >= len(m.Segments) {
		return false, nil
	}

	// 有下一个 segment
	nextSeg := m.Segments[nextIdx]
	r.closeDatFile()

	datPath := filepath.Join(r.dir, segDatName(nextSeg.ID))
	f, err := os.Open(datPath)
	if err != nil {
		return false, fmt.Errorf("open next segment: %w", err)
	}

	r.curSegIdx = nextIdx
	r.curSegID = nextSeg.ID
	r.curDatFile = f
	r.localOffset = 0
	return true, nil
}

func (r *fileReader) checkNewRecords() (bool, error) {
	// 检查活跃 segment 的 idx 文件是否有新记录
	count, err := idxRecordCount(r.dir, r.curSegID)
	if err != nil {
		return false, err
	}
	return count > r.localOffset, nil
}

func (r *fileReader) totalRecords() (int64, error) {
	m, err := readMeta(r.dir)
	if err != nil {
		return 0, err
	}
	if len(m.Segments) == 0 {
		return 0, nil
	}

	// 已完成 segment（除最后一个）的记录数直接从 meta 获取
	var total int64
	for i := 0; i < len(m.Segments)-1; i++ {
		total += m.Segments[i].RecordCount
	}

	// 最后一个 segment 始终从 idx 文件动态获取（无论 writer 是否已关闭）
	last := m.Segments[len(m.Segments)-1]
	lastCount, err := idxRecordCount(r.dir, last.ID)
	if err != nil {
		return 0, err
	}
	total += lastCount
	return total, nil
}

func (r *fileReader) openInitialSegment() error {
	m, err := readMeta(r.dir)
	if err != nil {
		return err
	}
	if len(m.Segments) == 0 {
		return nil
	}

	// 找到包含 curOffset 的 segment
	segIdx, err := m.segmentForOffset(r.curOffset)
	if err != nil {
		// offset 可能超出当前范围
		return nil
	}

	seg := m.Segments[segIdx]
	localOffset := r.curOffset - seg.StartOffset

	// 读 idx 获取 byte position
	bytePos, err := r.readIdxEntry(seg.ID, localOffset)
	if err != nil {
		return nil
	}

	datPath := filepath.Join(r.dir, segDatName(seg.ID))
	f, err := os.Open(datPath)
	if err != nil {
		return fmt.Errorf("open initial segment: %w", err)
	}

	if _, err := f.Seek(bytePos, io.SeekStart); err != nil {
		f.Close()
		return err
	}

	r.curSegIdx = segIdx
	r.curSegID = seg.ID
	r.curDatFile = f
	r.localOffset = localOffset
	return nil
}

func (r *fileReader) readIdxEntry(segID uint32, localOffset int64) (int64, error) {
	idxPath := filepath.Join(r.dir, segIdxName(segID))
	f, err := os.Open(idxPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	buf := make([]byte, indexEntrySize)
	if _, err := f.ReadAt(buf, localOffset*indexEntrySize); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(buf)), nil
}

func (r *fileReader) readUint32() (uint32, error) {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(r.curDatFile, buf); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(buf), nil
}

func (r *fileReader) closeDatFile() {
	if r.curDatFile != nil {
		r.curDatFile.Close()
		r.curDatFile = nil
	}
}

func (r *fileReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.closeDatFile()
	return nil
}
