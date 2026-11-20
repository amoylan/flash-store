package file

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

const (
	metaFileName   = "meta.json"
	indexEntrySize = 8 // 每条索引 8 字节
)

type segmentMeta struct {
	ID          uint32 `json:"id"`
	StartOffset int64  `json:"start_offset"`
	RecordCount int64  `json:"record_count"` // 已完成 segment 的最终记录数；活跃 segment 为 0
}

type meta struct {
	Segments []segmentMeta `json:"segments"`
}

// segmentForOffset 二分查找包含 offset 的 segment，返回 segment 在 Segments 切片中的下标
func (m *meta) segmentForOffset(offset int64) (int, error) {
	if len(m.Segments) == 0 {
		return 0, fmt.Errorf("no segments")
	}

	idx := sort.Search(len(m.Segments), func(i int) bool {
		return m.Segments[i].StartOffset > offset
	}) - 1

	if idx < 0 {
		return 0, fmt.Errorf("offset %d before first segment (start_offset=%d)", offset, m.Segments[0].StartOffset)
	}
	return idx, nil
}

// activeSegment 返回最后一个（活跃）segment
func (m *meta) activeSegment() *segmentMeta {
	if len(m.Segments) == 0 {
		return nil
	}
	return &m.Segments[len(m.Segments)-1]
}

// totalRecords 返回所有已完成 segment 的累计记录数（不含活跃 segment 的动态部分）
func (m *meta) completedRecords() int64 {
	var total int64
	for _, s := range m.Segments {
		total += s.RecordCount
	}
	return total
}

func segDatName(id uint32) string {
	return fmt.Sprintf("seg-%010d.dat", id)
}

func segIdxName(id uint32) string {
	return fmt.Sprintf("seg-%010d.idx", id)
}

// readMeta 从目录读取 meta.json。如果文件不存在，返回空 meta（无 error）
func readMeta(dir string) (*meta, error) {
	path := filepath.Join(dir, metaFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &meta{}, nil
		}
		return nil, fmt.Errorf("read meta: %w", err)
	}
	var m meta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("unmarshal meta: %w", err)
	}
	return &m, nil
}

// writeMeta 原子写 meta.json：先写 tmp 文件再 rename
func writeMeta(dir string, m *meta) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}

	tmpPath := filepath.Join(dir, metaFileName+".tmp")
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write meta tmp: %w", err)
	}

	finalPath := filepath.Join(dir, metaFileName)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("rename meta: %w", err)
	}
	return nil
}

// idxRecordCount 通过 idx 文件大小计算记录数
func idxRecordCount(dir string, segID uint32) (int64, error) {
	path := filepath.Join(dir, segIdxName(segID))
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return info.Size() / indexEntrySize, nil
}
