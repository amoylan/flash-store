package config

const DefaultMaxSegmentSize = 256 * 1024 * 1024 // 256MB

type Reader struct {
	// Dir WAL 目录路径
	Dir string
}

type Writer struct {
	// Dir WAL 目录路径
	Dir string
	// MaxSegmentSize 单 segment .dat 文件大小上限（字节），默认 256MB
	MaxSegmentSize int64
}

func (w *Writer) GetMaxSegmentSize() int64 {
	if w.MaxSegmentSize <= 0 {
		return DefaultMaxSegmentSize
	}
	return w.MaxSegmentSize
}
