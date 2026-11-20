package config

import (
	fileconf "github.com/amoylan/flash-store/store/snapshot/source/file/config"
	s3conf "github.com/amoylan/flash-store/store/snapshot/source/s3/config"
)

// ======================== SnapshotFormat ========================

type SnapshotFormat string

const (
	SnapshotFormatProtobuf SnapshotFormat = "proto"
	SnapshotFormatJson     SnapshotFormat = "json"
)

// ======================== SourceType ========================

type SourceType int

const (
	SourceTypeFile SourceType = 1
	SourceTypeS3   SourceType = 2
	SourceTypeNoop SourceType = 3
)

func (t SourceType) String() string {
	switch t {
	case SourceTypeFile:
		return "file"
	case SourceTypeS3:
		return "s3"
	case SourceTypeNoop:
		return "noop"
	}
	return ""
}

// ======================== Config ========================

type Writer struct {
	Source SourceType
	Format SnapshotFormat
	File   fileconf.Config
	S3     s3conf.Config
}

type Reader struct {
	Source SourceType
	File   fileconf.Config
	S3     s3conf.Config
}

type Config struct {
	// Name 快照逻辑名称（如 "order-store"），用于 segment 路径中的 name 部分
	Name   string
	Writer Writer
	Reader Reader
}
