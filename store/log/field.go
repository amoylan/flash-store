package log

import (
	"go.uber.org/zap"
)

// 字段构造器直接复用 zap 原生函数（Field 即 zap.Field，见
// log/logger 子包），不做二次封装：
//
//   - 错误字段用 zap.NamedError（nil err 自动变成 no-op 字段）
//   - 字节数组用 zap.ByteString
//   - zap.Object / zap.Array 需要 marshaler，当前代码未使用，未导出。
var (
	Skip       = zap.Skip
	Int        = zap.Int
	Int32      = zap.Int32
	Int64      = zap.Int64
	Uint       = zap.Uint
	Uint32     = zap.Uint32
	Uint64     = zap.Uint64
	Float32    = zap.Float32
	Float64    = zap.Float64
	Bool       = zap.Bool
	String     = zap.String
	ByteString = zap.ByteString
	Time       = zap.Time
	Duration   = zap.Duration
	NamedError = zap.NamedError
	Any        = zap.Any
)
