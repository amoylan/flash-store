// Package logger 提供 store 模块的 logger 接口层，内部实现基于 zap 原生 API。
//
// 保留相同的 Logger 接口形态（GetLevel / Debug / Info / Warn / Error / With /
// WithStacktrace / AddCallerSkip），但两点简化：
//
//  1. 不再自带 Field 抽象，字段直接用 zap.Field（type Field = zap.Field），
//     字段构造器用 zap 原生函数（zap.String / zap.Int64 / zap.NamedError …），
//     少一层转换开销，API 与 zap 完全对齐；
//  2. 只保留 zap 一个 driver，删除 zerolog 后端与 Driver 选择。
package logger

import (
	"go.uber.org/zap"
)

// Level 日志级别，与 zapcore 的常见四级一一对应。
type Level int8

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

func (level Level) String() string {
	switch level {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Field 直接别名 zap.Field —— 字段构造器一律用 zap 原生函数，
// 不维护一套平行的 Field 类型。
type Field = zap.Field

// Logger 是 store 内部组件使用的 logger 接口。
//
// 实现方是 zap.go 里的 zapLogger（以及 log 包里的 nopLogger）；
// 业务侧通常不直接拿接口，而是用 log 包的 package 级函数。
type Logger interface {
	GetLevel() Level
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)

	// With 返回带上常驻字段的子 logger（如 shard id、identity 等
	// 高频上下文，避免每次调用重复传）。
	With(fields ...Field) Logger

	// WithStacktrace 返回一个在 Error 级别自动附带调用栈的 logger。
	WithStacktrace() Logger

	// AddCallerSkip 返回一个把调用点归因上移 n 帧的 logger，
	// 供薄包装层（例如 log 包的 package 级 facade）使用，
	// 使 caller 行号落在真正的调用处而不是 wrapper 内部。
	AddCallerSkip(n int) Logger
}
