// Package log 是 store 模块的日志入口（facade）。
//
// 保留 package 级 Debug / Info / Warn / Error 与 Init / Get / Set 的调用形态，内部
// 实现基于 zap 原生 logger（见 log/logger 子包），不支持 zerolog。
//
// 未调用 Init 前 global 是 nopLogger（静默丢弃），保证 store 作为
// 库被引用时默认不产生日志输出；主程序启动期调用 log.Init 生效。
package log

import (
	"github.com/amoylan/flash-store/store/log/logger"
)

// nopLogger 是 Init 之前的默认实现：所有日志静默丢弃。
type nopLogger struct{}

func (n *nopLogger) GetLevel() logger.Level        { return logger.InfoLevel }
func (n *nopLogger) Debug(string, ...logger.Field) {}
func (n *nopLogger) Info(string, ...logger.Field)  {}
func (n *nopLogger) Warn(string, ...logger.Field)  {}
func (n *nopLogger) Error(string, ...logger.Field) {}
func (n *nopLogger) With(...logger.Field) logger.Logger {
	return n
}
func (n *nopLogger) WithStacktrace() logger.Logger { return n }
func (n *nopLogger) AddCallerSkip(int) logger.Logger {
	return n
}

// global 是返回给调用方的 logger（Get 的返回值），其 caller skip
// 校准为「调用方直接调用 logger 方法」。facade 在 global 之上多跳
// 一帧，专供 package 级 log.* 函数使用——因为它们比直接用接口
// 多了一层包装，否则 caller 行号会停在本文件而不是真正的调用处。
var (
	global logger.Logger = &nopLogger{}
	facade logger.Logger = &nopLogger{}
)

// Init 用 logger.Config 构建全局 zap logger。重复调用会覆盖之前的
// 全局实例。
func Init(config logger.Config) error {
	l, err := logger.New(config)
	if err != nil {
		return err
	}
	global = l
	facade = l.AddCallerSkip(1)
	return nil
}

// Get 返回全局 logger（未 Init 时是静默丢弃的 nopLogger）。
func Get() logger.Logger {
	return global
}

// Set 直接替换全局 logger（测试 / 自定义场景用）。
func Set(l logger.Logger) {
	global = l
	facade = l.AddCallerSkip(1)
}

func Debug(msg string, fields ...logger.Field) {
	facade.Debug(msg, fields...)
}

func Info(msg string, fields ...logger.Field) {
	facade.Info(msg, fields...)
}

func Warn(msg string, fields ...logger.Field) {
	facade.Warn(msg, fields...)
}

func Error(msg string, fields ...logger.Field) {
	facade.Error(msg, fields...)
}
