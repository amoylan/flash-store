package logger

import (
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// zapLogger 在 zap.Logger 之上实现 Logger 接口。
//
// 字段类型就是 zap.Field，所以各 level 方法零转换开销：
// ce.Write(fields...) 直接透传。
type zapLogger struct {
	level Level
	l     *zap.Logger
}

func levelFromConfig(config Config) zapcore.Level {
	switch config.GetLevel() {
	case DebugLevel:
		return zapcore.DebugLevel
	case InfoLevel:
		return zapcore.InfoLevel
	case WarnLevel:
		return zapcore.WarnLevel
	case ErrorLevel:
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

func sampledFromConfig(config Config) *zap.SamplingConfig {
	if config.Sampling == nil {
		return nil
	}
	if config.Sampling.Initial == 0 && config.Sampling.Thereafter == 0 {
		return nil
	}
	return &zap.SamplingConfig{
		Initial:    config.Sampling.Initial,
		Thereafter: config.Sampling.Thereafter,
	}
}

// New 用 Config 构建 zap 原生 logger。
//
// caller skip 基数为 1：zapLogger 的方法帧归因上移一层，
// 落在直接调用 logger 方法的调用方；log 包的 facade 会在此基础上
// 再 +1（见 log.Set），使 package 级 log.* 函数的 caller 行号
// 落在业务调用处。
func New(config Config) (Logger, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	encCfg := zapcore.EncoderConfig{
		TimeKey:       config.GetTimeKey(),
		LevelKey:      config.GetLevelKey(),
		NameKey:       "logger", // used by logger.Named(key); optional; useless
		CallerKey:     config.GetCallerKey(),
		MessageKey:    config.GetMessageKey(),
		StacktraceKey: config.GetStackKey(),
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.CapitalLevelEncoder,
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.UTC().Format(config.GetTimeFormat()))
		},
		EncodeDuration: zapcore.MillisDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	dyn := zap.NewAtomicLevel()
	dyn.SetLevel(levelFromConfig(config))

	var encoder zapcore.Encoder
	if config.Encoding == EncodingConsole {
		encoder = zapcore.NewConsoleEncoder(encCfg)
	} else {
		encoder = zapcore.NewJSONEncoder(encCfg)
	}
	writer, err := config.Output.BuildWriter()
	if err != nil {
		return nil, err
	}

	options := []zap.Option{
		zap.AddCallerSkip(1),
	}
	if config.EnableCaller {
		options = append(options, zap.AddCaller())
	}
	if config.EnableStack {
		options = append(options, zap.AddStacktrace(zapcore.ErrorLevel))
	}
	if sampled := sampledFromConfig(config); sampled != nil {
		options = append(options, zap.WrapCore(func(core zapcore.Core) zapcore.Core {
			var samplerOpts []zapcore.SamplerOption
			if sampled.Hook != nil {
				samplerOpts = append(samplerOpts, zapcore.SamplerHook(sampled.Hook))
			}
			return zapcore.NewSamplerWithOptions(
				core,
				time.Second,
				sampled.Initial,
				sampled.Thereafter,
				samplerOpts...,
			)
		}))
	}

	core := zapcore.NewCore(encoder, zapcore.AddSync(writer), dyn)
	l := zap.New(core, options...)
	if config.Fields != nil {
		fields := make([]zap.Field, 0, len(config.Fields))
		for k, v := range config.Fields {
			fields = append(fields, zap.Any(k, v))
		}
		l = l.With(fields...)
	}

	return &zapLogger{level: config.GetLevel(), l: l}, nil
}

// 每个 level 方法先走 Check：级别被过滤或采样丢弃时，字段构造的
// 开销完全跳过（zap 原生惯例）。

func (z *zapLogger) Debug(msg string, fields ...Field) {
	if ce := z.l.Check(zapcore.DebugLevel, msg); ce != nil {
		ce.Write(fields...)
	}
}

func (z *zapLogger) Info(msg string, fields ...Field) {
	if ce := z.l.Check(zapcore.InfoLevel, msg); ce != nil {
		ce.Write(fields...)
	}
}

func (z *zapLogger) Warn(msg string, fields ...Field) {
	if ce := z.l.Check(zapcore.WarnLevel, msg); ce != nil {
		ce.Write(fields...)
	}
}

func (z *zapLogger) Error(msg string, fields ...Field) {
	if ce := z.l.Check(zapcore.ErrorLevel, msg); ce != nil {
		ce.Write(fields...)
	}
}

func (z *zapLogger) With(fields ...Field) Logger {
	return &zapLogger{
		level: z.level,
		l:     z.l.With(fields...),
	}
}

func (z *zapLogger) WithStacktrace() Logger {
	return &zapLogger{
		level: z.level,
		l:     z.l.WithOptions(zap.AddStacktrace(zapcore.ErrorLevel)),
	}
}

func (z *zapLogger) AddCallerSkip(n int) Logger {
	return &zapLogger{
		level: z.level,
		l:     z.l.WithOptions(zap.AddCallerSkip(n)),
	}
}

func (z *zapLogger) GetLevel() Level {
	return z.level
}
