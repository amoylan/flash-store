package logger

import (
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"
)

// OutputType 日志输出目标。
type OutputType string

const (
	OutputStdout OutputType = "stdout"
	OutputStderr OutputType = "stderr"
	OutputFile   OutputType = "file"
)

// OutputConfig 描述日志写到哪里。Type=file 时 Path 必填；
// 可选挂 Lumberjack 做按大小轮转。
type OutputConfig struct {
	Type OutputType `json:"type" yaml:"type"`
	// file path（Type=file 时必须）
	Path string `json:"path,omitempty" yaml:"path,omitempty"`
	// lumberjack 轮转配置（可选，仅 Type=file 生效）
	Lumberjack *LumberjackConfig `json:"lumberjack,omitempty" yaml:"lumberjack,omitempty"`
}

// LumberjackConfig 对应 lumberjack.Logger 的轮转参数。
type LumberjackConfig struct {
	Filename   string `json:"filename" yaml:"filename"`
	MaxSize    int    `json:"max_size" yaml:"max_size"` // MB
	MaxAge     int    `json:"max_age" yaml:"max_age"`   // days
	MaxBackups int    `json:"max_backups" yaml:"max_backups"`
	Compress   bool   `json:"compress" yaml:"compress"`
	LocalTime  bool   `json:"local_time" yaml:"local_time"`
}

func (lc LumberjackConfig) GetFilename() string {
	if lc.Filename == "" {
		return "./logs/app"
	}
	return lc.Filename
}

func (lc LumberjackConfig) GetMaxSize() int {
	if lc.MaxSize == 0 {
		return 100
	}
	return lc.MaxSize
}

func (lc LumberjackConfig) GetMaxAge() int {
	if lc.MaxAge == 0 {
		return 7
	}
	return lc.MaxAge
}

func (lc LumberjackConfig) GetMaxBackups() int {
	if lc.MaxBackups == 0 {
		return 10
	}
	return lc.MaxBackups
}

// BuildWriter 把 OutputConfig 转成 zap 可用的 io.Writer。
func (oc OutputConfig) BuildWriter() (io.Writer, error) {
	switch oc.Type {
	case OutputStdout:
		return os.Stdout, nil

	case OutputStderr:
		return os.Stderr, nil

	case OutputFile:
		// 普通 file：append 模式
		if oc.Lumberjack == nil {
			return os.OpenFile(
				oc.Path,
				os.O_APPEND|os.O_CREATE|os.O_WRONLY,
				0644,
			)
		}

		// lumberjack 轮转
		lj := oc.Lumberjack
		return &lumberjack.Logger{
			Filename:   lj.GetFilename(),
			MaxSize:    lj.GetMaxSize(),
			MaxAge:     lj.GetMaxAge(),
			MaxBackups: lj.GetMaxBackups(),
			Compress:   lj.Compress,
			LocalTime:  lj.LocalTime,
		}, nil

	default:
		return nil, fmt.Errorf("unknown output type: %s", oc.Type)
	}
}

// Encoding 日志编码格式。
type Encoding string

const (
	EncodingJSON    Encoding = "json"
	EncodingConsole Encoding = "console"
)

// Config 构建 zap logger 的配置。
//
// zap 是唯一后端，无 Driver 选择；键名/默认值与 zap 惯例一致。
type Config struct {
	Level    string
	Encoding Encoding // json / console

	EnableCaller bool
	EnableStack  bool

	TimeKey    string
	LevelKey   string
	MessageKey string
	CallerKey  string
	StackKey   string

	TimeFormat string

	Output OutputConfig

	Fields   map[string]any
	Sampling *SamplingConfig
}

func (c Config) Validate() error {
	if c.Output.Type == OutputFile && c.Output.Path == "" {
		return fmt.Errorf("output path is required when output type is file")
	}

	if c.Encoding != EncodingJSON && c.Encoding != EncodingConsole {
		return fmt.Errorf("unsupported encoding: %s", c.Encoding)
	}
	return nil
}

func (c Config) GetLevel() Level {
	switch strings.ToUpper(c.Level) {
	case DebugLevel.String():
		return DebugLevel
	case InfoLevel.String():
		return InfoLevel
	case WarnLevel.String():
		return WarnLevel
	case ErrorLevel.String():
		return ErrorLevel
	default:
		return InfoLevel
	}
}

func (c Config) GetTimeKey() string {
	if c.TimeKey != "" {
		return c.TimeKey
	}
	return "t"
}

func (c Config) GetLevelKey() string {
	if c.LevelKey != "" {
		return c.LevelKey
	}
	return "l"
}

func (c Config) GetMessageKey() string {
	if c.MessageKey != "" {
		return c.MessageKey
	}
	return "m"
}

func (c Config) GetCallerKey() string {
	if c.CallerKey != "" {
		return c.CallerKey
	}
	return "c"
}

func (c Config) GetStackKey() string {
	if c.StackKey != "" {
		return c.StackKey
	}
	return "s"
}

func (c Config) GetTimeFormat() string {
	if c.TimeFormat != "" {
		return c.TimeFormat
	}
	return "01-02 15:04:05.000"
}

// SamplingConfig 是 zap 的采样配置（Initial/Thereafter），
// 见 go.uber.org/zap.SamplingConfig。
type SamplingConfig struct {
	Initial    int `json:"initial" yaml:"initial"`
	Thereafter int `json:"thereafter" yaml:"thereafter"`
}
