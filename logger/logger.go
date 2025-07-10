package logger

import "errors"

type Logger interface {
	Log(level LogLevel, msg string, fields ...any)
	SetLevel(level LogLevel) error
}

type LogLevel int

const (
	LevelNone LogLevel = iota
	LevelError
	LevelWarn
	LevelInfo
	LevelDebug
)

func (l LogLevel) String() (string, error) {
	switch l {
	case LevelNone:
		return "NONE", nil
	case LevelError:
		return "ERROR", nil
	case LevelWarn:
		return "WARN", nil
	case LevelInfo:
		return "INFO", nil
	case LevelDebug:
		return "DEBUG", nil
	default:
		return "", errors.New("unknown log level")
	}
}
