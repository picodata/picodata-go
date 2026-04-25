package logger

import "errors"

// Logger is the interface implemented by pool loggers.
type Logger interface {
	Log(level LogLevel, msg string, fields ...any)
	SetLevel(level LogLevel) error
}

// LogLevel defines logging verbosity.
type LogLevel int

const (
	// LevelNone disables all logs.
	LevelNone LogLevel = iota
	// LevelError logs only errors.
	LevelError
	// LevelWarn logs warnings and errors.
	LevelWarn
	// LevelInfo logs informational messages, warnings, and errors.
	LevelInfo
	// LevelDebug logs all available messages.
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
