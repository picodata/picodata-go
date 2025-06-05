package logger

import (
	"fmt"
	"log"
)

var _ Logger = (*defaultLogger)(nil)

var globalLogger Logger = newDefaultLogger()

func SetDefaultLogger(logger Logger)                { globalLogger = logger }
func SetLevel(level LogLevel) error                 { return globalLogger.SetLevel(level) }
func Log(level LogLevel, msg string, fields ...any) { globalLogger.Log(level, msg, fields...) }

// DefaultLogger declaration
type defaultLogger struct {
	level LogLevel
}

func newDefaultLogger() *defaultLogger {
	return &defaultLogger{
		level: LevelInfo,
	}
}

func (l *defaultLogger) SetLevel(level LogLevel) error {
	if _, err := level.String(); err != nil {
		return err
	}
	l.level = level

	return nil
}

func (l *defaultLogger) Log(level LogLevel, msg string, fields ...any) {
	if level > l.level {
		return
	}
	// NOTE: We can ignore error handling, because the default one is already valid.
	// The only way to change logging level is to use pool options, which has error handling.
	levelStr, _ := level.String()
	log.Printf("[%s] %s\n", levelStr, fmt.Sprintf(msg, fields...))
}
