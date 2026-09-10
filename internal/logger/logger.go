package logger

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// Level represents the log level
type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
)

// String returns the string representation of the level
func (l Level) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Config holds logging configuration
type Config struct {
	Level          string // debug, info, warn, error
	LogFile        string // path to log file (empty to disable file logging)
	FileLogging    bool   // enable/disable file logging
	ConsoleLogging bool   // enable/disable console logging
}

// Logger is a simple logger that supports different log levels
type Logger struct {
	level          Level
	consoleLogger  *log.Logger
	fileLogger     *log.Logger
	fileLogging    bool
	consoleLogging bool
	file           *os.File

	// mu guards the sinks so Close, which runs during shutdown while other
	// goroutines are still logging, cannot swap them mid-write.
	mu sync.Mutex
}

// New creates a new Logger with the specified level
func New(level string) *Logger {
	return NewWithConfig(Config{
		Level:          level,
		LogFile:        "",
		FileLogging:    false,
		ConsoleLogging: true,
	})
}

// NewWithConfig creates a new Logger with the specified configuration
func NewWithConfig(config Config) *Logger {
	var consoleLogger, fileLogger *log.Logger
	var logFileHandle *os.File

	// Setup console logging
	if config.ConsoleLogging {
		consoleLogger = log.New(os.Stdout, "", 0)
	}

	fileLogging := config.FileLogging && config.LogFile != ""
	consoleLogging := config.ConsoleLogging

	// Setup file logging
	if fileLogging {
		// Open log file in append mode, create if doesn't exist
		logFile, err := os.OpenFile(config.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			// Fall back to the console, and actually enable it: leaving
			// consoleLogging off while fileLogging stayed on with no sink
			// discarded every line, so a service whose log path is
			// unwritable logged nothing at all.
			fmt.Printf("[WARN] Could not open log file %s: %v; logging to console instead\n",
				config.LogFile, err)
			fileLogging = false
			consoleLogging = true
			if consoleLogger == nil {
				consoleLogger = log.New(os.Stdout, "", 0)
			}
		} else {
			fileLogger = log.New(logFile, "", 0)
			logFileHandle = logFile
		}
	}

	l := &Logger{
		consoleLogger:  consoleLogger,
		fileLogger:     fileLogger,
		fileLogging:    fileLogging,
		consoleLogging: consoleLogging,
		file:           logFileHandle,
	}

	switch config.Level {
	case "debug":
		l.level = DEBUG
	case "info":
		l.level = INFO
	case "warn":
		l.level = WARN
	case "error":
		l.level = ERROR
	default:
		l.level = INFO
	}

	return l
}

// emit formats and writes one line to every enabled sink.
func (l *Logger) emit(level Level, prefix, format string, args ...any) {
	if l.level > level {
		return
	}
	msg := fmt.Sprintf("[%s] %s%s",
		time.Now().Format("2006-01-02 15:04:05"), prefix, fmt.Sprintf(format, args...))

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.consoleLogging && l.consoleLogger != nil {
		_ = l.consoleLogger.Output(2, msg)
	}
	if l.fileLogging && l.fileLogger != nil {
		_ = l.fileLogger.Output(2, msg)
	}
}

// Debug logs a debug message.
func (l *Logger) Debug(format string, args ...any) {
	l.emit(DEBUG, "", format, args...)
}

// Info logs an info message.
func (l *Logger) Info(format string, args ...any) {
	l.emit(INFO, "", format, args...)
}

// Warn logs a warning message.
func (l *Logger) Warn(format string, args ...any) {
	l.emit(WARN, "WARNING: ", format, args...)
}

// Error logs an error message.
func (l *Logger) Error(format string, args ...any) {
	l.emit(ERROR, "ERROR: ", format, args...)
}

// Fatal logs a fatal message and exits.
func (l *Logger) Fatal(format string, args ...any) {
	l.emit(ERROR, "FATAL: ", format, args...)
	_ = l.Close()
	os.Exit(1)
}

// IsDebug returns true if debug logging is enabled
func (l *Logger) IsDebug() bool {
	return l.level <= DEBUG
}

// SerialOperation logs one serial exchange.
//
// Failures log at warning level: on a flaky link they are the primary
// diagnostic, and hiding them behind debug meant a silent device looked
// like a healthy one. Successes stay at debug, since there is one per
// register per cycle.
func (l *Logger) SerialOperation(operation string, command string, response string, duration time.Duration, err error) {
	if err != nil {
		l.Warn("SERIAL %s FAILED (took %v): cmd=%q error=%v", operation, duration, command, err)
		return
	}
	l.Debug("SERIAL %s (took %v): cmd=%q response=%q", operation, duration, command, response)
}

// FileLoggingActive reports whether log lines are being written to a file.
// It can be false even when file logging was requested, if the path could not
// be opened and the console fallback took over.
func (l *Logger) FileLoggingActive() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.fileLogging
}

// SerialSend logs one command written to the device.
//
// A register write expects no response, so no response field is logged.
// Failures surface at warning level, successes at debug.
func (l *Logger) SerialSend(command string, duration time.Duration, err error) {
	if err != nil {
		l.Warn("SERIAL SEND FAILED (took %v): cmd=%q error=%v", duration, command, err)
		return
	}
	l.Debug("SERIAL SEND (took %v): cmd=%q", duration, command)
}

// Close releases the log file, if one is open. It is idempotent, and safe to
// call while other goroutines are still logging.
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return nil
	}
	// Stop writing to the file before closing its descriptor.
	l.fileLogging = false
	l.fileLogger = nil
	err := l.file.Close()
	l.file = nil
	return err
}

// SignalReceived logs when a signal is received
func (l *Logger) SignalReceived(sig os.Signal) {
	// Goes through emit so the sinks stay guarded by mu; reading them directly
	// races with Close during shutdown.
	l.emit(ERROR, "", "Received signal %v, shutting down...", sig)
}
