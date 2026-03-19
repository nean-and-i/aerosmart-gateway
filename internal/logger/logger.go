package logger

import (
	"fmt"
	"log"
	"os"
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

	// Setup console logging
	if config.ConsoleLogging {
		consoleLogger = log.New(os.Stdout, "", 0)
	}

	// Setup file logging
	if config.FileLogging && config.LogFile != "" {
		// Open log file in append mode, create if doesn't exist
		logFile, err := os.OpenFile(config.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			// If we can't open the log file, fall back to console only
			fmt.Printf("[WARN] Could not open log file %s: %v\n", config.LogFile, err)
			// Only set consoleLogger if it wasn't already configured
			if consoleLogger == nil {
				consoleLogger = log.New(os.Stdout, "", 0)
			}
		} else {
			fileLogger = log.New(logFile, "", 0)
		}
	}

	l := &Logger{
		consoleLogger:  consoleLogger,
		fileLogger:     fileLogger,
		fileLogging:    config.FileLogging && config.LogFile != "",
		consoleLogging: config.ConsoleLogging,
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

// Debug logs a debug message
func (l *Logger) Debug(format string, args ...interface{}) {
	if l.level <= DEBUG {
		msg := fmt.Sprintf("[%s] "+format, append([]interface{}{time.Now().Format("2006-01-02 15:04:05")}, args...)...)
		if l.consoleLogging && l.consoleLogger != nil {
			_ = l.consoleLogger.Output(2, msg)
		}
		if l.fileLogging && l.fileLogger != nil {
			_ = l.fileLogger.Output(2, msg)
		}
	}
}

// DebugWithTiming logs a debug message with timing information
func (l *Logger) DebugWithTiming(operation string, duration time.Duration, format string, args ...interface{}) {
	if l.level <= DEBUG {
		msg := fmt.Sprintf("[%s] %s (took %v): "+format, append([]interface{}{time.Now().Format("2006-01-02 15:04:05"), operation, duration}, args...)...)
		if l.consoleLogging && l.consoleLogger != nil {
			_ = l.consoleLogger.Output(2, msg)
		}
		if l.fileLogging && l.fileLogger != nil {
			_ = l.fileLogger.Output(2, msg)
		}
	}
}

// Info logs an info message
func (l *Logger) Info(format string, args ...interface{}) {
	if l.level <= INFO {
		msg := fmt.Sprintf("[%s] "+format, append([]interface{}{time.Now().Format("2006-01-02 15:04:05")}, args...)...)
		if l.consoleLogging && l.consoleLogger != nil {
			_ = l.consoleLogger.Output(2, msg)
		}
		if l.fileLogging && l.fileLogger != nil {
			_ = l.fileLogger.Output(2, msg)
		}
	}
}

// Warn logs a warning message
func (l *Logger) Warn(format string, args ...interface{}) {
	if l.level <= WARN {
		msg := fmt.Sprintf("[%s] WARNING: "+format, append([]interface{}{time.Now().Format("2006-01-02 15:04:05")}, args...)...)
		if l.consoleLogging && l.consoleLogger != nil {
			_ = l.consoleLogger.Output(2, msg)
		}
		if l.fileLogging && l.fileLogger != nil {
			_ = l.fileLogger.Output(2, msg)
		}
	}
}

// Error logs an error message
func (l *Logger) Error(format string, args ...interface{}) {
	if l.level <= ERROR {
		msg := fmt.Sprintf("[%s] ERROR: "+format, append([]interface{}{time.Now().Format("2006-01-02 15:04:05")}, args...)...)
		if l.consoleLogging && l.consoleLogger != nil {
			_ = l.consoleLogger.Output(2, msg)
		}
		if l.fileLogging && l.fileLogger != nil {
			_ = l.fileLogger.Output(2, msg)
		}
	}
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(format string, args ...interface{}) {
	msg := fmt.Sprintf("[%s] FATAL: "+format, append([]interface{}{time.Now().Format("2006-01-02 15:04:05")}, args...)...)
	if l.consoleLogging && l.consoleLogger != nil {
		_ = l.consoleLogger.Output(2, msg)
	}
	if l.fileLogging && l.fileLogger != nil {
		_ = l.fileLogger.Output(2, msg)
	}
	os.Exit(1)
}

// IsDebug returns true if debug logging is enabled
func (l *Logger) IsDebug() bool {
	return l.level <= DEBUG
}

// SerialOperation logs serial port operations with full details
func (l *Logger) SerialOperation(operation string, command string, response string, duration time.Duration, err error) {
	if l.level <= DEBUG {
		var msg string
		if err != nil {
			msg = fmt.Sprintf("[%s] SERIAL %s FAILED (took %v): cmd=%q error=%v",
				time.Now().Format("2006-01-02 15:04:05"), operation, duration, command, err)
		} else {
			msg = fmt.Sprintf("[%s] SERIAL %s (took %v): cmd=%q response=%q",
				time.Now().Format("2006-01-02 15:04:05"), operation, duration, command, response)
		}
		if l.consoleLogging && l.consoleLogger != nil {
			_ = l.consoleLogger.Output(2, msg)
		}
		if l.fileLogging && l.fileLogger != nil {
			_ = l.fileLogger.Output(2, msg)
		}
	}
}

// SerialConnection logs serial connection state changes
func (l *Logger) SerialConnection(action string, port string, baudRate int, err error) {
	var msg string
	if err != nil {
		msg = fmt.Sprintf("[%s] SERIAL %s FAILED: port=%s baudrate=%d error=%v",
			time.Now().Format("2006-01-02 15:04:05"), action, port, baudRate, err)
	} else {
		msg = fmt.Sprintf("[%s] SERIAL %s: port=%s baudrate=%d",
			time.Now().Format("2006-01-02 15:04:05"), action, port, baudRate)
	}
	if l.consoleLogging && l.consoleLogger != nil {
		_ = l.consoleLogger.Output(2, msg)
	}
	if l.fileLogging && l.fileLogger != nil {
		_ = l.fileLogger.Output(2, msg)
	}
}

// SerialReopen logs serial port reopen attempts
func (l *Logger) SerialReopen(attempt int, maxAttempts int, success bool) {
	if success {
		l.Info("Serial port reopened successfully (attempt %d/%d)", attempt, maxAttempts)
	} else {
		l.Warn("Serial port reopen failed (attempt %d/%d)", attempt, maxAttempts)
	}
}

// MQTTOperation logs MQTT operations with full details
func (l *Logger) MQTTOperation(operation string, topic string, payload string, qos int, retain bool, err error) {
	if l.level <= DEBUG {
		var msg string
		if err != nil {
			msg = fmt.Sprintf("[%s] MQTT %s FAILED: topic=%q payload=%q qos=%d retain=%t error=%v",
				time.Now().Format("2006-01-02 15:04:05"), operation, topic, payload, qos, retain, err)
		} else {
			msg = fmt.Sprintf("[%s] MQTT %s: topic=%q payload=%q qos=%d retain=%t",
				time.Now().Format("2006-01-02 15:04:05"), operation, topic, payload, qos, retain)
		}
		if l.consoleLogging && l.consoleLogger != nil {
			_ = l.consoleLogger.Output(2, msg)
		}
		if l.fileLogging && l.fileLogger != nil {
			_ = l.fileLogger.Output(2, msg)
		}
	}
}

// MQTTConnection logs MQTT connection state changes
func (l *Logger) MQTTConnection(action string, broker string, port int, err error) {
	var msg string
	if err != nil {
		msg = fmt.Sprintf("[%s] MQTT %s FAILED: broker=%s port=%d error=%v",
			time.Now().Format("2006-01-02 15:04:05"), action, broker, port, err)
	} else {
		msg = fmt.Sprintf("[%s] MQTT %s: broker=%s port=%d",
			time.Now().Format("2006-01-02 15:04:05"), action, broker, port)
	}
	if l.consoleLogging && l.consoleLogger != nil {
		_ = l.consoleLogger.Output(2, msg)
	}
	if l.fileLogging && l.fileLogger != nil {
		_ = l.fileLogger.Output(2, msg)
	}
}

// RegisterOperation logs register read/write operations
func (l *Logger) RegisterOperation(operation string, register string, command string, value string, valid bool, err error) {
	if err != nil {
		msg := fmt.Sprintf("[%s] REGISTER %s FAILED: register=%s cmd=%q error=%v",
			time.Now().Format("2006-01-02 15:04:05"), operation, register, command, err)
		if l.consoleLogging && l.consoleLogger != nil {
			_ = l.consoleLogger.Output(2, msg)
		}
		if l.fileLogging && l.fileLogger != nil {
			_ = l.fileLogger.Output(2, msg)
		}
	} else if l.level <= DEBUG {
		msg := fmt.Sprintf("[%s] REGISTER %s: register=%s cmd=%q value=%q valid=%t",
			time.Now().Format("2006-01-02 15:04:05"), operation, register, command, value, valid)
		if l.consoleLogging && l.consoleLogger != nil {
			_ = l.consoleLogger.Output(2, msg)
		}
		if l.fileLogging && l.fileLogger != nil {
			_ = l.fileLogger.Output(2, msg)
		}
	}
}

// SignalReceived logs when a signal is received
func (l *Logger) SignalReceived(sig os.Signal) {
	msg := fmt.Sprintf("Received signal %v, initiating immediate shutdown...", sig)
	if l.consoleLogging && l.consoleLogger != nil {
		_ = l.consoleLogger.Output(2, msg)
	}
	if l.fileLogging && l.fileLogger != nil {
		_ = l.fileLogger.Output(2, msg)
	}
}
