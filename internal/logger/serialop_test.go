package logger

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
	"time"
)

// captureLogger returns a logger writing into buf at the given level.
func captureLogger(level Level) (*Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return &Logger{
		level:          level,
		consoleLogger:  log.New(buf, "", 0),
		consoleLogging: true,
	}, buf
}

// A failed serial exchange is the main diagnostic for a flaky link, so it must
// be visible at the default level, not only in debug.
func TestSerialOperationFailureVisibleAtInfoLevel(t *testing.T) {
	l, buf := captureLogger(INFO)

	l.SerialOperation("READ", "130 1067", "", 0, errors.New("read timeout"))

	out := buf.String()
	if out == "" {
		t.Fatal("SerialOperation logged nothing for a failure at info level")
	}
	if !strings.Contains(out, "130 1067") || !strings.Contains(out, "read timeout") {
		t.Errorf("output = %q, want the command and the error", out)
	}
}

// Successful exchanges stay at debug: one line per register per cycle would
// flood the log and, with file logging on, the SD card.
func TestSerialOperationSuccessStaysAtDebug(t *testing.T) {
	l, buf := captureLogger(INFO)

	l.SerialOperation("READ", "130 1067", "3", 0, nil)

	if buf.String() != "" {
		t.Errorf("SerialOperation logged a success at info level: %q", buf.String())
	}
}

func TestSerialOperationSuccessVisibleAtDebug(t *testing.T) {
	l, buf := captureLogger(DEBUG)

	l.SerialOperation("READ", "130 1067", "3", 0, nil)

	if !strings.Contains(buf.String(), "130 1067") {
		t.Errorf("output = %q, want the command at debug level", buf.String())
	}
}

// Errors must never be suppressed by the level filter.
func TestErrorAlwaysLogged(t *testing.T) {
	l, buf := captureLogger(ERROR)

	l.Debug("quiet")
	l.Info("quiet")
	l.Warn("quiet")
	l.Error("loud %d", 1)

	out := buf.String()
	if strings.Contains(out, "quiet") {
		t.Errorf("output = %q, want lower levels suppressed", out)
	}
	if !strings.Contains(out, "loud 1") {
		t.Errorf("output = %q, want the error message", out)
	}
}

// Close runs during shutdown while other goroutines may still be logging.
func TestConcurrentLoggingAndClose(t *testing.T) {
	dir := t.TempDir()
	l := NewWithConfig(Config{
		Level:          "debug",
		LogFile:        dir + "/test.log",
		FileLogging:    true,
		ConsoleLogging: false,
	})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				l.Info("worker %d line %d", n, j)
			}
		}(i)
	}

	time.Sleep(2 * time.Millisecond)
	if err := l.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
	wg.Wait()

	// Close must be idempotent: shutdown paths can reach it twice.
	if err := l.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}
	l.Info("after close must not panic")
}
