package logger

import (
	"bytes"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
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
	for i := range 8 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := range 50 {
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

// When the log file cannot be opened, the fallback must actually take effect.
// It used to build a console logger but leave consoleLogging false while
// fileLogging stayed true with a nil sink, so every line was discarded.
func TestFileOpenFailureFallsBackToConsole(t *testing.T) {
	// A path underneath a regular file cannot be opened (ENOTDIR).
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	unopenable := filepath.Join(blocker, "aerosmart.log")

	stdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe() error = %v", err)
	}
	os.Stdout = w

	l := NewWithConfig(Config{
		Level:          "info",
		LogFile:        unopenable,
		FileLogging:    true,
		ConsoleLogging: false, // as a service unit would have it
	})
	l.Info("this line must not be lost")

	_ = w.Close()
	os.Stdout = stdout
	out, _ := io.ReadAll(r)

	if !strings.Contains(string(out), "this line must not be lost") {
		t.Errorf("log output = %q, want the message on stdout after the file failed to open", out)
	}
	if l.fileLogging {
		t.Error("fileLogging = true with no file sink")
	}
	if !l.consoleLogging {
		t.Error("consoleLogging = false, so nothing would be logged anywhere")
	}
}
