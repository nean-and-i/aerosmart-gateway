package logger

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"testing"
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
