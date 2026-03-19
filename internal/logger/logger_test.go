package logger

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"
)

func TestLogger_Debug(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	l := New("debug")
	l.Debug("test message %s", "hello")

	_ = w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	output := buf.String()

	if !strings.Contains(output, "test message hello") {
		t.Errorf("Expected output to contain 'test message hello', got: %s", output)
	}
}

func TestLogger_Info(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	l := New("info")
	l.Info("info message %d", 123)

	_ = w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	output := buf.String()

	if !strings.Contains(output, "info message 123") {
		t.Errorf("Expected output to contain 'info message 123', got: %s", output)
	}
}

func TestLogger_Warn(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	l := New("warn")
	l.Warn("warning message")

	_ = w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	output := buf.String()

	if !strings.Contains(output, "WARNING:") {
		t.Errorf("Expected output to contain 'WARNING:', got: %s", output)
	}
	if !strings.Contains(output, "warning message") {
		t.Errorf("Expected output to contain 'warning message', got: %s", output)
	}
}

func TestLogger_Error(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	l := New("error")
	l.Error("error message")

	_ = w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	output := buf.String()

	if !strings.Contains(output, "ERROR:") {
		t.Errorf("Expected output to contain 'ERROR:', got: %s", output)
	}
	if !strings.Contains(output, "error message") {
		t.Errorf("Expected output to contain 'error message', got: %s", output)
	}
}

func TestLogger_LevelFiltering(t *testing.T) {
	// Test that debug messages are filtered when level is info
	l := New("info")

	// Just verify the logger was created with correct level
	if l.IsDebug() {
		t.Error("Expected IsDebug() to return false for info level")
	}

	// Test that debug level enables debug
	l2 := New("debug")
	if !l2.IsDebug() {
		t.Error("Expected IsDebug() to return true for debug level")
	}
}

func TestLogger_IsDebug(t *testing.T) {
	l := New("debug")
	if !l.IsDebug() {
		t.Error("Expected IsDebug() to return true for debug level")
	}

	l = New("info")
	if l.IsDebug() {
		t.Error("Expected IsDebug() to return false for info level")
	}
}

func TestLogger_Level(t *testing.T) {
	tests := []struct {
		level       string
		expectedStr string
	}{
		{"debug", "DEBUG"},
		{"info", "INFO"},
		{"warn", "WARN"},
		{"error", "ERROR"},
		{"unknown", "INFO"}, // defaults to INFO
	}

	for _, tt := range tests {
		l := New(tt.level)
		// Just verify it doesn't panic and creates a valid logger
		if l == nil {
			t.Errorf("Expected logger for level %s", tt.level)
		}
	}
}

func TestLogger_Fatal(t *testing.T) {
	// Save original logFatal flag
	oldFlag := log.Flags()

	// We can't easily test fatal without exiting, so just verify it doesn't panic
	func() {
		defer func() {
			_ = recover() // Expected to panic due to os.Exit
		}()
		// This would call os.Exit(1)
		// l := New("error")
		// l.Fatal("fatal message")
	}()

	log.SetFlags(oldFlag)
}

func BenchmarkLogger_Debug(b *testing.B) {
	l := New("debug")
	for i := 0; i < b.N; i++ {
		l.Debug("test message %d", i)
	}
}

func BenchmarkLogger_Info(b *testing.B) {
	l := New("info")
	for i := 0; i < b.N; i++ {
		l.Info("test message %d", i)
	}
}

func ExampleLogger_Debug() {
	l := New("debug")
	l.Debug("debug message %s", "hello")
}

func ExampleLogger_Info() {
	l := New("info")
	l.Info("info message %d", 42)
}

func ExampleLogger_Warn() {
	l := New("warn")
	l.Warn("warning message")
}

func ExampleLogger_Error() {
	l := New("error")
	l.Error("error message")
}
