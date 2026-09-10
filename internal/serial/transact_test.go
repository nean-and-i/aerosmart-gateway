package serial

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"
)

// fakeDevice answers commands the way the real unit does: a written command
// queues a response frame to be read back. It also models TCIOFLUSH, which
// discards bytes that have not been read yet.
type fakeDevice struct {
	pending []byte
	respond func(cmd string) string
	writes  []string
	flushes int
	opens   int
	readErr error
	closed  bool
}

func (d *fakeDevice) Read(p []byte) (int, error) {
	if d.readErr != nil {
		return 0, d.readErr
	}
	if len(d.pending) == 0 {
		return 0, nil // idle: no data, no error
	}
	n := copy(p, d.pending)
	d.pending = d.pending[n:]
	return n, nil
}

func (d *fakeDevice) Write(p []byte) (int, error) {
	cmd := string(p)
	d.writes = append(d.writes, cmd)
	if d.respond != nil {
		d.pending = append(d.pending, d.respond(cmd)...)
	}
	return len(p), nil
}

// Flush models TCIOFLUSH: undelivered input is discarded.
func (d *fakeDevice) Flush() error { d.flushes++; d.pending = nil; return nil }
func (d *fakeDevice) Close() error { d.closed = true; return nil }

// echoValue replies to "<node> <reg>[ v]" with "<node> <reg> <value>".
func echoValue(value string) func(string) string {
	return func(cmd string) string {
		return commandPrefix(cmd) + " " + value + "\r\n"
	}
}

func testPort(t *testing.T, d *fakeDevice) *SerialPort {
	t.Helper()
	sp := New(Config{
		Port:            "/dev/fake",
		BaudRate:        115200,
		ResponseTimeout: 60 * time.Millisecond,
		ReopenBackoff:   time.Millisecond,
		Opener:          func() (Port, error) { d.opens++; return d, nil },
	}, nopLogger{})
	if err := sp.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return sp
}

func TestCommandPrefix(t *testing.T) {
	tests := []struct{ in, want string }{
		{"130 1067", "130 1067"},
		{"130 5002 3", "130 5002"},
		{"140 5036 1", "140 5036"},
		{"130", "130"},
	}
	for _, tt := range tests {
		if got := commandPrefix(tt.in); got != tt.want {
			t.Errorf("commandPrefix(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestTransactReturnsMatchingValue(t *testing.T) {
	d := &fakeDevice{respond: echoValue("3")}
	sp := testPort(t, d)

	got, err := sp.Transact(context.Background(), "130 1067")
	if err != nil {
		t.Fatalf("Transact() error = %v, want nil", err)
	}
	if got != "3" {
		t.Errorf("Transact() = %q, want %q", got, "3")
	}
	if len(d.writes) != 1 || d.writes[0] != "130 1067\r\n" {
		t.Errorf("writes = %q, want exactly one %q", d.writes, "130 1067\r\n")
	}
}

// A stale or unsolicited frame must be skipped, not cause the command to be
// re-sent. Re-sending is what makes writes non-idempotent.
func TestTransactSkipsUnmatchedFrames(t *testing.T) {
	d := &fakeDevice{respond: func(cmd string) string {
		return "140 9999 1\r\n130 5003 2\r\n" + commandPrefix(cmd) + " 3\r\n"
	}}
	sp := testPort(t, d)

	got, err := sp.Transact(context.Background(), "130 1067")
	if err != nil {
		t.Fatalf("Transact() error = %v, want nil", err)
	}
	if got != "3" {
		t.Errorf("Transact() = %q, want %q", got, "3")
	}
	if len(d.writes) != 1 {
		t.Errorf("wrote %d times, want 1 (command must not be re-sent)", len(d.writes))
	}
}

func TestTransactFlushesBeforeWriting(t *testing.T) {
	d := &fakeDevice{respond: echoValue("7")}
	sp := testPort(t, d)

	// Stale bytes on the wire and a stale partial frame in the software buffer.
	d.pending = []byte("130 1067 99\r\n130 10")
	sp.fr.buf = append(sp.fr.buf, "130 10"...)

	got, err := sp.Transact(context.Background(), "130 1067")
	if err != nil {
		t.Fatalf("Transact() error = %v, want nil", err)
	}
	if got != "7" {
		t.Errorf("Transact() = %q, want %q (stale data must be discarded)", got, "7")
	}
	if d.flushes == 0 {
		t.Error("Flush() was never called; stale input was not discarded")
	}
	if len(sp.fr.buf) != 0 {
		t.Errorf("software buffer = %q, want empty after flush", sp.fr.buf)
	}
}

func TestTransactTimesOutWhenSilent(t *testing.T) {
	d := &fakeDevice{} // never responds
	sp := testPort(t, d)

	_, err := sp.Transact(context.Background(), "130 1067")
	if !errors.Is(err, ErrReadTimeout) {
		t.Fatalf("Transact() error = %v, want ErrReadTimeout", err)
	}
	if !sp.IsHealthy() {
		t.Error("IsHealthy() = false after a timeout, want true (a timeout is not a dead port)")
	}
}

func TestTransactHonoursContextCancellation(t *testing.T) {
	d := &fakeDevice{} // never responds
	sp := testPort(t, d)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := sp.Transact(ctx, "130 1067")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Transact() error = %v, want context.Canceled", err)
	}
}

func TestTransactMarksPortUnhealthyOnIOError(t *testing.T) {
	d := &fakeDevice{readErr: syscall.EIO}
	sp := testPort(t, d)

	_, err := sp.Transact(context.Background(), "130 1067")
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("Transact() error = %v, want syscall.EIO", err)
	}
	if sp.IsHealthy() {
		t.Error("IsHealthy() = true after an I/O error, want false")
	}
}

func TestTransactOnClosedPort(t *testing.T) {
	d := &fakeDevice{respond: echoValue("3")}
	sp := testPort(t, d)
	if err := sp.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err := sp.Transact(context.Background(), "130 1067")
	if !errors.Is(err, ErrPortNotOpen) {
		t.Fatalf("Transact() error = %v, want ErrPortNotOpen", err)
	}
}

// The supervisor owns reopening, so a dead port recovers without every
// transaction attempting its own close/reopen cycle.
func TestSuperviseReopensUnhealthyPort(t *testing.T) {
	d := &fakeDevice{respond: echoValue("3")}
	sp := testPort(t, d)

	sp.markUnhealthy(syscall.EIO)
	if sp.IsHealthy() {
		t.Fatal("IsHealthy() = true after markUnhealthy")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go sp.Supervise(ctx)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if sp.IsHealthy() {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sp.IsHealthy() {
		t.Fatal("Supervise() did not recover the port")
	}
	if d.opens < 2 {
		t.Errorf("opens = %d, want >= 2 (initial open plus a reopen)", d.opens)
	}
}

// Reopening must not be attempted while the port is healthy: closing and
// reopening a USB tty toggles DTR/RTS, which many devices treat as a reset.
func TestSuperviseLeavesHealthyPortAlone(t *testing.T) {
	d := &fakeDevice{respond: echoValue("3")}
	sp := testPort(t, d)
	opensAfterSetup := d.opens

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	sp.Supervise(ctx)

	if d.opens != opensAfterSetup {
		t.Errorf("opens = %d, want %d (healthy port must not be reopened)", d.opens, opensAfterSetup)
	}
}

// Send is used for register writes, which the unit does not reliably echo.
func TestSendWritesWithoutWaitingForAResponse(t *testing.T) {
	d := &fakeDevice{} // never responds
	sp := testPort(t, d)

	start := time.Now()
	if err := sp.Send(context.Background(), "130 5002 3"); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}
	elapsed := time.Since(start)

	if elapsed > 50*time.Millisecond {
		t.Errorf("Send() took %v; it must not wait for a response", elapsed)
	}
	if len(d.writes) != 1 || d.writes[0] != "130 5002 3\r\n" {
		t.Errorf("writes = %q, want exactly one %q", d.writes, "130 5002 3\r\n")
	}
	if d.flushes == 0 {
		t.Error("Send() did not discard stale input before writing")
	}
}

func TestSendOnClosedPort(t *testing.T) {
	sp := testPort(t, &fakeDevice{})
	if err := sp.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := sp.Send(context.Background(), "130 5002 3"); !errors.Is(err, ErrPortNotOpen) {
		t.Fatalf("Send() error = %v, want ErrPortNotOpen", err)
	}
}

func TestSendHonoursContextCancellation(t *testing.T) {
	sp := testPort(t, &fakeDevice{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := sp.Send(ctx, "130 5002 3"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Send() error = %v, want context.Canceled", err)
	}
}
