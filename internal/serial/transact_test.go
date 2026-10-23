package serial

import (
	"context"
	"errors"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	bugst "go.bug.st/serial"
)

// fakeDevice answers commands the way the real unit does: a written command
// queues a response frame to be read back. It also models TCIFLUSH, which
// discards input that has not been read yet.
type fakeDevice struct {
	pending []byte
	respond func(cmd string) string
	writes  []string
	resets  int
	opens   int
	readErr error
	closed  bool

	// busyReads is how many more reads return another device's traffic,
	// modelling a panel talking on the shared bus. A queued reply still
	// takes priority, as it would on the wire once the panel pauses.
	busyReads int

	// ops records the order of port operations: "busy", "reset", "write",
	// "drain" and "timeout=<d>".
	ops []string

	// drainEINTR is how many Drain calls fail with EINTR before one succeeds.
	drainEINTR int
}

func (d *fakeDevice) Read(p []byte) (int, error) {
	if d.readErr != nil {
		return 0, d.readErr
	}
	if len(d.pending) == 0 && d.busyReads > 0 {
		d.busyReads--
		d.ops = append(d.ops, "busy")
		return copy(p, "140 9999 1\r\n"), nil
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
	d.ops = append(d.ops, "write")
	if d.respond != nil {
		d.pending = append(d.pending, d.respond(cmd)...)
	}
	return len(p), nil
}

// ResetInputBuffer models TCIFLUSH: undelivered input is discarded.
func (d *fakeDevice) ResetInputBuffer() error {
	d.resets++
	d.ops = append(d.ops, "reset")
	d.pending = nil
	return nil
}

func (d *fakeDevice) Drain() error {
	d.ops = append(d.ops, "drain")
	if d.drainEINTR > 0 {
		d.drainEINTR--
		return syscall.EINTR
	}
	return nil
}

func (d *fakeDevice) SetReadTimeout(t time.Duration) error {
	d.ops = append(d.ops, "timeout="+t.String())
	return nil
}

func (d *fakeDevice) Close() error { d.closed = true; return nil }

// opsFrom returns the recorded operations of the given kinds, in order.
func (d *fakeDevice) opsFrom(kinds ...string) []string {
	var out []string
	for _, op := range d.ops {
		if slices.Contains(kinds, op) {
			out = append(out, op)
		}
	}
	return out
}

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
		BusQuietMaxWait: 30 * time.Millisecond,
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

func TestTransactDiscardsStaleInputBeforeWriting(t *testing.T) {
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
	if d.resets == 0 {
		t.Error("ResetInputBuffer() was never called; stale input was not discarded")
	}
	if len(sp.fr.buf) != 0 {
		t.Errorf("software buffer = %q, want empty after the reset", sp.fr.buf)
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
	if d.resets == 0 {
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

// Send must not return while the command is still queued in the kernel: the
// next exchange would start, and a write that has not left the port yet could
// be lost or collide with it. Draining is what makes "sent" mean sent.
func TestSendDrainsTheCommandBeforeReturning(t *testing.T) {
	d := &fakeDevice{}
	sp := testPort(t, d)

	if err := sp.Send(context.Background(), "130 5002 3"); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}
	if got := d.opsFrom("write", "drain"); !slices.Equal(got, []string{"write", "drain"}) {
		t.Errorf("operations = %v, want write then drain", got)
	}
}

func TestTransactDrainsTheCommandBeforeWaitingForTheReply(t *testing.T) {
	d := &fakeDevice{respond: echoValue("3")}
	sp := testPort(t, d)

	if _, err := sp.Transact(context.Background(), "130 1067"); err != nil {
		t.Fatalf("Transact() error = %v, want nil", err)
	}
	if got := d.opsFrom("write", "drain"); !slices.Equal(got, []string{"write", "drain"}) {
		t.Errorf("operations = %v, want write then drain", got)
	}
}

// Another device shares the bus. Transmitting while it is mid-frame corrupts
// both, so a command waits until the line has been quiet for a moment.
func TestTransactWaitsForAQuietBusBeforeWriting(t *testing.T) {
	d := &fakeDevice{respond: echoValue("3"), busyReads: 3}
	sp := testPort(t, d)

	got, err := sp.Transact(context.Background(), "130 1067")
	if err != nil {
		t.Fatalf("Transact() error = %v, want nil", err)
	}
	if got != "3" {
		t.Errorf("Transact() = %q, want %q", got, "3")
	}
	want := []string{"busy", "busy", "busy", "write"}
	if ops := d.opsFrom("busy", "write"); !slices.Equal(ops, want) {
		t.Errorf("operations = %v, want %v (the bus must be quiet before writing)", ops, want)
	}
}

func TestSendWaitsForAQuietBusBeforeWriting(t *testing.T) {
	d := &fakeDevice{busyReads: 2}
	sp := testPort(t, d)

	if err := sp.Send(context.Background(), "130 5002 3"); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}
	want := []string{"busy", "busy", "write"}
	if ops := d.opsFrom("busy", "write"); !slices.Equal(ops, want) {
		t.Errorf("operations = %v, want %v (the bus must be quiet before writing)", ops, want)
	}
}

// The quiet-bus wait is bounded: a device that never stops talking must not
// stall every command forever.
func TestBusyBusDelaysACommandOnlyUpToTheMaximumWait(t *testing.T) {
	d := &fakeDevice{respond: echoValue("3"), busyReads: 1 << 30}
	sp := testPort(t, d)

	start := time.Now()
	got, err := sp.Transact(context.Background(), "130 1067")
	if err != nil {
		t.Fatalf("Transact() error = %v, want nil", err)
	}
	if got != "3" {
		t.Errorf("Transact() = %q, want %q", got, "3")
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("Transact() took %v on a busy bus, want it bounded by the 30ms maximum wait", elapsed)
	}
}

// slowPort blocks for its configured read timeout when it has nothing to
// deliver, the way a real tty does.
type slowPort struct {
	mu      sync.Mutex
	timeout time.Duration
}

func (p *slowPort) Read([]byte) (int, error) {
	p.mu.Lock()
	t := p.timeout
	p.mu.Unlock()
	time.Sleep(t)
	return 0, nil
}
func (p *slowPort) Write(b []byte) (int, error) { return len(b), nil }
func (p *slowPort) ResetInputBuffer() error     { return nil }
func (p *slowPort) Drain() error                { return nil }
func (p *slowPort) Close() error                { return nil }
func (p *slowPort) SetReadTimeout(t time.Duration) error {
	p.mu.Lock()
	p.timeout = t
	p.mu.Unlock()
	return nil
}

// A read that starts just before the response deadline must not block for a
// whole read timeout past it: that overrun is what the cycle budget would
// otherwise have to charge for on every attempt.
func TestTransactDoesNotBlockPastItsResponseDeadline(t *testing.T) {
	sp := New(Config{
		Port:            "/dev/fake",
		ReadTimeout:     200 * time.Millisecond,
		ResponseTimeout: 250 * time.Millisecond,
		BusQuietGap:     10 * time.Millisecond,
		Opener:          func() (Port, error) { return &slowPort{}, nil },
	}, nopLogger{})
	if err := sp.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	start := time.Now()
	_, err := sp.Transact(context.Background(), "130 1067")
	elapsed := time.Since(start)

	if !errors.Is(err, ErrReadTimeout) {
		t.Fatalf("Transact() error = %v, want ErrReadTimeout", err)
	}
	// Quiet gap (10ms) + response timeout (250ms), plus scheduling slack.
	// Without the cap the second read runs to 400ms.
	if elapsed > 330*time.Millisecond {
		t.Errorf("Transact() took %v, want it bounded by the 250ms response timeout", elapsed)
	}
}

// go.bug.st/serial reports a vanished or unusable device as a *PortError that
// does not wrap the errno, so it must be recognised explicitly or the
// supervisor never reopens the port.
func TestIsPortErrorRecognisesDriverPortErrors(t *testing.T) {
	// /dev/null is not a tty, so the driver rejects it as an invalid port.
	_, err := bugst.Open("/dev/null", &bugst.Mode{BaudRate: 115200})
	var pe *bugst.PortError
	if !errors.As(err, &pe) {
		t.Skipf("opening /dev/null returned %v, not a *PortError", err)
	}
	if !IsPortError(err) {
		t.Errorf("IsPortError(%v) = false, want true", err)
	}
}

// A port that returns at once with no data (a non-blocking transport) must
// not make the bus look quiet: silence counts only once it has lasted a gap.
func TestQuietBusNeedsAGapOfRealSilence(t *testing.T) {
	d := &fakeDevice{respond: echoValue("3")} // reads return immediately
	sp := New(Config{
		Port:            "/dev/fake",
		ResponseTimeout: 60 * time.Millisecond,
		BusQuietGap:     40 * time.Millisecond,
		BusQuietMaxWait: 100 * time.Millisecond,
		Opener:          func() (Port, error) { return d, nil },
	}, nopLogger{})
	if err := sp.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	start := time.Now()
	if _, err := sp.Transact(context.Background(), "130 1067"); err != nil {
		t.Fatalf("Transact() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Errorf("Transact() took %v, want at least the 40ms quiet gap before transmitting", elapsed)
	}
}

// The wait for a quiet bus must end at bus_quiet_max_wait, not a whole gap
// later: the cycle budget charges bus_quiet_max_wait per attempt.
func TestQuietBusWaitEndsAtTheMaximumWait(t *testing.T) {
	sp := New(Config{
		Port:            "/dev/fake",
		ReadTimeout:     10 * time.Millisecond, // so the 10ms response timeout is not raised
		ResponseTimeout: 10 * time.Millisecond,
		BusQuietGap:     400 * time.Millisecond,
		BusQuietMaxWait: 500 * time.Millisecond,
		Opener:          func() (Port, error) { return &chattyPort{every: 300 * time.Millisecond}, nil },
	}, nopLogger{})
	if err := sp.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	start := time.Now()
	_, _ = sp.Transact(context.Background(), "130 1067")
	// A byte arrives at 300ms, so the bus never stays quiet for 400ms. The
	// wait must stop at 500ms (then a 10ms response timeout). Without the cap,
	// the read started at 300ms runs until the next byte at 600ms.
	if elapsed := time.Since(start); elapsed > 560*time.Millisecond {
		t.Errorf("Transact() took %v, want the quiet-bus wait to stop at 500ms", elapsed)
	}
}

// chattyPort delivers a byte every so often, like a panel that never pauses
// for long, and blocks up to its read timeout in between.
type chattyPort struct {
	mu      sync.Mutex
	every   time.Duration
	timeout time.Duration
	last    time.Time
}

func (p *chattyPort) Read(b []byte) (int, error) {
	p.mu.Lock()
	t, every, last := p.timeout, p.every, p.last
	p.mu.Unlock()
	if last.IsZero() {
		last = time.Now()
	}
	next := last.Add(every)
	if wait := time.Until(next); wait > t {
		time.Sleep(t)
		return 0, nil
	} else if wait > 0 {
		time.Sleep(wait)
	}
	p.mu.Lock()
	p.last = time.Now()
	p.mu.Unlock()
	b[0] = 'x'
	return 1, nil
}
func (p *chattyPort) Write(b []byte) (int, error) { return len(b), nil }
func (p *chattyPort) ResetInputBuffer() error     { return nil }
func (p *chattyPort) Drain() error                { return nil }
func (p *chattyPort) Close() error                { return nil }
func (p *chattyPort) SetReadTimeout(t time.Duration) error {
	p.mu.Lock()
	p.timeout = t
	p.mu.Unlock()
	return nil
}

// The serial layer's own default must match the documented one the cycle
// budget assumes.
func TestDefaultQuietMaxWaitMatchesTheDocumentedDefault(t *testing.T) {
	cfg := &Config{}
	cfg.applyDefaults(nopLogger{})
	if cfg.BusQuietMaxWait != 100*time.Millisecond {
		t.Errorf("BusQuietMaxWait default = %v, want 100ms", cfg.BusQuietMaxWait)
	}
}

// tcdrain blocks, and the Go runtime signals its own threads (async
// preemption), so the ioctl can return EINTR. The command has been written by
// then; failing the exchange made reads fail and writes go out twice.
func TestTransactRetriesADrainInterruptedBySignal(t *testing.T) {
	d := &fakeDevice{respond: echoValue("3"), drainEINTR: 2}
	sp := testPort(t, d)

	got, err := sp.Transact(context.Background(), "130 5003")
	if err != nil {
		t.Fatalf("Transact() error = %v, want the interrupted drain retried", err)
	}
	if got != "3" {
		t.Errorf("Transact() = %q, want 3", got)
	}
	if len(d.writes) != 1 {
		t.Errorf("command written %d times, want 1", len(d.writes))
	}
}

func TestSendRetriesADrainInterruptedBySignal(t *testing.T) {
	d := &fakeDevice{drainEINTR: 1}
	sp := testPort(t, d)

	if err := sp.Send(context.Background(), "130 5002 2"); err != nil {
		t.Fatalf("Send() error = %v, want the interrupted drain retried", err)
	}
	if len(d.writes) != 1 {
		t.Errorf("command written %d times, want 1", len(d.writes))
	}
}
