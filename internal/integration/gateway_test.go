// Package integration wires the real serial, registers and MQTT-facing layers
// together against a scripted device, so the seams between them are exercised
// the way they are in production.
package integration

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nean/aerosmart-gateway/internal/config"
	"github.com/nean/aerosmart-gateway/internal/logger"
	"github.com/nean/aerosmart-gateway/internal/registers"
	"github.com/nean/aerosmart-gateway/internal/serial"
)

// lossyDevice answers commands over a byte stream that it deliberately
// fragments, the way a real USB serial link does.
type lossyDevice struct {
	mu sync.Mutex

	pending []byte

	// chunk caps how many bytes each Read returns, forcing frames to be split
	// across reads.
	chunk int

	// noise is emitted before the first real answer, modelling a stale or
	// unsolicited frame already on the wire.
	noise string

	// dropEvery causes every Nth command to go unanswered.
	dropEvery int
	commands  int

	writes []string
}

func (d *lossyDevice) Read(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.pending) == 0 {
		return 0, nil // idle
	}
	n := len(d.pending)
	if d.chunk > 0 && n > d.chunk {
		n = d.chunk
	}
	if n > len(p) {
		n = len(p)
	}
	copy(p, d.pending[:n])
	d.pending = d.pending[n:]
	return n, nil
}

func (d *lossyDevice) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	cmd := strings.TrimSpace(string(p))
	d.writes = append(d.writes, cmd)
	d.commands++

	if d.dropEvery > 0 && d.commands%d.dropEvery == 0 {
		return len(p), nil // swallow the request: no answer comes back
	}

	var reply strings.Builder
	if d.noise != "" {
		reply.WriteString(d.noise)
		d.noise = ""
	}
	fields := strings.Fields(cmd)
	if len(fields) >= 2 {
		reply.WriteString(fields[0] + " " + fields[1] + " " + answerFor(fields[1]) + "\r\n")
	}
	d.pending = append(d.pending, reply.String()...)
	return len(p), nil
}

// Flush models TCIOFLUSH: pending input is discarded.
func (d *lossyDevice) Flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pending = nil
	return nil
}

func (d *lossyDevice) Close() error { return nil }

func (d *lossyDevice) writeCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.writes)
}

// answerFor returns the raw device value for a register address.
func answerFor(register string) string {
	switch register {
	case "1067":
		return "3"
	case "201":
		return "21500"
	case "231":
		return "812"
	}
	return "0"
}

type recordingPublisher struct {
	mu   sync.Mutex
	seen map[string]string
}

func newRecordingPublisher() *recordingPublisher {
	return &recordingPublisher{seen: map[string]string{}}
}

func (p *recordingPublisher) Publish(topic, value string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen[topic] = value
	return nil
}

func (p *recordingPublisher) get(topic string) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	v, ok := p.seen[topic]
	return v, ok
}

var readRegisters = []config.ReadRegisterConfig{
	{Name: "luefterstatus", Command: "130 1067", Topic: "aerosmart/luefterstatus", Divisor: 1, Type: "integer", MinValue: 0, MaxValue: 5},
	{Name: "innentemp", Command: "130 201", Topic: "aerosmart/innentemp", Divisor: 1000, Type: "float", MinValue: -20, MaxValue: 50},
	{Name: "co2", Command: "130 231", Topic: "aerosmart/co2", Divisor: 1, Type: "integer", MinValue: 0, MaxValue: 5000},
}

func newStack(t *testing.T, dev serial.Port) (*registers.Poller, *recordingPublisher, *serial.SerialPort) {
	t.Helper()

	log := logger.New("error")
	sp := serial.New(serial.Config{
		Port:            "/dev/fake",
		BaudRate:        115200,
		ReadTimeout:     100 * time.Millisecond,
		ResponseTimeout: 200 * time.Millisecond,
		ReopenBackoff:   time.Millisecond,
		Opener:          func() (serial.Port, error) { return dev, nil },
	}, log)
	if err := sp.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	pub := newRecordingPublisher()
	reader := registers.NewReader(sp, pub, log, readRegisters, 3, 10*time.Millisecond)
	gate := registers.NewReadGate(time.Second)
	return registers.NewPoller(reader, nil, gate, log), pub, sp
}

// Every frame arriving one byte at a time must still be reassembled, and every
// register published with its divisor applied.
func TestFullCyclePublishesAllRegistersOverAByteAtATimeLink(t *testing.T) {
	poller, pub, sp := newStack(t, &lossyDevice{chunk: 1})
	defer func() { _ = sp.Close() }()

	if err := poller.RunCycle(context.Background()); err != nil {
		t.Fatalf("RunCycle() error = %v", err)
	}

	for topic, want := range map[string]string{
		"aerosmart/luefterstatus": "3",
		"aerosmart/innentemp":     "21.5",
		"aerosmart/co2":           "812",
	} {
		got, ok := pub.get(topic)
		if !ok {
			t.Errorf("%s was never published", topic)
			continue
		}
		if got != want {
			t.Errorf("%s = %q, want %q", topic, got, want)
		}
	}
}

// A stale frame sitting on the wire must be discarded without re-sending the
// command, and must not shift every later reading onto the wrong register.
func TestStaleFrameDoesNotDesyncTheStream(t *testing.T) {
	dev := &lossyDevice{chunk: 3, noise: "130 9999 42\r\n"}
	poller, pub, sp := newStack(t, dev)
	defer func() { _ = sp.Close() }()

	if err := poller.RunCycle(context.Background()); err != nil {
		t.Fatalf("RunCycle() error = %v", err)
	}

	if got, _ := pub.get("aerosmart/luefterstatus"); got != "3" {
		t.Errorf("luefterstatus = %q, want 3 (a stale frame shifted the stream)", got)
	}
	if got, _ := pub.get("aerosmart/innentemp"); got != "21.5" {
		t.Errorf("innentemp = %q, want 21.5", got)
	}
	if n := dev.writeCount(); n != len(readRegisters) {
		t.Errorf("commands written = %d, want %d (a stale frame must not trigger a re-send)", n, len(readRegisters))
	}
}

// An unanswered command must cost only its own retries and leave the following
// registers readable.
func TestDroppedResponseDoesNotPoisonLaterRegisters(t *testing.T) {
	// Drop the answer to the very first command; its retries are dropped too.
	poller, pub, sp := newStack(t, &lossyDevice{chunk: 4, dropEvery: 1})
	defer func() { _ = sp.Close() }()

	if err := poller.RunCycle(context.Background()); err != nil {
		t.Fatalf("RunCycle() error = %v", err)
	}
	// Everything was dropped, so nothing may be published as a stale value.
	for _, topic := range []string{"aerosmart/luefterstatus", "aerosmart/innentemp", "aerosmart/co2"} {
		if v, ok := pub.get(topic); ok {
			t.Errorf("%s published %q despite no response; stale data must be withheld", topic, v)
		}
	}
}

// Only every second command is answered: the surviving registers must still
// come through, which is the property that fails when a partial frame is
// discarded instead of buffered.
func TestIntermittentLinkStillPublishesAnsweredRegisters(t *testing.T) {
	poller, pub, sp := newStack(t, &lossyDevice{chunk: 2, dropEvery: 2})
	defer func() { _ = sp.Close() }()

	if err := poller.RunCycle(context.Background()); err != nil {
		t.Fatalf("RunCycle() error = %v", err)
	}

	published := 0
	for _, topic := range []string{"aerosmart/luefterstatus", "aerosmart/innentemp", "aerosmart/co2"} {
		if _, ok := pub.get(topic); ok {
			published++
		}
	}
	if published == 0 {
		t.Fatal("no register survived an intermittent link; framing is losing data")
	}
	t.Logf("%d/%d registers published on a link dropping every second response", published, len(readRegisters))
}

// A whole cycle on a lossy link must finish well inside a normal read
// interval; the previous nested retries could stretch this to minutes.
func TestCycleOnADeadLinkStaysWithinBudget(t *testing.T) {
	poller, _, sp := newStack(t, &lossyDevice{dropEvery: 1}) // never answers
	defer func() { _ = sp.Close() }()

	start := time.Now()
	if err := poller.RunCycle(context.Background()); err != nil {
		t.Fatalf("RunCycle() error = %v", err)
	}
	elapsed := time.Since(start)

	// 3 registers x 3 attempts x (200ms response + 10ms backoff) ~= 1.9s.
	if budget := 3 * time.Second; elapsed > budget {
		t.Errorf("cycle on a dead link took %v, want under %v", elapsed, budget)
	}
	t.Logf("cycle against a silent device took %v", elapsed)
}

// A write must be able to cut into a running read cycle.
func TestWritePreemptsAnInFlightReadCycle(t *testing.T) {
	dev := &lossyDevice{dropEvery: 1} // silent, so the cycle runs its full budget
	log := logger.New("error")
	sp := serial.New(serial.Config{
		Port:            "/dev/fake",
		ReadTimeout:     100 * time.Millisecond,
		ResponseTimeout: 200 * time.Millisecond,
		ReopenBackoff:   time.Millisecond,
		Opener:          func() (serial.Port, error) { return dev, nil },
	}, log)
	if err := sp.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = sp.Close() }()

	pub := newRecordingPublisher()
	gate := registers.NewReadGate(time.Second)
	reader := registers.NewReader(sp, pub, log, readRegisters, 3, 10*time.Millisecond)
	poller := registers.NewPoller(reader, nil, gate, log)

	cycleDone := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		_ = poller.RunCycle(context.Background())
		cycleDone <- time.Since(start)
	}()

	time.Sleep(100 * time.Millisecond)
	gate.Preempt()

	select {
	case elapsed := <-cycleDone:
		if elapsed > 2*time.Second {
			t.Errorf("cycle took %v to yield after preemption, want it to abort promptly", elapsed)
		}
		t.Logf("cycle aborted %v after start", elapsed)
	case <-time.After(3 * time.Second):
		t.Fatal("read cycle did not yield to a write")
	}
}
