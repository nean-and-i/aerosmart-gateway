// Package integration wires the real serial, registers and MQTT-facing layers
// together against a scripted device, so the seams between them are exercised
// the way they are in production.
package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

// ResetInputBuffer models TCIFLUSH: pending input is discarded.
func (d *lossyDevice) ResetInputBuffer() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pending = nil
	return nil
}

func (d *lossyDevice) Close() error                       { return nil }
func (d *lossyDevice) Drain() error                       { return nil }
func (d *lossyDevice) SetReadTimeout(time.Duration) error { return nil }

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
	return registers.NewPoller(reader, nil, gate, log, nil), pub, sp
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
	dev := &lossyDevice{chunk: 3}
	poller, pub, sp := newStack(t, dev)
	defer func() { _ = sp.Close() }()

	// A first cycle reads every register twice to confirm it. Prime the
	// trusted values, so the cycle under test is one command per register.
	if err := poller.RunCycle(context.Background()); err != nil {
		t.Fatalf("priming RunCycle() error = %v", err)
	}
	dev.mu.Lock()
	dev.noise = "130 9999 42\r\n"
	before := len(dev.writes)
	dev.mu.Unlock()

	if err := poller.RunCycle(context.Background()); err != nil {
		t.Fatalf("RunCycle() error = %v", err)
	}

	if got, _ := pub.get("aerosmart/luefterstatus"); got != "3" {
		t.Errorf("luefterstatus = %q, want 3 (a stale frame shifted the stream)", got)
	}
	if got, _ := pub.get("aerosmart/innentemp"); got != "21.5" {
		t.Errorf("innentemp = %q, want 21.5", got)
	}
	if n := dev.writeCount() - before; n != len(readRegisters) {
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
	poller := registers.NewPoller(reader, nil, gate, log, nil)

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

// End-to-end guard for the regression the review caught: a write preempts a
// cycle, whose context.Canceled must not be mistaken for shutdown. Polling has
// to carry on afterwards.
func TestPollingSurvivesAWritePreemption(t *testing.T) {
	dev := &lossyDevice{chunk: 2}
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
	gate := registers.NewReadGate(50 * time.Millisecond)
	reader := registers.NewReader(sp, pub, log, readRegisters, 3, 10*time.Millisecond)
	poller := registers.NewPoller(reader, nil, gate, log, nil)

	// First cycle succeeds.
	if err := poller.RunCycle(context.Background()); err != nil {
		t.Fatalf("first RunCycle() error = %v", err)
	}

	// A write arrives and preempts; the skipped cycle is reported as such so
	// the caller retries sooner than a whole interval.
	gate.Preempt()
	if err := poller.RunCycle(context.Background()); !errors.Is(err, registers.ErrCycleSkipped) {
		t.Fatalf("suppressed RunCycle() error = %v, want ErrCycleSkipped", err)
	}
	gate.Release()

	// Polling must still work afterwards.
	pub.mu.Lock()
	pub.seen = map[string]string{}
	pub.mu.Unlock()

	if err := poller.RunCycle(context.Background()); err != nil {
		t.Fatalf("RunCycle() after preemption error = %v", err)
	}
	if v, ok := pub.get("aerosmart/luefterstatus"); !ok || v != "3" {
		t.Errorf("luefterstatus after preemption = %q (%v), want 3; polling did not resume", v, ok)
	}
}

// silentPort never answers, so a transaction runs its full response budget.
type silentPort struct{}

func (silentPort) Read([]byte) (int, error)           { return 0, nil }
func (silentPort) Write(p []byte) (int, error)        { return len(p), nil }
func (silentPort) ResetInputBuffer() error            { return nil }
func (silentPort) Drain() error                       { return nil }
func (silentPort) SetReadTimeout(time.Duration) error { return nil }
func (silentPort) Close() error                       { return nil }

// config.EffectiveResponseTimeout predicts what the serial layer will actually
// wait, and the cycle-budget check depends on that prediction. The two apply
// the same raising rule separately, so this pins them together: an
// under-estimate would let a config pass validation and then overrun its read
// interval.
func TestConfigResponseBudgetMatchesSerialBehaviour(t *testing.T) {
	cases := []struct{ readTimeout, responseTimeout int }{
		{200, 300}, // response timeout dominates
		{300, 100}, // read timeout dominates and is raised
		{250, 100}, // no rounding: 250 is used as is
		{10, 50},   // short timeouts are honoured exactly
	}

	for _, tc := range cases {
		path := filepath.Join(t.TempDir(), "config.yaml")
		body := fmt.Sprintf("mqtt:\n  broker: \"x\"\nserial:\n  read_timeout: %d\n  response_timeout: %d\n",
			tc.readTimeout, tc.responseTimeout)
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
		ac, err := config.LoadAppConfig(path)
		if err != nil {
			t.Fatalf("LoadAppConfig() error = %v", err)
		}
		predicted := time.Duration(ac.EffectiveResponseTimeout()) * time.Millisecond

		sp := serial.New(serial.Config{
			Port:            "/dev/fake",
			ReadTimeout:     time.Duration(ac.Serial.ReadTimeout) * time.Millisecond,
			ResponseTimeout: time.Duration(ac.Serial.ResponseTimeout) * time.Millisecond,
			Opener:          func() (serial.Port, error) { return silentPort{}, nil },
		}, logger.New("error"))
		if err := sp.Open(); err != nil {
			t.Fatalf("Open() error = %v", err)
		}

		start := time.Now()
		_, _ = sp.Transact(context.Background(), "130 1067")
		elapsed := time.Since(start)
		_ = sp.Close()

		// The real wait must not exceed what config predicted, or the budget
		// check under-estimates the cycle.
		if elapsed > predicted+150*time.Millisecond {
			t.Errorf("read_timeout=%d response_timeout=%d: transaction took %v, config predicted %v",
				tc.readTimeout, tc.responseTimeout, elapsed, predicted)
		}
		// And it must actually use most of the budget, or the prediction is
		// needlessly pessimistic.
		if elapsed < predicted/2 {
			t.Errorf("read_timeout=%d response_timeout=%d: transaction took only %v, config predicted %v",
				tc.readTimeout, tc.responseTimeout, elapsed, predicted)
		}
	}
}

// aerosmartDevice models what the real unit was observed to do:
//   - it answers reads promptly,
//   - it applies writes but does NOT reliably echo the command,
//   - it continuously broadcasts unsolicited status frames, including from
//     node addresses that are not configured registers at all.
type aerosmartDevice struct {
	mu      sync.Mutex
	pending []byte
	applied map[string]string // register address -> value actually set
	writes  []string
}

func newAerosmartDevice() *aerosmartDevice {
	return &aerosmartDevice{applied: map[string]string{}}
}

// writeToReadRegister maps a write register's address to the address the value
// is read back from.
var writeToReadRegister = map[string]string{
	"5002": "5003", // luefterstufe -> lueftermode
	"5036": "1039", // boilerheizstab -> boilerheizstabanforderungstatus
}

func (d *aerosmartDevice) Read(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.pending) == 0 {
		return 0, nil
	}
	n := copy(p, d.pending)
	d.pending = d.pending[n:]
	return n, nil
}

func (d *aerosmartDevice) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	cmd := strings.TrimSpace(string(p))
	d.writes = append(d.writes, cmd)
	fields := strings.Fields(cmd)

	// Unsolicited chatter always precedes whatever we do next.
	d.pending = append(d.pending, "120 1048 1\r\n130 1066 1\r\n"...)

	switch {
	case len(fields) == 3: // a write: applied, but never echoed
		// A setpoint written to one address reads back from another: the real
		// unit takes luefterstufe on 5002 and reports it on 5003.
		target := fields[1]
		if readBack, ok := writeToReadRegister[target]; ok {
			target = readBack
		}
		d.applied[target] = fields[2]
		// The unit broadcasts the resulting change instead of an echo.
		d.pending = append(d.pending, "130 1092 3492\r\n"...)
	case len(fields) == 2: // a read: answered
		value, ok := d.applied[fields[1]]
		if !ok {
			value = answerFor(fields[1])
		}
		d.pending = append(d.pending, fields[0]+" "+fields[1]+" "+value+"\r\n"...)
	}
	return len(p), nil
}

func (d *aerosmartDevice) ResetInputBuffer() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pending = nil
	return nil
}

func (d *aerosmartDevice) Close() error                       { return nil }
func (d *aerosmartDevice) Drain() error                       { return nil }
func (d *aerosmartDevice) SetReadTimeout(time.Duration) error { return nil }

func (d *aerosmartDevice) writeCount(command string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, w := range d.writes {
		if w == command {
			n++
		}
	}
	return n
}

// A write against the real device's behaviour must succeed, be sent once, and
// be confirmed by reading the register back.
func TestWriteAgainstAnEcholessDevice(t *testing.T) {
	dev := newAerosmartDevice()
	log := logger.New("error")
	sp := serial.New(serial.Config{
		Port:            "/dev/fake",
		ReadTimeout:     100 * time.Millisecond,
		ResponseTimeout: 300 * time.Millisecond,
		ReopenBackoff:   time.Millisecond,
		Opener:          func() (serial.Port, error) { return dev, nil },
	}, log)
	if err := sp.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = sp.Close() }()

	pub := newRecordingPublisher()
	gate := registers.NewReadGate(time.Second)
	readRegs := []config.ReadRegisterConfig{
		{Name: "lueftermode", Command: "130 5003", Topic: "aerosmart/lueftermode",
			Divisor: 1, Type: "integer", MinValue: 0, MaxValue: 5},
	}
	writeRegs := []config.WriteRegisterConfig{{
		Name: "luefterstufe", CommandTemplate: "130 5002 {value}",
		SubscribeTopic: "dw/aerosmart/luefterstufe", Topic: "aerosmart/luefterstufe",
		MinValue: 0, MaxValue: 5, VerifyRegisters: []string{"lueftermode"},
	}}

	reader := registers.NewReader(sp, pub, log, readRegs, 3, 10*time.Millisecond)
	writer := registers.NewWriter(sp, pub, log, writeRegs, gate, 3, 10*time.Millisecond, time.Millisecond)
	writer.SetReader(reader)

	if err := writer.HandleMessage(context.Background(), "dw/aerosmart/luefterstufe", "3"); err != nil {
		t.Fatalf("HandleMessage() error = %v, want success against an echoless device", err)
	}

	if n := dev.writeCount("130 5002 3"); n != 1 {
		t.Errorf("command sent %d times, want 1 (a landed write must not be re-sent)", n)
	}
	if v, ok := pub.get("aerosmart/luefterstufe"); !ok || v != "3" {
		t.Errorf("state topic = %q (%v), want 3", v, ok)
	}
	// The read-back is what confirms it, and must reflect the new value.
	if v, ok := pub.get("aerosmart/lueftermode"); !ok || v != "3" {
		t.Errorf("verified lueftermode = %q (%v), want 3", v, ok)
	}
}

// Unsolicited chatter must not disturb a read cycle either.
func TestReadCycleIgnoresUnsolicitedChatter(t *testing.T) {
	dev := newAerosmartDevice()
	log := logger.New("error")
	sp := serial.New(serial.Config{
		Port:            "/dev/fake",
		ReadTimeout:     100 * time.Millisecond,
		ResponseTimeout: 300 * time.Millisecond,
		ReopenBackoff:   time.Millisecond,
		Opener:          func() (serial.Port, error) { return dev, nil },
	}, log)
	if err := sp.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = sp.Close() }()

	pub := newRecordingPublisher()
	reader := registers.NewReader(sp, pub, log, readRegisters, 3, 10*time.Millisecond)
	poller := registers.NewPoller(reader, nil, registers.NewReadGate(time.Second), log, nil)

	if err := poller.RunCycle(context.Background()); err != nil {
		t.Fatalf("RunCycle() error = %v", err)
	}
	for topic, want := range map[string]string{
		"aerosmart/luefterstatus": "3",
		"aerosmart/innentemp":     "21.5",
		"aerosmart/co2":           "812",
	} {
		if got, _ := pub.get(topic); got != want {
			t.Errorf("%s = %q, want %q despite interleaved broadcasts", topic, got, want)
		}
	}
}

// laggingDevice applies a write immediately to its setpoint register but lets
// the "actual stage" register trail, as the real unit does: 60ms after a stage
// change 130 1067 still reported the previous value.
type laggingDevice struct {
	mu      sync.Mutex
	pending []byte
	mode    string // 130 5003, updates at once
	status  string // 130 1067, updates after lag
	lagUpTo time.Time
	lag     time.Duration
}

func newLaggingDevice(lag time.Duration) *laggingDevice {
	return &laggingDevice{mode: "1", status: "1", lag: lag}
}

func (d *laggingDevice) Read(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.pending) == 0 {
		return 0, nil
	}
	n := copy(p, d.pending)
	d.pending = d.pending[n:]
	return n, nil
}

func (d *laggingDevice) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	fields := strings.Fields(strings.TrimSpace(string(p)))
	switch {
	case len(fields) == 3 && fields[1] == "5002": // setpoint write, never echoed
		d.mode = fields[2]
		d.lagUpTo = time.Now().Add(d.lag)
	case len(fields) == 2:
		var value string
		switch fields[1] {
		case "5003":
			value = d.mode
		case "1067":
			if time.Now().Before(d.lagUpTo) {
				value = d.status // still the old stage
			} else {
				d.status = d.mode
				value = d.status
			}
		default:
			value = "0"
		}
		d.pending = append(d.pending, fields[0]+" "+fields[1]+" "+value+"\r\n"...)
	}
	return len(p), nil
}

func (d *laggingDevice) ResetInputBuffer() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pending = nil
	return nil
}

func (d *laggingDevice) Close() error                       { return nil }
func (d *laggingDevice) Drain() error                       { return nil }
func (d *laggingDevice) SetReadTimeout(time.Duration) error { return nil }

// The immediate read-back publishes the stale stage; the settle re-read must
// correct it without waiting for the next full read cycle.
func TestSettleRereadCorrectsALaggingValue(t *testing.T) {
	dev := newLaggingDevice(150 * time.Millisecond)
	log := logger.New("error")
	sp := serial.New(serial.Config{
		Port:            "/dev/fake",
		ReadTimeout:     100 * time.Millisecond,
		ResponseTimeout: 300 * time.Millisecond,
		ReopenBackoff:   time.Millisecond,
		Opener:          func() (serial.Port, error) { return dev, nil },
	}, log)
	if err := sp.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = sp.Close() }()

	statusReg := config.ReadRegisterConfig{
		Name: "luefterstatus", Command: "130 1067", Topic: "aerosmart/luefterstatus",
		Divisor: 1, Type: "integer", MinValue: 0, MaxValue: 5,
	}
	modeReg := config.ReadRegisterConfig{
		Name: "lueftermode", Command: "130 5003", Topic: "aerosmart/lueftermode",
		Divisor: 1, Type: "integer", MinValue: 0, MaxValue: 5,
	}
	writeReg := config.WriteRegisterConfig{
		Name: "luefterstufe", CommandTemplate: "130 5002 {value}",
		SubscribeTopic: "dw/aerosmart/luefterstufe", Topic: "aerosmart/luefterstufe",
		MinValue: 0, MaxValue: 5,
		StateFrom:       "lueftermode",
		VerifyRegisters: []string{"luefterstatus", "lueftermode"},
	}

	pub := newRecordingPublisher()
	reader := registers.NewReader(sp, pub, log, []config.ReadRegisterConfig{statusReg, modeReg}, 3, 10*time.Millisecond)
	writer := registers.NewWriter(sp, pub, log, []config.WriteRegisterConfig{writeReg},
		registers.NewReadGate(time.Second), 3, 10*time.Millisecond, time.Millisecond)
	writer.SetReader(reader)

	runResettle := writer.SetResettleDelay(250 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resettleDone := make(chan struct{})
	go func() { runResettle(ctx); close(resettleDone) }()

	if err := writer.HandleMessage(ctx, "dw/aerosmart/luefterstufe", "4"); err != nil {
		t.Fatalf("HandleMessage() error = %v", err)
	}

	// Immediately after the write the actual stage still reads the old value.
	if v, _ := pub.get("aerosmart/luefterstatus"); v != "1" {
		t.Errorf("luefterstatus right after the write = %q, want the stale 1", v)
	}

	// The settle re-read must publish the value the unit ended up at.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if v, _ := pub.get("aerosmart/luefterstatus"); v == "4" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if v, _ := pub.get("aerosmart/luefterstatus"); v != "4" {
		t.Errorf("luefterstatus after settling = %q, want 4; the lagging value was never corrected", v)
	}

	cancel()
	<-resettleDone
}
