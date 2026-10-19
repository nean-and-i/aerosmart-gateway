package registers

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nean/aerosmart-gateway/internal/config"
	"github.com/nean/aerosmart-gateway/internal/logger"
)

// captureStdout runs fn with os.Stdout redirected, and returns what it wrote.
// Only safe because this package's tests never run in parallel.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error = %v", err)
	}
	orig := os.Stdout
	os.Stdout = w

	fn()

	os.Stdout = orig
	if err := w.Close(); err != nil {
		t.Fatalf("closing the pipe writer: %v", err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("reading captured output: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("closing the pipe reader: %v", err)
	}
	return buf.String()
}

// seqRecorder captures which write sequence each refresh was asked for.
type seqRecorder struct {
	mu   sync.Mutex
	seqs []uint64
}

func (r *seqRecorder) refresh(_ context.Context, _ config.WriteRegisterConfig, seq uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seqs = append(r.seqs, seq)
}

func (r *seqRecorder) snapshot() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.seqs...)
}

// A send that fails still bumps the write sequence, which cancels the settle
// re-read the previous command scheduled. Without a replacement, a value that
// lags that earlier command - the stage the unit picks for itself under auto
// or party - is never published until the next full read cycle.
func TestFailedSendStillSchedulesASettleReRead(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "3", "130 5003": "3"}}
	pub := newFakePublisher()
	w, reg := stateWriter(t, dev, pub, NewReadGate(time.Second))

	rec := &seqRecorder{}
	w.resettle = newResettler(10*time.Millisecond, rec.refresh, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { w.resettle.run(ctx); close(done) }()

	// A first command lands and schedules its re-read.
	if err := w.HandleMessage(context.Background(), reg.SubscribeTopic, "3"); err != nil {
		t.Fatalf("HandleMessage(3) error = %v", err)
	}

	// The next command cannot reach the wire, so it supersedes the first.
	dev.mu.Lock()
	dev.sendErr = errors.New("write failed")
	dev.mu.Unlock()

	if err := w.HandleMessage(context.Background(), reg.SubscribeTopic, "5"); err == nil {
		t.Fatal("HandleMessage(5) error = nil, want the send failure reported")
	}

	current := w.WriteSequence(reg.Name)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, seq := range rec.snapshot() {
			if seq == current {
				cancel()
				<-done
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}

	cancel()
	<-done
	t.Fatalf("no settle re-read ran for the current write sequence %d (saw %v); "+
		"the failed send cancelled the pending one and scheduled no replacement",
		current, rec.snapshot())
}

// The settle re-read puts transactions on the wire and publishes fresher
// values. A poll cycle running alongside it samples older values and publishes
// them afterwards, leaving Home Assistant wrong for a whole read interval.
func TestSettleReReadHoldsOffReadCycles(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "1", "130 5003": "4"}}
	pub := newFakePublisher()
	gate := NewReadGate(time.Second)
	w, reg := stateWriter(t, dev, pub, gate)

	suppressedDuringRead := false
	dev.afterCall = func(int) {
		if gate.Suppressed() {
			suppressedDuringRead = true
		}
	}

	w.refreshIfCurrent(context.Background(), reg, w.WriteSequence(reg.Name))

	if !suppressedDuringRead {
		t.Error("read cycles were not held off during the settle re-read; a concurrent cycle would overwrite it")
	}
	if gate.Suppressed() {
		t.Error("the gate was left suppressed after the settle re-read")
	}
}

// A settle re-read must cancel an in-flight cycle, not merely block the next
// one: that cycle's snapshot is already older than what the re-read publishes.
func TestSettleReReadPreemptsAnInFlightCycle(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "1", "130 5003": "4"}}
	pub := newFakePublisher()
	gate := NewReadGate(time.Second)
	w, reg := stateWriter(t, dev, pub, gate)

	cycleCtx, cancel, ok := gate.TryBeginCycle(context.Background())
	if !ok {
		t.Fatal("TryBeginCycle() refused to start")
	}
	defer cancel()

	w.refreshIfCurrent(context.Background(), reg, w.WriteSequence(reg.Name))

	if cycleCtx.Err() == nil {
		t.Error("the in-flight read cycle was not cancelled by the settle re-read")
	}
}

// The preempt check sits before a publish phase of ~35 MQTT publishes, each
// bounded only by op_timeout. A write arriving once publishing has started
// still has its fresher values overwritten by the rest of the stale snapshot.
func TestPollerStopsPublishingWhenPreemptedMidPublish(t *testing.T) {
	readRegs := []config.ReadRegisterConfig{fanReg, tempReg}
	dev := &fakeDevice{responses: map[string]string{
		fanReg.Command:  "1",
		tempReg.Command: "21000",
	}}
	pub := newFakePublisher()
	gate := NewReadGate(time.Second)

	// Preempt as the first publish of the cycle goes out.
	var once sync.Once
	pub.beforePublish = func(string) {
		once.Do(func() { gate.Preempt() })
	}

	p := NewPoller(NewReader(dev, pub, testLogger(), readRegs, 1, time.Millisecond),
		nil, gate, testLogger(), nil)

	if err := p.RunCycle(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("RunCycle() error = %v", err)
	}

	if n := pub.count(); n > 1 {
		t.Errorf("published %d value(s) after being preempted mid-publish, want at most 1; "+
			"the rest of the stale snapshot overwrites the write's fresher values", n)
	}
}

// A poller cancelled mid-publish must not go on to publish write-register
// state or derived values computed from the stale snapshot.
func TestPollerSkipsDerivedAndStateWhenPreemptedMidPublish(t *testing.T) {
	readRegs := []config.ReadRegisterConfig{fanReg, modeReadReg}
	dev := &fakeDevice{responses: map[string]string{
		fanReg.Command:      "1",
		modeReadReg.Command: "4",
	}}
	pub := newFakePublisher()
	gate := NewReadGate(time.Second)

	var once sync.Once
	pub.beforePublish = func(string) {
		once.Do(func() { gate.Preempt() })
	}

	writeReg := fanWrite
	writeReg.StateFrom = "lueftermode"

	p := NewPoller(NewReader(dev, pub, testLogger(), readRegs, 1, time.Millisecond),
		nil, gate, testLogger(), []config.WriteRegisterConfig{writeReg})

	if err := p.RunCycle(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("RunCycle() error = %v", err)
	}

	if _, ok := pub.get(writeReg.Topic); ok {
		t.Error("write-register state was republished from a cancelled cycle's stale snapshot")
	}
}

// The settle re-read runs on its own goroutine, so it holds the gate
// alongside the command worker. A release by one holder must not lift the
// other's hold, or a read cycle slips in while a write is still in flight.
func TestReadGateSuppressionSurvivesOverlappingHolders(t *testing.T) {
	gate := NewReadGate(time.Second)

	gate.Preempt() // the command worker
	gate.Preempt() // the settle re-read
	gate.Release() // the settle re-read finishes first

	if !gate.Suppressed() {
		t.Error("suppression lifted while another holder still has the gate")
	}

	gate.Release() // the command worker finishes
	if gate.Suppressed() {
		t.Error("suppression outlived its last holder")
	}
}

// A release without a matching hold must not leave the count negative, or the
// next genuine hold is cancelled by a single release.
func TestReadGateIgnoresUnmatchedRelease(t *testing.T) {
	gate := NewReadGate(time.Second)

	gate.Release()
	gate.Preempt()

	if !gate.Suppressed() {
		t.Error("an unmatched release left the gate unable to suppress")
	}
}

// A holder whose deadline expired must not be able to steal a later, still
// live holder's suppression when it finally calls Release. Sequence: the
// command worker Preempts and overruns write_priority_timeout; the deadline
// sweep lifts suppression so polling can resume; the settle re-read then
// Preempts for its own re-read; the overrun write finally returns and runs
// its deferred Release. That Release must not lift the settle re-read's hold.
func TestReadGateExpiredHolderReleaseDoesNotStealALaterHold(t *testing.T) {
	gate := NewReadGate(30 * time.Millisecond)

	gate.Preempt() // the command worker takes the gate

	// The write overruns write_priority_timeout.
	time.Sleep(50 * time.Millisecond)

	// The poll loop observes the gate; the deadline sweep lifts suppression.
	if gate.Suppressed() {
		t.Fatal("expected suppression to have expired")
	}

	// The settle re-read now takes the gate.
	gate.Preempt()

	// The overrunning write finally finishes and runs its deferred Release.
	gate.Release()

	if !gate.Suppressed() {
		t.Error("an expired holder's Release lifted a later holder's suppression")
	}
	if _, _, ok := gate.TryBeginCycle(context.Background()); ok {
		t.Error("a read cycle started while the settle re-read still held the gate")
	}
}

// The confirmation comparison must compare like with like: the raw MQTT
// payload against a state_from value that has gone through the read
// pipeline's divisor and type formatting. A state_from register with type
// "float" formats even a whole-number reading with a decimal ("22.0"), which
// never equals an unformatted payload ("22"), so a write that fully succeeded
// would otherwise be reported as refused.
func TestHandleMessageConfirmsAgainstAFloatStateFromRegister(t *testing.T) {
	writeReg := config.WriteRegisterConfig{
		Name:            "raumsolltempset",
		CommandTemplate: "130 5017 {value}",
		SubscribeTopic:  "dw/aerosmart/raumsolltempset",
		Topic:           "aerosmart/raumsolltempset_state",
		MinValue:        15,
		MaxValue:        30,
		StateFrom:       "raumsolltemp",
	}
	floatReadReg := config.ReadRegisterConfig{
		Name: "raumsolltemp", Command: "130 5017", Topic: "aerosmart/raumsolltemp",
		Divisor: 1, Type: "float", MinValue: 15, MaxValue: 40,
	}

	// The unit reports back exactly the commanded value, unscaled.
	dev := &fakeDevice{responses: map[string]string{"130 5017": "22"}}
	pub := newFakePublisher()
	log := testLogger()
	w := NewWriter(dev, pub, log, []config.WriteRegisterConfig{writeReg}, NewReadGate(time.Second), 3, time.Millisecond, time.Millisecond)
	w.SetReader(NewReader(dev, pub, log, []config.ReadRegisterConfig{floatReadReg}, 1, time.Millisecond))

	if err := w.HandleMessage(context.Background(), writeReg.SubscribeTopic, "22"); err != nil {
		t.Fatalf("HandleMessage() error = %v, want a successful write confirmed by its float state_from", err)
	}
	if v, ok := pub.get(writeReg.Topic); !ok || v != "22.0" {
		t.Errorf("state topic = %q (%v), want the formatted 22.0", v, ok)
	}
}

// A read cancelled by a preempting write is routine, not a device failure.
// SerialOperation logs a failed exchange at WARN, which is the documented
// primary flaky-link diagnostic; every write would otherwise pollute it with
// a false alarm, drowning out genuine device problems.
func TestPreemptedReadDoesNotLogAsSerialFailure(t *testing.T) {
	dev := &fakeDevice{block: make(chan struct{})}

	var r *Reader
	out := captureStdout(t, func() {
		// The logger captures os.Stdout at construction time, so it must be
		// built after the redirect, or its output bypasses the capture.
		log := logger.NewWithConfig(logger.Config{Level: "warn", ConsoleLogging: true})
		r = NewReader(dev, newFakePublisher(), log, []config.ReadRegisterConfig{fanReg}, 3, time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()
		_, _ = r.ReadSingle(ctx, fanReg)
	})

	if strings.Contains(out, "SERIAL") && strings.Contains(out, "FAILED") {
		t.Errorf("a cancelled read was logged as a serial failure: %q", out)
	}
}

// ---------- shared bus ----------

// countCalls reports how many times command was put on the wire.
func countCalls(dev *fakeDevice, command string) int {
	dev.mu.Lock()
	defer dev.mu.Unlock()
	n := 0
	for _, c := range dev.calls {
		if c == command {
			n++
		}
	}
	return n
}

// pendingSeq reports the write sequence a settle re-read is scheduled for.
func pendingSeq(w *Writer, name string) (uint64, bool) {
	w.resettle.mu.Lock()
	defer w.resettle.mu.Unlock()
	entry, ok := w.resettle.pending[name]
	return entry.seq, ok
}

// A collision on the shared bus can corrupt the command on its way to the
// unit. Writes set an absolute value, so sending it again is harmless, and a
// read-back that disagrees is the only sign the first one was lost.
func TestHandleMessageResendsWhenTheReadBackDisagrees(t *testing.T) {
	dev := &fakeDevice{
		responses: map[string]string{"130 1067": "2"},
		sequences: map[string][]string{"130 5003": {"4", "4", "2", "2"}},
	}
	pub := newFakePublisher()
	w, reg := stateWriter(t, dev, pub, NewReadGate(time.Second))

	if err := w.HandleMessage(context.Background(), reg.SubscribeTopic, "2"); err != nil {
		t.Fatalf("HandleMessage() error = %v, want success after the re-send", err)
	}
	if n := countCalls(dev, "130 5002 2"); n != 2 {
		t.Errorf("command sent %d times, want 2 (the original and one re-send)", n)
	}
	if v, _ := pub.get(reg.Topic); v != "2" {
		t.Errorf("state topic = %q, want 2", v)
	}
}

func TestHandleMessageResendsAtMostOnce(t *testing.T) {
	// Asked for 2, but the unit stays on 4: it may simply refuse the value.
	dev := &fakeDevice{responses: map[string]string{"130 1067": "4", "130 5003": "4"}}
	pub := newFakePublisher()
	w, reg := stateWriter(t, dev, pub, NewReadGate(time.Second))

	if err := w.HandleMessage(context.Background(), reg.SubscribeTopic, "2"); err == nil {
		t.Fatal("HandleMessage() error = nil, want the refusal reported")
	}
	if n := countCalls(dev, "130 5002 2"); n != 2 {
		t.Errorf("command sent %d times, want 2", n)
	}
	if v, _ := pub.get(reg.Topic); v != "4" {
		t.Errorf("state topic = %q, want the actual value 4", v)
	}
}

func TestHandleMessageResendsWhenNothingCanBeReadBack(t *testing.T) {
	dev := &fakeDevice{} // answers no reads
	w, reg := stateWriter(t, dev, newFakePublisher(), NewReadGate(time.Second))

	if err := w.HandleMessage(context.Background(), reg.SubscribeTopic, "3"); err == nil {
		t.Fatal("HandleMessage() error = nil, want an unconfirmed-write error")
	}
	if n := countCalls(dev, "130 5002 3"); n != 2 {
		t.Errorf("command sent %d times, want 2", n)
	}
}

// With state_from configured, the state topic carries what the unit holds. If
// that could not be read, publishing the commanded value would show Home
// Assistant a state nobody verified.
func TestHandleMessageDoesNotPublishAnUnverifiedState(t *testing.T) {
	dev := &fakeDevice{}
	pub := newFakePublisher()
	w, reg := stateWriter(t, dev, pub, NewReadGate(time.Second))

	_ = w.HandleMessage(context.Background(), reg.SubscribeTopic, "3")

	if v, ok := pub.get(reg.Topic); ok {
		t.Errorf("state topic = %q, want nothing published for an unverified write", v)
	}
}

// A refused or unconfirmed write still needs the settle re-read: the command
// bumped the write sequence, which cancelled any earlier one, and the value
// may yet change.
func TestHandleMessageSchedulesASettleReReadWhenTheWriteIsRefused(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "4", "130 5003": "4"}}
	w, reg := stateWriter(t, dev, newFakePublisher(), NewReadGate(time.Second))
	w.resettle = newResettler(time.Hour, (&seqRecorder{}).refresh, testLogger())

	_ = w.HandleMessage(context.Background(), reg.SubscribeTopic, "2")

	if seq, ok := pendingSeq(w, reg.Name); !ok || seq != w.WriteSequence(reg.Name) {
		t.Errorf("pending re-read = (%d, %v), want one for sequence %d", seq, ok, w.WriteSequence(reg.Name))
	}
}

func TestHandleMessageSchedulesASettleReReadWhenTheWriteIsUnconfirmed(t *testing.T) {
	dev := &fakeDevice{}
	w, reg := stateWriter(t, dev, newFakePublisher(), NewReadGate(time.Second))
	w.resettle = newResettler(time.Hour, (&seqRecorder{}).refresh, testLogger())

	_ = w.HandleMessage(context.Background(), reg.SubscribeTopic, "3")

	if seq, ok := pendingSeq(w, reg.Name); !ok || seq != w.WriteSequence(reg.Name) {
		t.Errorf("pending re-read = (%d, %v), want one for sequence %d", seq, ok, w.WriteSequence(reg.Name))
	}
}

// The command is built from the parsed number, not the raw payload: "+3" and
// "03" parse as 3 but would otherwise reach the unit as written.
func TestHandleMessageSendsTheParsedValue(t *testing.T) {
	for _, payload := range []string{"+3", "03", " 3 "} {
		t.Run(payload, func(t *testing.T) {
			dev := &fakeDevice{responses: map[string]string{"130 1067": "3", "130 5003": "3"}}
			w, reg := stateWriter(t, dev, newFakePublisher(), NewReadGate(time.Second))

			if err := w.HandleMessage(context.Background(), reg.SubscribeTopic, payload); err != nil {
				t.Fatalf("HandleMessage(%q) error = %v", payload, err)
			}
			if n := countCalls(dev, "130 5002 3"); n != 1 {
				dev.mu.Lock()
				t.Errorf("calls = %q, want exactly one %q", dev.calls, "130 5002 3")
				dev.mu.Unlock()
			}
		})
	}
}

// "3" and "03" are the same command, so a redelivery spelled differently is
// still a redelivery.
func TestDedupeComparesParsedValues(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "3", "130 5003": "3"}}
	w, reg := stateWriter(t, dev, newFakePublisher(), NewReadGate(time.Second))

	if err := w.HandleMessage(context.Background(), reg.SubscribeTopic, "3"); err != nil {
		t.Fatalf("HandleMessage(3) error = %v", err)
	}
	if err := w.HandleMessage(context.Background(), reg.SubscribeTopic, "03"); err != nil {
		t.Fatalf("HandleMessage(03) error = %v", err)
	}
	if n := countCalls(dev, "130 5002 3") + countCalls(dev, "130 5002 03"); n != 1 {
		t.Errorf("command sent %d times, want 1 (the second is a redelivery)", n)
	}
}
