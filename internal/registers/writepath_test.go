package registers

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nean/aerosmart-gateway/internal/config"
)

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
