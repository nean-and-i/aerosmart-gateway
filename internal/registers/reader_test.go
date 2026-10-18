package registers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nean/aerosmart-gateway/internal/config"
	"github.com/nean/aerosmart-gateway/internal/logger"
	"github.com/nean/aerosmart-gateway/internal/serial"
)

// fakeDevice is a Transactor whose answers are scripted per command.
type fakeDevice struct {
	mu        sync.Mutex
	responses map[string]string
	err       error
	failFirst int // fail this many calls before answering
	unhealthy bool
	sendErr   error // if set, Send fails with this
	afterCall func(n int)
	calls     []string
	block     chan struct{} // if set, Transact waits on it (or on ctx)

	// sequences scripts successive answers per command, taking precedence
	// over responses. The last answer repeats once the rest are used up.
	sequences map[string][]string
}

func (f *fakeDevice) Transact(ctx context.Context, command string) (string, error) {
	f.mu.Lock()
	f.calls = append(f.calls, command)
	n := len(f.calls)
	block := f.block
	f.mu.Unlock()

	if block != nil {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-block:
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if n <= f.failFirst {
		return "", serial.ErrReadTimeout
	}
	if f.err != nil {
		return "", f.err
	}
	f.mu.Lock()
	seq, scripted := f.sequences[command]
	if scripted && len(seq) > 1 {
		f.sequences[command] = seq[1:]
	}
	f.mu.Unlock()
	if scripted && len(seq) > 0 {
		return seq[0], nil
	}
	if v, ok := f.responses[command]; ok {
		// Fires as this read succeeds, so a cancellation triggered here lands
		// after the read rather than interrupting it.
		if f.afterCall != nil {
			f.afterCall(n)
		}
		return v, nil
	}
	return "", serial.ErrReadTimeout
}

func (f *fakeDevice) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// Send records the command without producing a response, which is how the
// real unit behaves for register writes.
func (f *fakeDevice) Send(ctx context.Context, command string) error {
	f.mu.Lock()
	f.calls = append(f.calls, command)
	err := f.sendErr
	f.mu.Unlock()

	if err != nil {
		return err
	}
	return ctx.Err()
}
func (f *fakeDevice) IsHealthy() bool { return !f.unhealthy }

// fakePublisher records what was published.
type fakePublisher struct {
	mu        sync.Mutex
	published map[string]string
	order     []string
	err       error
	// beforePublish fires before each publish is recorded, so a test can
	// disturb the cycle partway through its publish phase.
	beforePublish func(topic string)
}

func newFakePublisher() *fakePublisher {
	return &fakePublisher{published: map[string]string{}}
}

func (p *fakePublisher) Publish(topic, value string) error {
	p.mu.Lock()
	hook := p.beforePublish
	p.mu.Unlock()
	if hook != nil {
		hook(topic)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.published[topic] = value
	p.order = append(p.order, topic+"="+value)
	return nil
}

// count reports how many publishes were recorded.
func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.order)
}

func (p *fakePublisher) get(topic string) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	v, ok := p.published[topic]
	return v, ok
}

// testLogger discards output: several tests exercise error paths on purpose.
func testLogger() *logger.Logger {
	return logger.NewWithConfig(logger.Config{Level: "error", ConsoleLogging: false})
}

var fanReg = config.ReadRegisterConfig{
	Name: "luefterstatus", Command: "130 1067", Topic: "aerosmart/luefterstatus",
	Divisor: 1, Type: "integer", MinValue: 0, MaxValue: 5,
}

var tempReg = config.ReadRegisterConfig{
	Name: "innentemp", Command: "130 201", Topic: "aerosmart/innentemp",
	Divisor: 1000, Type: "float", MinValue: -20, MaxValue: 50,
}

// ---------- ReadGate ----------

func TestReadGatePreemptCancelsCycle(t *testing.T) {
	g := NewReadGate(time.Second)
	ctx, cancel, ok := g.TryBeginCycle(context.Background())
	if !ok {
		t.Fatal("TryBeginCycle() refused on a fresh gate")
	}
	defer cancel()

	if ctx.Err() != nil {
		t.Fatalf("fresh cycle context already done: %v", ctx.Err())
	}

	g.Preempt()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("Preempt() did not cancel the in-flight cycle")
	}
}

func TestReadGateSuppressionClearedByRelease(t *testing.T) {
	g := NewReadGate(time.Second)

	if g.Suppressed() {
		t.Fatal("Suppressed() = true on a fresh gate, want false")
	}
	g.Preempt()
	if !g.Suppressed() {
		t.Fatal("Suppressed() = false after Preempt(), want true")
	}
	g.Release()
	if g.Suppressed() {
		t.Fatal("Suppressed() = true after Release(), want false")
	}
}

// Suppression must expire on its own, so a lost Release can never wedge reads.
func TestReadGateSuppressionExpires(t *testing.T) {
	g := NewReadGate(20 * time.Millisecond)
	g.Preempt()
	if !g.Suppressed() {
		t.Fatal("Suppressed() = false right after Preempt(), want true")
	}
	time.Sleep(40 * time.Millisecond)
	if g.Suppressed() {
		t.Fatal("Suppressed() = true after the timeout elapsed, want false")
	}
}

// ---------- ReadSingle ----------

func TestReadSingleAppliesDivisorAndType(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 201": "21500"}}
	r := NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{tempReg}, 3, time.Millisecond)

	got, err := r.ReadSingle(context.Background(), tempReg)
	if err != nil {
		t.Fatalf("ReadSingle() error = %v", err)
	}
	if got != "21.5" {
		t.Errorf("ReadSingle() = %q, want %q", got, "21.5")
	}
}

func TestReadSingleRetriesBoundedNumberOfTimes(t *testing.T) {
	// One failure, then a first reading and the read that confirms it.
	dev := &fakeDevice{responses: map[string]string{"130 1067": "3"}, failFirst: 1}
	r := NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{fanReg}, 3, time.Millisecond)

	got, err := r.ReadSingle(context.Background(), fanReg)
	if err != nil {
		t.Fatalf("ReadSingle() error = %v", err)
	}
	if got != "3" {
		t.Errorf("ReadSingle() = %q, want %q", got, "3")
	}
	if n := dev.callCount(); n != 3 {
		t.Errorf("device called %d times, want exactly 3 (retries must not multiply)", n)
	}
}

func TestReadSingleGivesUpAfterRetries(t *testing.T) {
	dev := &fakeDevice{failFirst: 100}
	r := NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{fanReg}, 3, time.Millisecond)

	if _, err := r.ReadSingle(context.Background(), fanReg); err == nil {
		t.Fatal("ReadSingle() error = nil, want failure")
	}
	if n := dev.callCount(); n != 3 {
		t.Errorf("device called %d times, want exactly 3", n)
	}
}

// Hammering a port that is gone wastes the whole cycle budget; the supervisor
// reopens it, so one attempt is enough.
func TestReadSingleStopsImmediatelyOnPortError(t *testing.T) {
	dev := &fakeDevice{err: serial.ErrPortNotOpen}
	r := NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{fanReg}, 5, time.Millisecond)

	if _, err := r.ReadSingle(context.Background(), fanReg); !errors.Is(err, serial.ErrPortNotOpen) {
		t.Fatalf("ReadSingle() error = %v, want ErrPortNotOpen", err)
	}
	if n := dev.callCount(); n != 1 {
		t.Errorf("device called %d times, want 1 (a dead port must not be retried)", n)
	}
}

func TestReadSingleHonoursContextCancellation(t *testing.T) {
	dev := &fakeDevice{failFirst: 100}
	r := NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{fanReg}, 100, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := r.ReadSingle(ctx, fanReg); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadSingle() error = %v, want context.Canceled", err)
	}
}

// ---------- ReadAll ----------

func TestReadAllMarksUnreadableRegistersInvalid(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "3"}} // tempReg unanswered
	r := NewReader(dev, newFakePublisher(), testLogger(),
		[]config.ReadRegisterConfig{fanReg, tempReg}, 1, time.Millisecond)

	values, err := r.ReadAll(context.Background())
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if v := values["luefterstatus"]; v == nil || !v.Valid || v.Value != "3" {
		t.Errorf("luefterstatus = %+v, want valid 3", v)
	}
	if v := values["innentemp"]; v == nil || v.Valid {
		t.Errorf("innentemp = %+v, want invalid", v)
	}
}

// A cancelled cycle must be reported, so the caller does not publish partial
// results or compute derived values from missing sources.
func TestReadAllReportsCancellation(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "3", "130 201": "21500"}}
	r := NewReader(dev, newFakePublisher(), testLogger(),
		[]config.ReadRegisterConfig{fanReg, tempReg}, 1, time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := r.ReadAll(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadAll() error = %v, want context.Canceled", err)
	}
}

// An out-of-range fan status used to be clamped to the maximum and published.
// On a shared bus most such readings are corrupted frames, and clamping turned
// garbage into a confident "highest stage".
func TestReadAllDoesNotClampAnOutOfRangeFanStatus(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "9"}}
	r := NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{fanReg}, 3, time.Millisecond)

	values, err := r.ReadAll(context.Background())
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if v := values["luefterstatus"]; v == nil || v.Valid {
		t.Errorf("luefterstatus = %+v, want invalid", v)
	}
}

// ---------- Confirmation ----------
//
// The bus is shared with the unit's panel and frames carry no checksum, so a
// corrupted reply can still parse. A reading is trusted only when it agrees
// with the last trusted value, or a second read agrees with it.

func TestReadSingleConfirmsAFirstReadingWithASecond(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "3"}}
	r := NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{fanReg}, 3, time.Millisecond)

	got, err := r.ReadSingle(context.Background(), fanReg)
	if err != nil || got != "3" {
		t.Fatalf("ReadSingle() = %q, %v; want 3, nil", got, err)
	}
	if n := dev.callCount(); n != 2 {
		t.Errorf("device called %d times, want 2 (a first reading needs confirming)", n)
	}
}

func TestReadSingleTrustsAnUnchangedValueAfterOneRead(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "3"}}
	r := NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{fanReg}, 3, time.Millisecond)
	if _, err := r.ReadSingle(context.Background(), fanReg); err != nil {
		t.Fatalf("first ReadSingle() error = %v", err)
	}
	before := dev.callCount()

	if got, err := r.ReadSingle(context.Background(), fanReg); err != nil || got != "3" {
		t.Fatalf("ReadSingle() = %q, %v; want 3, nil", got, err)
	}
	if n := dev.callCount() - before; n != 1 {
		t.Errorf("device called %d times, want 1 (an unchanged value needs no confirming)", n)
	}
}

func TestReadSingleDiscardsAOneOffCorruptedReading(t *testing.T) {
	dev := &fakeDevice{sequences: map[string][]string{"130 1067": {"3", "3", "1", "3"}}}
	r := NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{fanReg}, 3, time.Millisecond)
	if _, err := r.ReadSingle(context.Background(), fanReg); err != nil {
		t.Fatalf("first ReadSingle() error = %v", err)
	}

	got, err := r.ReadSingle(context.Background(), fanReg)
	if err != nil || got != "3" {
		t.Fatalf("ReadSingle() = %q, %v; want the trusted 3, nil", got, err)
	}
}

func TestReadSingleAcceptsAChangeOnceTwoReadsAgree(t *testing.T) {
	dev := &fakeDevice{sequences: map[string][]string{"130 1067": {"3", "3", "1", "1"}}}
	r := NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{fanReg}, 3, time.Millisecond)
	if _, err := r.ReadSingle(context.Background(), fanReg); err != nil {
		t.Fatalf("first ReadSingle() error = %v", err)
	}

	got, err := r.ReadSingle(context.Background(), fanReg)
	if err != nil || got != "1" {
		t.Fatalf("ReadSingle() = %q, %v; want the confirmed change to 1", got, err)
	}
}

func TestReadSingleBreaksATieWithAThirdRead(t *testing.T) {
	dev := &fakeDevice{sequences: map[string][]string{"130 1067": {"4", "2", "2"}}}
	r := NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{fanReg}, 3, time.Millisecond)

	got, err := r.ReadSingle(context.Background(), fanReg)
	if err != nil || got != "2" {
		t.Fatalf("ReadSingle() = %q, %v; want 2, the value two reads agreed on", got, err)
	}
}

func TestReadSingleReportsReadingsThatNeverAgree(t *testing.T) {
	dev := &fakeDevice{sequences: map[string][]string{"130 1067": {"1", "2", "3"}}}
	r := NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{fanReg}, 3, time.Millisecond)

	_, err := r.ReadSingle(context.Background(), fanReg)
	if !errors.Is(err, ErrUnconfirmed) {
		t.Fatalf("ReadSingle() error = %v, want ErrUnconfirmed", err)
	}
	if n := dev.callCount(); n != 3 {
		t.Errorf("device called %d times, want 3 (confirmation shares the retry budget)", n)
	}
}

func TestReadAllMarksAnUnconfirmedReadingInvalid(t *testing.T) {
	dev := &fakeDevice{sequences: map[string][]string{"130 1067": {"1", "2", "3"}}}
	r := NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{fanReg}, 3, time.Millisecond)

	values, err := r.ReadAll(context.Background())
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if v := values["luefterstatus"]; v == nil || v.Valid {
		t.Errorf("luefterstatus = %+v, want invalid", v)
	}
}

// An out-of-range reading is almost certainly a corrupted frame: it uses up an
// attempt rather than ending the read.
func TestReadSingleRetriesPastAnOutOfRangeReading(t *testing.T) {
	dev := &fakeDevice{sequences: map[string][]string{"130 1067": {"31", "3", "3"}}}
	r := NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{fanReg}, 3, time.Millisecond)

	got, err := r.ReadSingle(context.Background(), fanReg)
	if err != nil || got != "3" {
		t.Fatalf("ReadSingle() = %q, %v; want 3, nil", got, err)
	}
}

// confirm_step lets a register drift by a small amount without a second read:
// a temperature that moved by a tenth of a degree is not suspicious.
func TestReadSingleTrustsAChangeWithinConfirmStep(t *testing.T) {
	reg := tempReg
	reg.ConfirmStep = 0.5
	dev := &fakeDevice{sequences: map[string][]string{"130 201": {"21500", "21500", "21700"}}}
	r := NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{reg}, 3, time.Millisecond)
	if _, err := r.ReadSingle(context.Background(), reg); err != nil {
		t.Fatalf("first ReadSingle() error = %v", err)
	}
	before := dev.callCount()

	got, err := r.ReadSingle(context.Background(), reg)
	if err != nil || got != "21.7" {
		t.Fatalf("ReadSingle() = %q, %v; want 21.7, nil", got, err)
	}
	if n := dev.callCount() - before; n != 1 {
		t.Errorf("device called %d times, want 1 (the change is within confirm_step)", n)
	}
}

func TestReadSingleConfirmsAChangeBeyondConfirmStep(t *testing.T) {
	reg := tempReg
	reg.ConfirmStep = 0.5
	dev := &fakeDevice{sequences: map[string][]string{"130 201": {"21500", "21500", "2150", "21500"}}}
	r := NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{reg}, 3, time.Millisecond)
	if _, err := r.ReadSingle(context.Background(), reg); err != nil {
		t.Fatalf("first ReadSingle() error = %v", err)
	}

	got, err := r.ReadSingle(context.Background(), reg)
	if err != nil || got != "21.5" {
		t.Fatalf("ReadSingle() = %q, %v; want 21.5 (the 2.15 reading is a dropped digit)", got, err)
	}
}

// Confirming needs a second read, so a register gets two attempts even when
// retries is configured as 1. Otherwise no value could ever change.
func TestReadSingleConfirmsEvenWithASingleRetry(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "3"}}
	r := NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{fanReg}, 1, time.Millisecond)

	if got, err := r.ReadSingle(context.Background(), fanReg); err != nil || got != "3" {
		t.Fatalf("ReadSingle() = %q, %v; want 3, nil", got, err)
	}
}

// A fixed backoff can keep landing on the panel's own polling rhythm and
// collide every time; jitter breaks that lockstep.
func TestRetryBackoffIsJitteredWithinHalfAgain(t *testing.T) {
	base := 100 * time.Millisecond
	seen := map[time.Duration]bool{}
	for range 200 {
		d := jittered(base)
		if d < base || d > base*3/2 {
			t.Fatalf("jittered(%v) = %v, want within [%v, %v]", base, d, base, base*3/2)
		}
		seen[d] = true
	}
	if len(seen) < 10 {
		t.Errorf("jittered(%v) produced only %d distinct values in 200 draws", base, len(seen))
	}
}

func TestPublishAllSkipsInvalidValues(t *testing.T) {
	pub := newFakePublisher()
	r := NewReader(&fakeDevice{}, pub, testLogger(),
		[]config.ReadRegisterConfig{fanReg, tempReg}, 1, time.Millisecond)

	_ = r.PublishAll(context.Background(), map[string]*RegisterValue{
		"luefterstatus": {Name: "luefterstatus", Value: "3", Valid: true},
		"innentemp":     {Name: "innentemp", Value: "", Valid: false},
	})

	if v, ok := pub.get("aerosmart/luefterstatus"); !ok || v != "3" {
		t.Errorf("luefterstatus published = %q (%v), want 3", v, ok)
	}
	if _, ok := pub.get("aerosmart/innentemp"); ok {
		t.Error("an invalid value was published; stale readings must not be republished")
	}
}

// ---------- Writer ----------

var fanWrite = config.WriteRegisterConfig{
	Name:            "luefterstufe",
	CommandTemplate: "130 5002 {value}",
	SubscribeTopic:  "dw/aerosmart/luefterstufe",
	Topic:           "aerosmart/luefterstufe_state",
	MinValue:        0,
	MaxValue:        5,
	VerifyRegisters: []string{"luefterstatus"},
}

func testWriter(dev Transactor, pub Publisher, gate *ReadGate) *Writer {
	log := testLogger()
	w := NewWriter(dev, pub, log, []config.WriteRegisterConfig{fanWrite}, gate, 3, time.Millisecond, time.Millisecond)
	w.SetReader(NewReader(dev, pub, log, []config.ReadRegisterConfig{fanReg}, 1, time.Millisecond))
	return w
}

func TestHandleMessageWritesAndPublishesVerifiedValue(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 5002 3": "3", "130 1067": "3"}}
	pub := newFakePublisher()
	gate := NewReadGate(time.Second)
	w := testWriter(dev, pub, gate)

	if err := w.HandleMessage(context.Background(), fanWrite.SubscribeTopic, "3"); err != nil {
		t.Fatalf("HandleMessage() error = %v", err)
	}
	if v, ok := pub.get("aerosmart/luefterstatus"); !ok || v != "3" {
		t.Errorf("verified value published = %q (%v), want 3", v, ok)
	}
	// The write register's own state topic must reflect the accepted command.
	if v, ok := pub.get(fanWrite.Topic); !ok || v != "3" {
		t.Errorf("state topic = %q (%v), want 3", v, ok)
	}
	if gate.Suppressed() {
		t.Error("gate still suppressed after a successful write")
	}
}

// Every one of these paths used to return while write priority was still set,
// which cancelled the read cycle and blocked reads until the timeout expired.
func TestHandleMessageAlwaysReleasesGate(t *testing.T) {
	cases := []struct {
		name    string
		topic   string
		payload string
		dev     *fakeDevice
		wantErr bool
	}{
		{
			name:    "unknown topic",
			topic:   "dw/aerosmart/nonexistent",
			payload: "3",
			dev:     &fakeDevice{},
		},
		{
			name:    "non-numeric payload",
			topic:   fanWrite.SubscribeTopic,
			payload: "ON",
			dev:     &fakeDevice{},
			wantErr: true,
		},
		{
			name:    "value above range",
			topic:   fanWrite.SubscribeTopic,
			payload: "9",
			dev:     &fakeDevice{},
			wantErr: true,
		},
		{
			name:    "device write fails",
			topic:   fanWrite.SubscribeTopic,
			payload: "3",
			dev:     &fakeDevice{failFirst: 100},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gate := NewReadGate(time.Hour) // long, so only an explicit Release clears it
			w := testWriter(tc.dev, newFakePublisher(), gate)

			err := w.HandleMessage(context.Background(), tc.topic, tc.payload)
			if tc.wantErr && err == nil {
				t.Error("HandleMessage() error = nil, want an error")
			}
			if gate.Suppressed() {
				t.Error("gate left suppressed; reads would stay blocked until timeout")
			}
		})
	}
}

// A failed command must remain retryable: deduplication may only remember
// commands that actually reached the device.
func TestHandleMessageRetryableAfterFailure(t *testing.T) {
	dev := &fakeDevice{failFirst: 3} // fail the first attempt's retries, then succeed
	dev.responses = map[string]string{"130 5002 3": "3", "130 1067": "3"}
	gate := NewReadGate(time.Second)
	w := testWriter(dev, newFakePublisher(), gate)

	if err := w.HandleMessage(context.Background(), fanWrite.SubscribeTopic, "3"); err == nil {
		t.Fatal("first HandleMessage() error = nil, want failure")
	}
	if err := w.HandleMessage(context.Background(), fanWrite.SubscribeTopic, "3"); err != nil {
		t.Fatalf("retry of the same command error = %v, want nil (it must not be deduplicated)", err)
	}
}

func TestHandleMessageDeduplicatesSuccessfulRepeat(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 5002 3": "3", "130 1067": "3"}}
	gate := NewReadGate(time.Second)
	w := testWriter(dev, newFakePublisher(), gate)

	if err := w.HandleMessage(context.Background(), fanWrite.SubscribeTopic, "3"); err != nil {
		t.Fatalf("HandleMessage() error = %v", err)
	}
	before := dev.callCount()
	if err := w.HandleMessage(context.Background(), fanWrite.SubscribeTopic, "3"); err != nil {
		t.Fatalf("duplicate HandleMessage() error = %v", err)
	}
	if dev.callCount() != before {
		t.Errorf("device called again for a duplicate within the window (%d -> %d)", before, dev.callCount())
	}
}

func TestHandleMessagePreemptsReadCycle(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 5002 3": "3", "130 1067": "3"}}
	gate := NewReadGate(time.Second)
	w := testWriter(dev, newFakePublisher(), gate)

	cycleCtx, cancel, ok := gate.TryBeginCycle(context.Background())
	if !ok {
		t.Fatal("TryBeginCycle() refused on a fresh gate")
	}
	defer cancel()

	if err := w.HandleMessage(context.Background(), fanWrite.SubscribeTopic, "3"); err != nil {
		t.Fatalf("HandleMessage() error = %v", err)
	}
	if cycleCtx.Err() == nil {
		t.Error("in-flight read cycle was not preempted by an incoming write")
	}
}

func TestGetSubscribeTopics(t *testing.T) {
	w := testWriter(&fakeDevice{}, newFakePublisher(), NewReadGate(time.Second))
	topics := w.GetSubscribeTopics()
	if len(topics) != 1 || topics[0] != fanWrite.SubscribeTopic {
		t.Errorf("GetSubscribeTopics() = %v, want [%s]", topics, fanWrite.SubscribeTopic)
	}
}

var _ = fmt.Sprintf // keep fmt referenced if assertions change

// ---------- Poller ----------

func TestPollerSkipsCycleWhileWriteIsPending(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "3"}}
	pub := newFakePublisher()
	gate := NewReadGate(time.Hour)
	p := NewPoller(NewReader(dev, pub, testLogger(), []config.ReadRegisterConfig{fanReg}, 1, time.Millisecond),
		nil, gate, testLogger(), nil)

	gate.Preempt() // a write is in progress

	if err := p.RunCycle(context.Background()); !errors.Is(err, ErrCycleSkipped) {
		t.Fatalf("RunCycle() error = %v, want ErrCycleSkipped", err)
	}
	if dev.callCount() != 0 {
		t.Errorf("device called %d times during suppression, want 0", dev.callCount())
	}
}

func TestPollerPublishesReadAndDerivedValues(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "3", "130 201": "21500"}}
	pub := newFakePublisher()
	log := testLogger()
	derived := NewDerivedCalculator(pub, log, []config.DerivedRegisterConfig{{
		Name:    "co2luefterstufe4",
		Topic:   "aerosmart/co2luefterstufe4",
		Sources: []string{"co2luefterstufe3"},
	}})
	reader := NewReader(dev, pub, log, []config.ReadRegisterConfig{fanReg, tempReg}, 1, time.Millisecond)
	p := NewPoller(reader, derived, NewReadGate(time.Second), log, nil)

	if err := p.RunCycle(context.Background()); err != nil {
		t.Fatalf("RunCycle() error = %v", err)
	}
	if v, ok := pub.get("aerosmart/luefterstatus"); !ok || v != "3" {
		t.Errorf("luefterstatus published = %q (%v), want 3", v, ok)
	}
	if v, ok := pub.get("aerosmart/innentemp"); !ok || v != "21.5" {
		t.Errorf("innentemp published = %q (%v), want 21.5", v, ok)
	}
}

// A cancelled cycle must not publish partial results, nor compute derived
// values from sources that were never read.
func TestPollerDoesNotPublishCancelledCycle(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "3"}}
	pub := newFakePublisher()
	p := NewPoller(NewReader(dev, pub, testLogger(), []config.ReadRegisterConfig{fanReg}, 1, time.Millisecond),
		nil, NewReadGate(time.Second), testLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := p.RunCycle(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunCycle() error = %v, want context.Canceled", err)
	}
	if _, ok := pub.get("aerosmart/luefterstatus"); ok {
		t.Error("a cancelled cycle published values; partial results must be withheld")
	}
}

// ---------- DerivedCalculator ----------

// The formula field in registers.yaml is documentation: the arithmetic lives in
// derived.go, keyed by register name. A register with no implementation must be
// rejected at startup rather than warning on every cycle forever.
func TestDerivedValidateRejectsUnimplementedRegister(t *testing.T) {
	d := NewDerivedCalculator(newFakePublisher(), testLogger(), []config.DerivedRegisterConfig{
		{Name: "brand_new_thing", Topic: "aerosmart/x", Formula: "a + b", Sources: []string{"a", "b"}},
	})

	err := d.Validate()
	if err == nil {
		t.Fatal("Validate() = nil, want an error naming the unimplemented register")
	}
	if !strings.Contains(err.Error(), "brand_new_thing") {
		t.Errorf("Validate() error = %v, want it to name brand_new_thing", err)
	}
}

func TestDerivedValidateAcceptsImplementedRegisters(t *testing.T) {
	d := NewDerivedCalculator(newFakePublisher(), testLogger(), []config.DerivedRegisterConfig{
		{Name: "zuluftabluftprozent", Sources: []string{"zuluftumin", "abluftumin"}},
		{Name: "co2luefterstufe4", Sources: []string{"co2luefterstufe3"}},
		{Name: "beschattungtemp_adjusted", Sources: []string{"beschattungtemp", "beschattungaussentemp", "aussentemp"}},
	})

	if err := d.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

// A register whose sources are missing must be reported invalid, not published.
func TestDerivedSkipsRegistersWithMissingSources(t *testing.T) {
	pub := newFakePublisher()
	d := NewDerivedCalculator(pub, testLogger(), []config.DerivedRegisterConfig{
		{Name: "co2luefterstufe4", Topic: "aerosmart/co2luefterstufe4", Sources: []string{"co2luefterstufe3"}},
	})

	values := d.Calculate(map[string]*RegisterValue{}) // source absent
	if v := values["co2luefterstufe4"]; v == nil || v.Valid {
		t.Errorf("co2luefterstufe4 = %+v, want invalid", v)
	}
	if err := d.PublishAll(context.Background(), values); err != nil {
		t.Fatalf("PublishAll() error = %v", err)
	}
	if _, ok := pub.get("aerosmart/co2luefterstufe4"); ok {
		t.Error("an invalid derived value was published")
	}
}

func TestDerivedDivisionByZeroGuard(t *testing.T) {
	d := NewDerivedCalculator(newFakePublisher(), testLogger(), []config.DerivedRegisterConfig{
		{Name: "zuluftabluftprozent", Topic: "aerosmart/z", Sources: []string{"zuluftumin", "abluftumin"}},
	})

	values := d.Calculate(map[string]*RegisterValue{
		"zuluftumin": {Name: "zuluftumin", Value: "120", Valid: true},
		"abluftumin": {Name: "abluftumin", Value: "0", Valid: true},
	})
	if v := values["zuluftabluftprozent"]; v == nil || !v.Valid || v.Value != "100.0" {
		t.Errorf("zuluftabluftprozent = %+v, want valid 100.0", v)
	}
}

// ---------- review follow-ups ----------

// Checking suppression and starting a cycle must be one atomic step, or a
// write that arrives in between is preempting a cycle that has not begun.
func TestReadGateTryBeginCycleRefusesWhileSuppressed(t *testing.T) {
	g := NewReadGate(time.Hour)
	g.Preempt()

	if _, _, ok := g.TryBeginCycle(context.Background()); ok {
		t.Fatal("TryBeginCycle() succeeded while a write was pending, want refusal")
	}

	g.Release()
	ctx, cancel, ok := g.TryBeginCycle(context.Background())
	if !ok {
		t.Fatal("TryBeginCycle() refused after Release(), want success")
	}
	defer cancel()
	if ctx.Err() != nil {
		t.Errorf("cycle context already done: %v", ctx.Err())
	}
}

// Once a cycle has finished, its cancel must not be retained: preempting then
// would cancel a completed cycle and leave the next one unprotected.
func TestReadGateForgetsFinishedCycle(t *testing.T) {
	g := NewReadGate(time.Hour)

	_, cancel, ok := g.TryBeginCycle(context.Background())
	if !ok {
		t.Fatal("TryBeginCycle() refused on a fresh gate")
	}
	cancel() // cycle finished

	if g.hasActiveCycle() {
		t.Error("gate still holds a cancel for a finished cycle")
	}
}

// Grinding 30+ registers through their retries against a port the supervisor
// is already reopening wastes the whole interval.
func TestPollerSkipsCycleWhilePortUnhealthy(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "3"}, unhealthy: true}
	pub := newFakePublisher()
	p := NewPoller(NewReader(dev, pub, testLogger(), []config.ReadRegisterConfig{fanReg}, 3, time.Millisecond),
		nil, NewReadGate(time.Second), testLogger(), nil)

	if err := p.RunCycle(context.Background()); !errors.Is(err, ErrCycleSkipped) {
		t.Fatalf("RunCycle() error = %v, want ErrCycleSkipped", err)
	}
	if dev.callCount() != 0 {
		t.Errorf("device called %d times while the port was unhealthy, want 0", dev.callCount())
	}
}

// Deduplication must only suppress a genuine redelivery of the last command,
// not a real change and change-back inside the window.
func TestHandleMessageAllowsValueChangeAndBack(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{
		"130 5002 3": "3", "130 5002 0": "0", "130 1067": "3",
	}}
	pub := newFakePublisher()
	w := testWriter(dev, pub, NewReadGate(time.Second))

	for _, v := range []string{"3", "0", "3"} {
		if err := w.HandleMessage(context.Background(), fanWrite.SubscribeTopic, v); err != nil {
			t.Fatalf("HandleMessage(%s) error = %v", v, err)
		}
	}

	// The device must have ended on 3, not on the intermediate 0.
	if got, _ := pub.get(fanWrite.Topic); got != "3" {
		t.Errorf("state topic = %q, want 3; the final command was dropped as a duplicate", got)
	}
	writes := 0
	dev.mu.Lock()
	for _, c := range dev.calls {
		if strings.HasPrefix(c, "130 5002") {
			writes++
		}
	}
	dev.mu.Unlock()
	if writes != 3 {
		t.Errorf("write commands sent = %d, want 3 (3, 0, 3)", writes)
	}
}

// RawValue must carry what the device actually reported, so a suspicious
// reading can be traced back; it was being filled with the processed value.
func TestReadAllRecordsRawDeviceValue(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 201": "21500"}}
	r := NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{tempReg}, 1, time.Millisecond)

	values, err := r.ReadAll(context.Background())
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	v := values["innentemp"]
	if v == nil {
		t.Fatal("innentemp missing")
	}
	if v.Value != "21.5" {
		t.Errorf("Value = %q, want 21.5", v.Value)
	}
	if v.RawValue != "21500" {
		t.Errorf("RawValue = %q, want the device value 21500", v.RawValue)
	}
}

// A skipped cycle must be distinguishable, so the caller can retry sooner than
// a full interval instead of losing minutes to a brief port blip.
func TestPollerReportsSkippedCycle(t *testing.T) {
	dev := &fakeDevice{unhealthy: true}
	p := NewPoller(NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{fanReg}, 1, time.Millisecond),
		nil, NewReadGate(time.Second), testLogger(), nil)

	err := p.RunCycle(context.Background())
	if !errors.Is(err, ErrCycleSkipped) {
		t.Fatalf("RunCycle() error = %v, want ErrCycleSkipped", err)
	}
}

func TestPollerReportsSkippedCycleWhenSuppressed(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "3"}}
	gate := NewReadGate(time.Hour)
	p := NewPoller(NewReader(dev, newFakePublisher(), testLogger(), []config.ReadRegisterConfig{fanReg}, 1, time.Millisecond),
		nil, gate, testLogger(), nil)
	gate.Preempt()

	if err := p.RunCycle(context.Background()); !errors.Is(err, ErrCycleSkipped) {
		t.Fatalf("RunCycle() error = %v, want ErrCycleSkipped", err)
	}
}

// A formula reads its inputs from the sources map, so an input the config
// forgot to list silently reads 0 and publishes a wrong value as valid.
func TestDerivedValidateRequiresEveryFormulaInput(t *testing.T) {
	d := NewDerivedCalculator(newFakePublisher(), testLogger(), []config.DerivedRegisterConfig{{
		Name:  "beschattungtemp_adjusted",
		Topic: "aerosmart/x",
		// "aussentemp" omitted: the comparison would become "x < 0" and always
		// take the same branch.
		Sources: []string{"beschattungtemp", "beschattungaussentemp"},
	}})

	err := d.Validate()
	if err == nil {
		t.Fatal("Validate() = nil, want an error about the missing source")
	}
	if !strings.Contains(err.Error(), "aussentemp") {
		t.Errorf("Validate() error = %v, want it to name the missing source aussentemp", err)
	}
}

func TestDerivedValidateAcceptsCompleteSources(t *testing.T) {
	d := NewDerivedCalculator(newFakePublisher(), testLogger(), []config.DerivedRegisterConfig{
		{Name: "zuluftabluftprozent", Topic: "t", Sources: []string{"zuluftumin", "abluftumin"}},
		{Name: "co2luefterstufe4", Topic: "t", Sources: []string{"co2luefterstufe3"}},
		{Name: "beschattungtemp_adjusted", Topic: "t",
			Sources: []string{"beschattungtemp", "beschattungaussentemp", "aussentemp"}},
	})
	if err := d.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

// ---------- write confirmation (real-device behaviour) ----------

// The unit applies a write but does not reliably echo the command: on a real
// gateway 4 of 6 writes produced no matching echo within 3 x 300ms, while the
// bus showed the resulting status change, proving the write had landed.
// Success must therefore be decided by reading the register back, not by an
// echo, and the command must not be re-sent while waiting for one.
func TestHandleMessageSucceedsWithoutAnEcho(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{
		// No entry for "130 5002 3": the command is accepted, never echoed.
		"130 1067": "3",
	}}
	pub := newFakePublisher()
	gate := NewReadGate(time.Second)
	w := testWriter(dev, pub, gate)

	if err := w.HandleMessage(context.Background(), fanWrite.SubscribeTopic, "3"); err != nil {
		t.Fatalf("HandleMessage() error = %v, want success once the register reads back", err)
	}

	if v, ok := pub.get("aerosmart/luefterstatus"); !ok || v != "3" {
		t.Errorf("verified value published = %q (%v), want 3", v, ok)
	}
	if v, ok := pub.get(fanWrite.Topic); !ok || v != "3" {
		t.Errorf("state topic = %q (%v), want 3", v, ok)
	}
	if gate.Suppressed() {
		t.Error("gate left suppressed")
	}
}

// The command must be written once. Re-sending it while waiting for an echo
// re-applies a setting that already took effect.
func TestHandleMessageSendsCommandOnce(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "3"}}
	w := testWriter(dev, newFakePublisher(), NewReadGate(time.Second))

	if err := w.HandleMessage(context.Background(), fanWrite.SubscribeTopic, "3"); err != nil {
		t.Fatalf("HandleMessage() error = %v", err)
	}

	dev.mu.Lock()
	defer dev.mu.Unlock()
	sends := 0
	for _, c := range dev.calls {
		if c == "130 5002 3" {
			sends++
		}
	}
	if sends != 1 {
		t.Errorf("command written %d times, want exactly 1", sends)
	}
}

// If the register cannot be read back, the write is unconfirmed and must be
// reported: the value may or may not have been applied.
func TestHandleMessageReportsUnconfirmedWrite(t *testing.T) {
	dev := &fakeDevice{} // neither echoes nor answers the verify read
	gate := NewReadGate(time.Second)
	w := testWriter(dev, newFakePublisher(), gate)

	err := w.HandleMessage(context.Background(), fanWrite.SubscribeTopic, "3")
	if err == nil {
		t.Fatal("HandleMessage() error = nil, want an unconfirmed-write error")
	}
	if !strings.Contains(err.Error(), "confirm") {
		t.Errorf("error = %v, want it to say the write could not be confirmed", err)
	}
	if gate.Suppressed() {
		t.Error("gate left suppressed")
	}
}

// A write register with nothing to verify against cannot be confirmed, but the
// command still goes out; that must not be reported as a failure.
func TestHandleMessageWithoutVerifyRegisters(t *testing.T) {
	reg := fanWrite
	reg.VerifyRegisters = nil
	dev := &fakeDevice{}
	pub := newFakePublisher()
	log := testLogger()
	w := NewWriter(dev, pub, log, []config.WriteRegisterConfig{reg}, NewReadGate(time.Second), 3, time.Millisecond, time.Millisecond)
	w.SetReader(NewReader(dev, pub, log, []config.ReadRegisterConfig{fanReg}, 1, time.Millisecond))

	if err := w.HandleMessage(context.Background(), reg.SubscribeTopic, "3"); err != nil {
		t.Fatalf("HandleMessage() error = %v, want success", err)
	}
	if v, ok := pub.get(reg.Topic); !ok || v != "3" {
		t.Errorf("state topic = %q (%v), want 3", v, ok)
	}
}

// A failure to even write the command must still be an error, and retried.
func TestHandleMessageRetriesAFailedSend(t *testing.T) {
	dev := &fakeDevice{sendErr: serial.ErrPortNotOpen}
	w := testWriter(dev, newFakePublisher(), NewReadGate(time.Second))

	if err := w.HandleMessage(context.Background(), fanWrite.SubscribeTopic, "3"); err == nil {
		t.Fatal("HandleMessage() error = nil, want the send failure reported")
	}
}

// The unit reports a fan setpoint on a different register than the one it is
// written to (write 5002, read back 5003, confirmed on hardware within ~30ms).
// Naming that register makes the write register's state topic carry what the
// unit actually did, not what was asked of it.
func TestHandleMessagePublishesStateFromReadBack(t *testing.T) {
	reg := fanWrite
	reg.StateFrom = "lueftermode"
	modeReg := config.ReadRegisterConfig{
		Name: "lueftermode", Command: "130 5003", Topic: "aerosmart/lueftermode",
		Divisor: 1, Type: "integer", MinValue: 0, MaxValue: 5,
	}

	// Asked for 3; the unit reports 3.
	dev := &fakeDevice{responses: map[string]string{"130 5003": "3", "130 1067": "3"}}
	pub := newFakePublisher()
	log := testLogger()
	w := NewWriter(dev, pub, log, []config.WriteRegisterConfig{reg}, NewReadGate(time.Second), 3, time.Millisecond, time.Millisecond)
	w.SetReader(NewReader(dev, pub, log, []config.ReadRegisterConfig{fanReg, modeReg}, 1, time.Millisecond))

	if err := w.HandleMessage(context.Background(), reg.SubscribeTopic, "3"); err != nil {
		t.Fatalf("HandleMessage() error = %v", err)
	}
	if v, ok := pub.get(reg.Topic); !ok || v != "3" {
		t.Errorf("state topic = %q (%v), want 3", v, ok)
	}
}

// If the unit refuses the value, the state topic must carry the truth rather
// than the value that was asked for, or Home Assistant shows a stage the
// device is not running.
func TestHandleMessagePublishesActualValueWhenUnitRefuses(t *testing.T) {
	reg := fanWrite
	reg.StateFrom = "lueftermode"
	modeReg := config.ReadRegisterConfig{
		Name: "lueftermode", Command: "130 5003", Topic: "aerosmart/lueftermode",
		Divisor: 1, Type: "integer", MinValue: 0, MaxValue: 5,
	}

	// Asked for 2, but the unit stays on 4.
	dev := &fakeDevice{responses: map[string]string{"130 5003": "4", "130 1067": "4"}}
	pub := newFakePublisher()
	log := testLogger()
	w := NewWriter(dev, pub, log, []config.WriteRegisterConfig{reg}, NewReadGate(time.Second), 3, time.Millisecond, time.Millisecond)
	w.SetReader(NewReader(dev, pub, log, []config.ReadRegisterConfig{fanReg, modeReg}, 1, time.Millisecond))

	err := w.HandleMessage(context.Background(), reg.SubscribeTopic, "2")
	if err == nil {
		t.Fatal("HandleMessage() error = nil, want the refusal reported")
	}
	if v, ok := pub.get(reg.Topic); !ok || v != "4" {
		t.Errorf("state topic = %q (%v), want the actual value 4, not the requested 2", v, ok)
	}
}

// Without state_from there is nothing authoritative to read, so the commanded
// value is published as before.
func TestHandleMessageFallsBackToCommandedValue(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "3"}}
	pub := newFakePublisher()
	w := testWriter(dev, pub, NewReadGate(time.Second))

	if err := w.HandleMessage(context.Background(), fanWrite.SubscribeTopic, "3"); err != nil {
		t.Fatalf("HandleMessage() error = %v", err)
	}
	if v, ok := pub.get(fanWrite.Topic); !ok || v != "3" {
		t.Errorf("state topic = %q (%v), want the commanded value 3", v, ok)
	}
}

// A verify register's value is published to its own topic; the log should say
// so, and the register must not be read twice when it is also state_from.
func TestVerifyRegistersArePublishedToTheirOwnTopics(t *testing.T) {
	reg := fanWrite
	reg.StateFrom = "lueftermode"
	reg.VerifyRegisters = []string{"luefterstatus", "lueftermode"}
	modeReg := config.ReadRegisterConfig{
		Name: "lueftermode", Command: "130 5003", Topic: "aerosmart/lueftermode",
		Divisor: 1, Type: "integer", MinValue: 0, MaxValue: 5,
	}

	dev := &fakeDevice{responses: map[string]string{"130 1067": "1", "130 5003": "4"}}
	pub := newFakePublisher()
	log := testLogger()
	w := NewWriter(dev, pub, log, []config.WriteRegisterConfig{reg}, NewReadGate(time.Second), 3, time.Millisecond, time.Millisecond)
	reader := NewReader(dev, pub, log, []config.ReadRegisterConfig{fanReg, modeReg}, 1, time.Millisecond)
	w.SetReader(reader)

	// Establish trusted values, so each read below is one transaction rather
	// than a reading plus its confirmation.
	for _, rr := range []config.ReadRegisterConfig{fanReg, modeReg} {
		if _, err := reader.ReadSingle(context.Background(), rr); err != nil {
			t.Fatalf("priming ReadSingle(%s) error = %v", rr.Name, err)
		}
	}
	dev.mu.Lock()
	dev.calls = nil
	dev.mu.Unlock()

	if err := w.HandleMessage(context.Background(), reg.SubscribeTopic, "4"); err != nil {
		t.Fatalf("HandleMessage() error = %v", err)
	}

	// The actual running stage reaches its own topic.
	if v, ok := pub.get("aerosmart/luefterstatus"); !ok || v != "1" {
		t.Errorf("aerosmart/luefterstatus = %q (%v), want 1", v, ok)
	}
	if v, ok := pub.get("aerosmart/lueftermode"); !ok || v != "4" {
		t.Errorf("aerosmart/lueftermode = %q (%v), want 4", v, ok)
	}
	if v, ok := pub.get(reg.Topic); !ok || v != "4" {
		t.Errorf("state topic = %q (%v), want 4", v, ok)
	}

	// lueftermode is both state_from and a verify register: reading it twice
	// puts an avoidable extra transaction on the wire for every write.
	dev.mu.Lock()
	defer dev.mu.Unlock()
	reads := 0
	for _, c := range dev.calls {
		if c == "130 5003" {
			reads++
		}
	}
	if reads != 1 {
		t.Errorf("lueftermode read %d times, want 1", reads)
	}
}

// ---------- review round 8 ----------

var modeReadReg = config.ReadRegisterConfig{
	Name: "lueftermode", Command: "130 5003", Topic: "aerosmart/lueftermode",
	Divisor: 1, Type: "integer", MinValue: 0, MaxValue: 5,
}

func stateWriter(t *testing.T, dev Transactor, pub Publisher, gate *ReadGate) (*Writer, config.WriteRegisterConfig) {
	t.Helper()
	reg := fanWrite
	reg.StateFrom = "lueftermode"
	reg.VerifyRegisters = []string{"luefterstatus", "lueftermode"}

	log := testLogger()
	w := NewWriter(dev, pub, log, []config.WriteRegisterConfig{reg}, gate, 3, time.Millisecond, time.Millisecond)
	w.SetReader(NewReader(dev, pub, log, []config.ReadRegisterConfig{fanReg, modeReadReg}, 1, time.Millisecond))
	return w, reg
}

// A settle re-read captures values before a newer command runs. Publishing
// them afterwards would leave Home Assistant showing the superseded mode, and
// the write register's state topic never self-corrects.
func TestRefreshReadBackSkipsSupersededWrite(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "4", "130 5003": "4"}}
	pub := newFakePublisher()
	w, reg := stateWriter(t, dev, pub, NewReadGate(time.Second))

	// A write of 4 happens, and its refresh is scheduled.
	if err := w.HandleMessage(context.Background(), reg.SubscribeTopic, "4"); err != nil {
		t.Fatalf("HandleMessage(4) error = %v", err)
	}
	stale := w.WriteSequence(reg.Name)

	// The user then changes to 2, which completes first.
	dev.mu.Lock()
	dev.responses["130 1067"] = "2"
	dev.responses["130 5003"] = "2"
	dev.mu.Unlock()
	if err := w.HandleMessage(context.Background(), reg.SubscribeTopic, "2"); err != nil {
		t.Fatalf("HandleMessage(2) error = %v", err)
	}

	// The older refresh now runs. It must not publish anything.
	w.refreshIfCurrent(context.Background(), reg, stale)

	if v, _ := pub.get(reg.Topic); v != "2" {
		t.Errorf("state topic = %q, want 2; a superseded refresh overwrote it", v)
	}
	if v, _ := pub.get("aerosmart/lueftermode"); v != "2" {
		t.Errorf("lueftermode = %q, want 2; a superseded refresh overwrote it", v)
	}
}

// A refresh that has not been superseded must still publish.
func TestRefreshReadBackRunsWhenCurrent(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "4", "130 5003": "4"}}
	pub := newFakePublisher()
	w, reg := stateWriter(t, dev, pub, NewReadGate(time.Second))

	if err := w.HandleMessage(context.Background(), reg.SubscribeTopic, "4"); err != nil {
		t.Fatalf("HandleMessage() error = %v", err)
	}
	seq := w.WriteSequence(reg.Name)

	// The unit settles on stage 1 under auto.
	dev.mu.Lock()
	dev.responses["130 1067"] = "1"
	dev.mu.Unlock()

	w.refreshIfCurrent(context.Background(), reg, seq)

	if v, _ := pub.get("aerosmart/luefterstatus"); v != "1" {
		t.Errorf("luefterstatus = %q, want the settled 1", v)
	}
}

// A command that reached the wire must be remembered even if the read-back
// disagreed, or the user's correction back to the previous value is dropped as
// a duplicate while the unit sits on the value they are trying to leave.
func TestDedupeDoesNotSwallowACorrectionAfterAnUnconfirmedWrite(t *testing.T) {
	// The unit never mirrors, so every write reports a mismatch.
	dev := &fakeDevice{responses: map[string]string{"130 1067": "9", "130 5003": "9"}}
	pub := newFakePublisher()
	w, reg := stateWriter(t, dev, pub, NewReadGate(time.Second))

	// 2 succeeds as far as being sent, then 4, then a correction back to 2.
	_ = w.HandleMessage(context.Background(), reg.SubscribeTopic, "2")
	_ = w.HandleMessage(context.Background(), reg.SubscribeTopic, "4")
	_ = w.HandleMessage(context.Background(), reg.SubscribeTopic, "2")

	dev.mu.Lock()
	defer dev.mu.Unlock()
	var sent []string
	for _, c := range dev.calls {
		if strings.HasPrefix(c, "130 5002") {
			sent = append(sent, c)
		}
	}
	want := []string{"130 5002 2", "130 5002 4", "130 5002 2"}
	if len(sent) != len(want) {
		t.Fatalf("commands sent = %v, want %v; the correction was dropped as a duplicate", sent, want)
	}
	for i := range want {
		if sent[i] != want[i] {
			t.Errorf("sent[%d] = %q, want %q", i, sent[i], want[i])
		}
	}
}

// A genuine broker redelivery of the last command is still dropped.
func TestDedupeStillDropsARedelivery(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "2", "130 5003": "2"}}
	w, reg := stateWriter(t, dev, newFakePublisher(), NewReadGate(time.Second))

	_ = w.HandleMessage(context.Background(), reg.SubscribeTopic, "2")
	before := dev.callCount()
	_ = w.HandleMessage(context.Background(), reg.SubscribeTopic, "2")

	if dev.callCount() != before {
		t.Error("a redelivery of the last command reached the device again")
	}
}

// The read cycle must keep a write register's state topic current, or a mode
// the unit leaves on its own - party reverting after ~90 minutes, or a change
// made at the unit's panel - never reaches Home Assistant.
func TestPollerPublishesWriteRegisterState(t *testing.T) {
	writeReg := fanWrite
	writeReg.StateFrom = "lueftermode"

	dev := &fakeDevice{responses: map[string]string{"130 1067": "1", "130 5003": "1"}}
	pub := newFakePublisher()
	log := testLogger()
	reader := NewReader(dev, pub, log, []config.ReadRegisterConfig{fanReg, modeReadReg}, 1, time.Millisecond)
	p := NewPoller(reader, nil, NewReadGate(time.Second), log, []config.WriteRegisterConfig{writeReg})

	if err := p.RunCycle(context.Background()); err != nil {
		t.Fatalf("RunCycle() error = %v", err)
	}
	if v, ok := pub.get(writeReg.Topic); !ok || v != "1" {
		t.Errorf("%s = %q (%v), want 1 from the read cycle", writeReg.Topic, v, ok)
	}
}

// A cycle cancelled after its last read must not publish its snapshot: a write
// that preempted it has already published newer values.
func TestPollerWithholdsPublishWhenCancelledAfterTheLastRead(t *testing.T) {
	dev := &fakeDevice{responses: map[string]string{"130 1067": "1"}}
	pub := newFakePublisher()
	gate := NewReadGate(time.Second)
	p := NewPoller(NewReader(dev, pub, testLogger(), []config.ReadRegisterConfig{fanReg}, 1, time.Millisecond),
		nil, gate, testLogger(), nil)

	// Cancel the moment the reads are done but before publishing.
	dev.afterCall = func(n int) {
		if n == 1 {
			gate.Preempt()
		}
	}

	err := p.RunCycle(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunCycle() error = %v, want context.Canceled", err)
	}
	if v, ok := pub.get("aerosmart/luefterstatus"); ok {
		t.Errorf("published %q from a cancelled cycle; a preempting write's values would be overwritten", v)
	}
}
