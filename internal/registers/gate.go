package registers

import (
	"context"
	"sync"
	"time"
)

// ReadGate coordinates the periodic read cycle with incoming write commands.
//
// A write must reach the device promptly, so it cancels any in-flight read
// cycle and holds further cycles off until it has finished. Suppression always
// carries a deadline: a release that never happens must not wedge reads
// permanently.
type ReadGate struct {
	mu sync.Mutex
	// cancel belongs to the cycle currently running, and is cleared when that
	// cycle ends so a later Preempt cannot cancel an already-finished one.
	// cycleID identifies which cycle owns it.
	cancel  context.CancelFunc
	cycleID uint64
	until   time.Time
	timeout time.Duration

	// holds counts the callers currently suppressing reads. The command
	// worker and the settle re-read run on separate goroutines and can hold
	// the gate at the same time, so a plain flag let whichever finished first
	// lift the other's hold and admit a cycle mid-write.
	holds int
}

// NewReadGate returns a gate whose suppression expires after timeout.
func NewReadGate(timeout time.Duration) *ReadGate {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &ReadGate{timeout: timeout}
}

// TryBeginCycle starts a read cycle unless a write is pending.
//
// Testing suppression and registering the cycle happen under one lock: done
// separately, a write arriving in between would preempt a cycle that had not
// started yet and then let it run unimpeded.
//
// The returned cancel also deregisters the cycle, and must always be called.
func (g *ReadGate) TryBeginCycle(parent context.Context) (context.Context, context.CancelFunc, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.suppressedLocked() {
		return nil, nil, false
	}

	ctx, cancel := context.WithCancel(parent)
	g.cycleID++
	id := g.cycleID
	g.cancel = cancel

	return ctx, func() {
		cancel()
		g.mu.Lock()
		// Only deregister if this cycle is still the current one.
		if g.cycleID == id {
			g.cancel = nil
		}
		g.mu.Unlock()
	}, true
}

// Preempt cancels the in-flight read cycle and suppresses further cycles until
// a matching Release, or until the timeout elapses.
//
// Each Preempt must be paired with exactly one Release.
func (g *ReadGate) Preempt() {
	g.mu.Lock()
	cancel := g.cancel
	g.cancel = nil
	// Re-read the expiry from now: the deadline covers the newest holder's
	// work, not whatever remained of an earlier one's.
	g.holds++
	g.until = time.Now().Add(g.timeout)
	g.mu.Unlock()

	if cancel != nil {
		cancel()
	}
}

// Release gives up one hold, lifting suppression once the last one is gone.
func (g *ReadGate) Release() {
	g.mu.Lock()
	if g.holds > 0 {
		g.holds--
	}
	if g.holds == 0 {
		g.until = time.Time{}
	}
	g.mu.Unlock()
}

// Suppressed reports whether reads are currently held off.
func (g *ReadGate) Suppressed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.suppressedLocked()
}

// suppressedLocked clears expired suppression as a side effect.
//
// The deadline also drops every outstanding hold: it exists so a Release that
// never happens cannot wedge polling, which a surviving count would defeat.
func (g *ReadGate) suppressedLocked() bool {
	if g.until.IsZero() {
		return false
	}
	if !time.Now().Before(g.until) {
		g.until = time.Time{}
		g.holds = 0
		return false
	}
	return true
}

// hasActiveCycle reports whether a cycle is registered. Used in tests.
func (g *ReadGate) hasActiveCycle() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cancel != nil
}

// sleepCtx waits for d, reporting false if ctx finished first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
