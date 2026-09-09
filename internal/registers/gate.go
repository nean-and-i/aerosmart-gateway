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
	mu      sync.Mutex
	cancel  context.CancelFunc
	until   time.Time
	timeout time.Duration
}

// NewReadGate returns a gate whose suppression expires after timeout.
func NewReadGate(timeout time.Duration) *ReadGate {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &ReadGate{timeout: timeout}
}

// BeginCycle derives the context for a read cycle and registers it so an
// incoming write can cancel it. The caller must still call the returned cancel.
func (g *ReadGate) BeginCycle(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	g.mu.Lock()
	g.cancel = cancel
	g.mu.Unlock()
	return ctx, cancel
}

// Preempt cancels the in-flight read cycle and suppresses further cycles until
// Release, or until the timeout elapses.
func (g *ReadGate) Preempt() {
	g.mu.Lock()
	cancel := g.cancel
	g.cancel = nil
	g.until = time.Now().Add(g.timeout)
	g.mu.Unlock()

	if cancel != nil {
		cancel()
	}
}

// Release lifts suppression so read cycles can resume.
func (g *ReadGate) Release() {
	g.mu.Lock()
	g.until = time.Time{}
	g.mu.Unlock()
}

// Suppressed reports whether reads are currently held off. Expired
// suppression is cleared as a side effect.
func (g *ReadGate) Suppressed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.until.IsZero() {
		return false
	}
	if !time.Now().Before(g.until) {
		g.until = time.Time{}
		return false
	}
	return true
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
