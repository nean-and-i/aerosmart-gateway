package registers

import (
	"context"
	"sync"
	"time"

	"github.com/nean/aerosmart-gateway/internal/config"
	"github.com/nean/aerosmart-gateway/internal/logger"
)

// refreshFunc re-reads a write register's read-back registers and publishes
// them, unless the command identified by seq has been superseded.
type refreshFunc func(ctx context.Context, register config.WriteRegisterConfig, seq uint64)

// pendingRefresh is one register awaiting a re-read.
type pendingRefresh struct {
	register config.WriteRegisterConfig
	seq      uint64
	due      time.Time
}

// resettler re-reads a write register's read-back registers a short while
// after a write.
//
// Some values trail the command: the actual fan stage still reported the
// previous value 60ms after a stage change, and for the auto and party modes
// the running stage is decided by the unit rather than by the command, so it
// is not known when the write returns. Waiting for the next full read cycle
// would leave Home Assistant wrong for up to the whole read interval.
//
// Each register has its own pending entry, so commanding a second register
// does not discard the first one's re-read. Repeating a command for the same
// register restarts that register's wait, so a burst of stage changes produces
// one read after the changes stop.
type resettler struct {
	delay   time.Duration
	refresh refreshFunc
	logger  *logger.Logger

	mu      sync.Mutex
	pending map[string]pendingRefresh

	// wake signals the run loop that pending changed. Capacity 1: a missed
	// signal is harmless because the loop re-derives its next deadline from
	// the map.
	wake chan struct{}
}

func newResettler(delay time.Duration, refresh refreshFunc, log *logger.Logger) *resettler {
	return &resettler{
		delay:   delay,
		refresh: refresh,
		logger:  log,
		pending: make(map[string]pendingRefresh),
		wake:    make(chan struct{}, 1),
	}
}

// schedule asks for a re-read of this register after the delay, replacing any
// pending request for it. It runs on the write path and never blocks.
func (r *resettler) schedule(register config.WriteRegisterConfig, seq uint64) {
	if r.delay <= 0 {
		return
	}

	r.mu.Lock()
	r.pending[register.Name] = pendingRefresh{
		register: register,
		seq:      seq,
		due:      time.Now().Add(r.delay),
	}
	r.mu.Unlock()

	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// due returns the entries ready to run and how long until the next one is.
// A zero duration means nothing is waiting.
func (r *resettler) due(now time.Time) ([]pendingRefresh, time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var ready []pendingRefresh
	var next time.Duration

	for name, entry := range r.pending {
		if !entry.due.After(now) {
			ready = append(ready, entry)
			delete(r.pending, name)
			continue
		}
		if wait := entry.due.Sub(now); next == 0 || wait < next {
			next = wait
		}
	}
	return ready, next
}

// run performs re-reads until ctx is done.
func (r *resettler) run(ctx context.Context) {
	if r.delay <= 0 {
		<-ctx.Done()
		return
	}

	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		ready, next := r.due(time.Now())
		for _, entry := range ready {
			if ctx.Err() != nil {
				return
			}
			r.logger.Debug("Re-reading %s after %v to catch a settled value",
				entry.register.Name, r.delay)
			r.refresh(ctx, entry.register, entry.seq)
		}

		if next == 0 {
			// Nothing waiting: sleep until something is scheduled.
			select {
			case <-ctx.Done():
				return
			case <-r.wake:
			}
			continue
		}

		timer.Reset(next)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-r.wake:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
}
