package registers

import (
	"context"
	"time"

	"github.com/nean/aerosmart-gateway/internal/config"
	"github.com/nean/aerosmart-gateway/internal/logger"
)

// refreshFunc re-reads a write register's read-back registers and publishes
// them.
type refreshFunc func(ctx context.Context, register config.WriteRegisterConfig)

// resettler re-reads a write register's read-back registers a short while
// after a write.
//
// Some values trail the command: the actual fan stage still reported the
// previous value 60ms after a stage change, so the immediate read-back
// publishes a number the unit is already moving away from. Waiting for the
// next full read cycle would leave Home Assistant wrong for up to the whole
// read interval.
//
// The wait restarts on every write, so changing stage repeatedly produces one
// read after the changes stop rather than one per command.
type resettler struct {
	delay   time.Duration
	refresh refreshFunc
	logger  *logger.Logger

	// pending holds the most recent register awaiting a re-read. Capacity 1
	// and latest-wins, so scheduling never blocks the write path.
	pending chan config.WriteRegisterConfig
}

func newResettler(delay time.Duration, refresh refreshFunc, log *logger.Logger) *resettler {
	return &resettler{
		delay:   delay,
		refresh: refresh,
		logger:  log,
		pending: make(chan config.WriteRegisterConfig, 1),
	}
}

// schedule asks for a re-read after the delay, replacing any pending request.
// It runs on the write path and never blocks.
func (r *resettler) schedule(register config.WriteRegisterConfig) {
	if r.delay <= 0 {
		return
	}

	// Drop any superseded request, then queue this one. The two steps are not
	// atomic, but only one goroutine schedules, so the worst case is that the
	// consumer sees an older register a moment before the newer one.
	select {
	case <-r.pending:
	default:
	}
	select {
	case r.pending <- register:
	default:
	}
}

// run performs re-reads until ctx is done.
func (r *resettler) run(ctx context.Context) {
	if r.delay <= 0 {
		<-ctx.Done()
		return
	}

	timer := time.NewTimer(r.delay)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	var waiting config.WriteRegisterConfig
	armed := false

	for {
		select {
		case <-ctx.Done():
			return

		case register := <-r.pending:
			// Restart the wait: another change may still be coming.
			if armed && !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			waiting = register
			armed = true
			timer.Reset(r.delay)

		case <-timer.C:
			armed = false
			r.logger.Debug("Re-reading %s after %v to catch a settled value", waiting.Name, r.delay)
			r.refresh(ctx, waiting)
		}
	}
}
