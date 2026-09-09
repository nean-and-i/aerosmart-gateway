package registers

import (
	"context"
	"errors"

	"github.com/nean/aerosmart-gateway/internal/logger"
)

// ErrCycleSkipped means no readout happened: a write was in flight or the port
// was being recovered. Callers should retry sooner than a full interval rather
// than treating it as a completed cycle.
var ErrCycleSkipped = errors.New("read cycle skipped")

// Poller runs one read/publish cycle: read every register, publish the valid
// ones, then compute and publish derived values from that same snapshot.
type Poller struct {
	reader  *Reader
	derived *DerivedCalculator
	gate    *ReadGate
	logger  *logger.Logger
}

// NewPoller creates a poller. derived may be nil.
func NewPoller(reader *Reader, derived *DerivedCalculator, gate *ReadGate, log *logger.Logger) *Poller {
	return &Poller{reader: reader, derived: derived, gate: gate, logger: log}
}

// RunCycle performs one full readout, unless a write is currently in flight.
//
// Nothing is published for a cancelled cycle: partial results would publish
// some registers from this cycle and leave others stale, and derived values
// would be computed from sources that were never read.
func (p *Poller) RunCycle(ctx context.Context) error {
	// Reading 30+ registers through their retries against a port the
	// supervisor is still reopening would burn the whole interval.
	if !p.reader.DeviceHealthy() {
		p.logger.Warn("=== Serial port unhealthy, skipping this read cycle ===")
		return ErrCycleSkipped
	}

	cycleCtx, cancel, ok := p.gate.TryBeginCycle(ctx)
	if !ok {
		p.logger.Info("=== Write in progress, skipping this read cycle ===")
		return ErrCycleSkipped
	}
	defer cancel()

	values, err := p.reader.ReadAll(cycleCtx)
	if err != nil {
		return err
	}

	if err := p.reader.PublishAll(values); err != nil {
		p.logger.Error("Failed to publish some register values: %v", err)
	}

	if p.derived != nil {
		derivedValues, err := p.derived.Calculate(values)
		if err != nil {
			p.logger.Error("Failed to calculate derived registers: %v", err)
		}
		if err := p.derived.PublishAll(derivedValues); err != nil {
			p.logger.Error("Failed to publish derived register values: %v", err)
		}
	}

	return nil
}
