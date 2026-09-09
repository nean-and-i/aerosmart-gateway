package registers

import (
	"context"

	"github.com/nean/aerosmart-gateway/internal/logger"
)

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
	if p.gate.Suppressed() {
		p.logger.Info("=== Write in progress, skipping this read cycle ===")
		return nil
	}

	cycleCtx, cancel := p.gate.BeginCycle(ctx)
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
