package registers

import (
	"context"
	"errors"

	"github.com/nean/aerosmart-gateway/internal/config"

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

	// writeRegisters carry their own state topic, which only a write would
	// otherwise ever publish. The unit changes these by itself - party mode
	// reverts after about 90 minutes, and the mode can be changed at the
	// unit's panel - so each cycle republishes them from their state_from
	// register. Without this the Home Assistant number and switch entities
	// keep the value from the last write indefinitely, including across a
	// gateway restart.
	writeRegisters []config.WriteRegisterConfig
}

// NewPoller creates a poller. derived may be nil.
func NewPoller(reader *Reader, derived *DerivedCalculator, gate *ReadGate, log *logger.Logger, writeRegisters []config.WriteRegisterConfig) *Poller {
	return &Poller{
		reader:         reader,
		derived:        derived,
		gate:           gate,
		logger:         log,
		writeRegisters: writeRegisters,
	}
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

	// A write can preempt the cycle after its last read. Publishing the
	// snapshot then would overwrite the newer values that write just
	// published, and they would not be corrected until the next cycle.
	if err := cycleCtx.Err(); err != nil {
		p.logger.Info("=== Read cycle preempted before publishing, discarding the snapshot ===")
		return err
	}

	if err := p.reader.PublishAll(values); err != nil {
		p.logger.Error("Failed to publish some register values: %v", err)
	}

	p.publishWriteRegisterState(values)

	if p.derived != nil {
		if err := p.derived.PublishAll(p.derived.Calculate(values)); err != nil {
			p.logger.Error("Failed to publish derived register values: %v", err)
		}
	}

	return nil
}

// publishWriteRegisterState republishes each write register's state from the
// read register that carries its real value.
func (p *Poller) publishWriteRegisterState(values map[string]*RegisterValue) {
	for _, reg := range p.writeRegisters {
		if reg.StateFrom == "" || reg.Topic == "" {
			continue
		}
		source, ok := values[reg.StateFrom]
		if !ok || source == nil || !source.Valid {
			continue
		}
		if err := p.reader.pub.Publish(reg.Topic, source.Value); err != nil {
			p.logger.Error("Failed to publish state for %s to %s: %v", reg.Name, reg.Topic, err)
			continue
		}
		p.logger.Debug("Published %s state = %s to %s", reg.Name, source.Value, reg.Topic)
	}
}
