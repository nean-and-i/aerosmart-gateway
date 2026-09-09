package registers

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/nean/aerosmart-gateway/internal/config"
	"github.com/nean/aerosmart-gateway/internal/logger"
	"github.com/nean/aerosmart-gateway/internal/serial"
)

// Transactor performs one request/response exchange with the device.
type Transactor interface {
	// Transact writes a command and returns the value from the matching
	// response frame.
	Transact(ctx context.Context, command string) (string, error)
	// ResponseTimeout is the budget one exchange is allowed.
	ResponseTimeout() time.Duration
	// IsHealthy reports whether the port is usable.
	IsHealthy() bool
}

// Publisher publishes a value to a topic.
type Publisher interface {
	Publish(topic, value string) error
}

// RegisterValue holds a register value with metadata.
type RegisterValue struct {
	Name     string
	Value    string
	RawValue string
	Valid    bool
}

// Reader reads registers from the device.
type Reader struct {
	dev       Transactor
	pub       Publisher
	logger    *logger.Logger
	registers []config.ReadRegisterConfig
	byName    map[string]config.ReadRegisterConfig

	// retries is the number of attempts per register. It is deliberately the
	// only retry level: nesting retries multiplies the worst case into hours.
	retries int
	backoff time.Duration

	mu     sync.RWMutex
	values map[string]*RegisterValue
}

// NewReader creates a register reader. retries counts total attempts per
// register (not additional ones) and backoff is the delay between them.
func NewReader(dev Transactor, pub Publisher, log *logger.Logger, registers []config.ReadRegisterConfig, retries int, backoff time.Duration) *Reader {
	if retries < 1 {
		retries = 1
	}
	byName := make(map[string]config.ReadRegisterConfig, len(registers))
	for _, reg := range registers {
		byName[reg.Name] = reg
	}
	return &Reader{
		dev:       dev,
		pub:       pub,
		logger:    log,
		registers: registers,
		byName:    byName,
		retries:   retries,
		backoff:   backoff,
		values:    make(map[string]*RegisterValue),
	}
}

// Register returns the configuration for a register by name.
func (r *Reader) Register(name string) (config.ReadRegisterConfig, bool) {
	reg, ok := r.byName[name]
	return reg, ok
}

// ReadSingle reads one register, retrying transient failures.
//
// It gives up immediately on context cancellation (a write is waiting) and on
// a port-level error (the supervisor owns reconnection; retrying here would
// only burn the cycle budget against a dead port).
func (r *Reader) ReadSingle(ctx context.Context, register config.ReadRegisterConfig) (string, error) {
	var lastErr error

	for attempt := 1; attempt <= r.retries; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if attempt > 1 && !sleepCtx(ctx, r.backoff) {
			return "", ctx.Err()
		}

		start := time.Now()
		value, err := r.dev.Transact(ctx, register.Command)
		duration := time.Since(start)

		if err == nil {
			r.logger.SerialOperation("READ", register.Command, value, duration, nil)
			return r.processValue(value, register), nil
		}

		lastErr = err
		r.logger.SerialOperation("READ", register.Command, "", duration, err)

		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if serial.IsPortError(err) {
			break
		}
		r.logger.Debug("Read of %s failed (attempt %d/%d): %v", register.Name, attempt, r.retries, err)
	}

	return "", fmt.Errorf("read register %s (%s): %w", register.Name, register.Command, lastErr)
}

// ReadAll reads every configured register.
//
// Registers that cannot be read are recorded as invalid so they are not
// republished with a stale value. A cancelled cycle is reported as an error,
// so the caller does not publish partial results or derive values from
// missing sources.
func (r *Reader) ReadAll(ctx context.Context) (map[string]*RegisterValue, error) {
	results := make(map[string]*RegisterValue, len(r.registers))
	cycleStart := time.Now()

	r.logger.Info("=== SERIAL: Starting register read cycle ===")

	for _, register := range r.registers {
		if err := ctx.Err(); err != nil {
			r.logger.Info("=== SERIAL: read cycle cancelled after %v (write priority) ===", time.Since(cycleStart))
			return results, err
		}

		value, err := r.ReadSingle(ctx, register)
		if err != nil {
			if isCancellation(err) {
				r.logger.Info("=== SERIAL: read cycle cancelled after %v (write priority) ===", time.Since(cycleStart))
				return results, err
			}
			r.logger.Warn("Failed to read register %s: %v", register.Name, err)
			results[register.Name] = &RegisterValue{Name: register.Name, Valid: false}
			continue
		}

		valid, processed := r.ValidateValue(value, register)
		results[register.Name] = &RegisterValue{
			Name:     register.Name,
			Value:    processed,
			RawValue: value,
			Valid:    valid,
		}
		r.logger.Debug("Register %s = %s (valid: %v)", register.Name, processed, valid)
	}

	r.logger.Info("=== SERIAL: Register read cycle completed in %v ===", time.Since(cycleStart))

	r.mu.Lock()
	r.values = results
	r.mu.Unlock()

	return results, nil
}

// PublishAll publishes valid register values. Invalid values are skipped: with
// retained MQTT messages, republishing a stale reading would make a dead
// sensor look alive. Home Assistant's expire_after handles the rest.
func (r *Reader) PublishAll(values map[string]*RegisterValue) error {
	var firstErr error
	skipped := 0

	for name, regVal := range values {
		if regVal == nil || !regVal.Valid {
			skipped++
			continue
		}
		reg, ok := r.byName[name]
		if !ok || reg.Topic == "" {
			continue
		}
		if err := r.pub.Publish(reg.Topic, regVal.Value); err != nil {
			r.logger.Error("Failed to publish %s to %s: %v", name, reg.Topic, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		r.logger.Debug("Published %s = %s to %s", name, regVal.Value, reg.Topic)
	}

	if skipped > 0 {
		r.logger.Warn("Skipped publishing %d/%d register(s) that could not be read", skipped, len(values))
	}
	return firstErr
}

// Values returns a snapshot of the most recent read cycle.
func (r *Reader) Values() map[string]*RegisterValue {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make(map[string]*RegisterValue, len(r.values))
	for k, v := range r.values {
		copied := *v
		out[k] = &copied
	}
	return out
}

// processValue applies the divisor and formats according to the register type.
func (r *Reader) processValue(rawValue string, register config.ReadRegisterConfig) string {
	floatVal, err := strconv.ParseFloat(rawValue, 64)
	if err != nil {
		r.logger.Warn("Failed to parse value %q for %s as a number: %v", rawValue, register.Name, err)
		return rawValue
	}

	if register.Divisor > 1 {
		floatVal /= float64(register.Divisor)
	}

	if register.Type == "float" {
		return fmt.Sprintf("%.1f", math.Round(floatVal*10)/10)
	}
	return fmt.Sprintf("%.0f", math.Round(floatVal))
}

// ValidateValue checks the value against the register's configured range.
func (r *Reader) ValidateValue(value string, register config.ReadRegisterConfig) (bool, string) {
	floatVal, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return false, value
	}

	if floatVal < float64(register.MinValue) || floatVal > float64(register.MaxValue) {
		r.logger.Warn("Value %s out of range for %s (min: %d, max: %d)",
			value, register.Name, register.MinValue, register.MaxValue)

		// The unit reports a transient status above the documented maximum
		// while the fan is ramping; clamping keeps the entity usable.
		if register.Name == "luefterstatus" && floatVal > float64(register.MaxValue) {
			return true, strconv.Itoa(register.MaxValue)
		}
		return false, value
	}

	return true, value
}

// isCancellation reports whether err is a context cancellation or deadline.
func isCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
