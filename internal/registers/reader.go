package registers

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/nean/aerosmart-gateway/internal/config"
	"github.com/nean/aerosmart-gateway/internal/logger"
	"github.com/nean/aerosmart-gateway/internal/serial"
)

// ErrUnconfirmed means a register answered, but no reading could be trusted:
// it changed and no second read agreed with it.
var ErrUnconfirmed = errors.New("reading could not be confirmed")

// Transactor performs one request/response exchange with the device.
type Transactor interface {
	// Transact writes a command and returns the value from the matching
	// response frame. Used for reads, which the unit answers reliably.
	Transact(ctx context.Context, command string) (string, error)
	// Send writes a command without expecting a matching response frame.
	// Used for register writes, which the unit does not reliably echo.
	Send(ctx context.Context, command string) error
	// IsHealthy reports whether the port is usable, so a cycle can be skipped
	// while the port is being recovered.
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

	// retries is the number of attempts per register, shared between retrying
	// failures and confirming readings. It is deliberately the only retry
	// level: nesting retries multiplies the worst case into hours.
	retries int
	backoff time.Duration

	// trusted holds the last trusted value of each register, in processed
	// units. A reading that agrees with it needs no confirmation. The command
	// worker and settle re-reads read registers concurrently with the poll
	// cycle, hence the lock.
	trustMu sync.Mutex
	trusted map[string]float64
}

// NewReader creates a register reader. retries counts total attempts per
// register (not additional ones) and backoff is the delay between them.
//
// Confirming a changed value takes a second read, so there are always at
// least two attempts; with one, no value could ever change.
func NewReader(dev Transactor, pub Publisher, log *logger.Logger, registers []config.ReadRegisterConfig, retries int, backoff time.Duration) *Reader {
	retries = max(retries, 2)
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
		trusted:   make(map[string]float64),
	}
}

// DeviceHealthy reports whether the underlying port is usable.
func (r *Reader) DeviceHealthy() bool { return r.dev.IsHealthy() }

// Register returns the configuration for a register by name.
func (r *Reader) Register(name string) (config.ReadRegisterConfig, bool) {
	reg, ok := r.byName[name]
	return reg, ok
}

// ReadSingle reads one register and returns a trusted, validated value.
//
// It gives up immediately on context cancellation (a write is waiting) and on
// a port-level error (the supervisor owns reconnection; retrying here would
// only burn the cycle budget against a dead port).
func (r *Reader) ReadSingle(ctx context.Context, register config.ReadRegisterConfig) (string, error) {
	processed, _, err := r.readSingleRaw(ctx, register)
	return processed, err
}

// sample is one validated reading.
type sample struct {
	value float64
	text  string // processed and formatted
	raw   string // as the device reported it
}

// readSingleRaw reads one register until a reading can be trusted, and
// returns both the processed value and the value the device actually
// reported, which is what makes a suspicious reading traceable.
//
// The unit's panel shares the bus and frames carry no checksum, so a
// collision can turn a reply into a different but well-formed number. A
// reading is trusted when it is within confirm_step of the last trusted value,
// or when another reading in this call agrees with it. Out-of-range readings
// are treated as corrupted. Failures, confirmations and tie-breaks all share
// one budget of attempts, so confirming never lengthens the worst case.
func (r *Reader) readSingleRaw(ctx context.Context, register config.ReadRegisterConfig) (string, string, error) {
	var lastErr error
	var samples []sample

	for attempt := 1; attempt <= r.retries; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", "", err
		}
		if attempt > 1 && !sleepCtx(ctx, jittered(r.backoff)) {
			return "", "", ctx.Err()
		}

		start := time.Now()
		value, err := r.dev.Transact(ctx, register.Command)
		duration := time.Since(start)

		if err != nil {
			lastErr = err

			// A write preempting this read cancels ctx, and Transact reports
			// that as an ordinary error. Logging it through SerialOperation -
			// the documented primary flaky-link diagnostic, at WARN - would
			// mean every write pollutes it with a false alarm, drowning out
			// genuine device problems. Check before logging, not after.
			if ctx.Err() != nil {
				r.logger.Debug("Read of %s cancelled: %v", register.Name, ctx.Err())
				return "", "", ctx.Err()
			}
			r.logger.SerialOperation("READ", register.Command, "", duration, err)
			if serial.IsPortError(err) {
				break
			}
			r.logger.Debug("Read of %s failed (attempt %d/%d): %v", register.Name, attempt, r.retries, err)
			continue
		}
		r.logger.SerialOperation("READ", register.Command, value, duration, nil)

		s, err := r.toSample(value, register)
		if err != nil {
			lastErr = err
			continue
		}
		if r.isTrusted(register, s.value) {
			r.trust(register.Name, s.value)
			return s.text, s.raw, nil
		}
		for _, earlier := range samples {
			if earlier.text == s.text {
				r.trust(register.Name, s.value)
				return s.text, s.raw, nil
			}
		}
		samples = append(samples, s)
	}

	if len(samples) > 0 {
		readings := make([]string, len(samples))
		for i, s := range samples {
			readings[i] = s.raw
		}
		r.logger.Warn("Read of %s: readings %v disagree, keeping the last trusted value", register.Name, readings)
		return "", "", fmt.Errorf("read register %s (%s): %w: readings %v disagree",
			register.Name, register.Command, ErrUnconfirmed, readings)
	}
	return "", "", fmt.Errorf("read register %s (%s): %w", register.Name, register.Command, lastErr)
}

// toSample processes and validates one reading. An out-of-range value is an
// error: on a shared bus it is most likely a corrupted frame.
func (r *Reader) toSample(raw string, register config.ReadRegisterConfig) (sample, error) {
	text := r.processValue(raw, register)
	valid, checked := r.ValidateValue(text, register)
	if !valid {
		return sample{}, fmt.Errorf("value %q out of range for %s", raw, register.Name)
	}
	value, err := strconv.ParseFloat(checked, 64)
	if err != nil {
		return sample{}, fmt.Errorf("value %q for %s is not a number: %w", raw, register.Name, err)
	}
	return sample{value: value, text: checked, raw: raw}, nil
}

// isTrusted reports whether value is close enough to the register's last
// trusted value to need no confirmation.
func (r *Reader) isTrusted(register config.ReadRegisterConfig, value float64) bool {
	r.trustMu.Lock()
	defer r.trustMu.Unlock()
	last, ok := r.trusted[register.Name]
	return ok && math.Abs(value-last) <= register.ConfirmStep
}

// trust records a register's latest trusted value.
func (r *Reader) trust(name string, value float64) {
	r.trustMu.Lock()
	defer r.trustMu.Unlock()
	r.trusted[name] = value
}

// jittered returns d stretched by a random amount of up to half again. A fixed
// delay between attempts can keep landing on the panel's own polling rhythm
// and collide every time.
func jittered(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	return d + rand.N(d/2+1)
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

		value, raw, err := r.readSingleRaw(ctx, register)
		if err != nil {
			if isCancellation(err) {
				r.logger.Info("=== SERIAL: read cycle cancelled after %v (write priority) ===", time.Since(cycleStart))
				return results, err
			}
			r.logger.Warn("Failed to read register %s: %v", register.Name, err)
			results[register.Name] = &RegisterValue{Name: register.Name, Valid: false}
			continue
		}

		results[register.Name] = &RegisterValue{
			Name:     register.Name,
			Value:    value,
			RawValue: raw,
			Valid:    true,
		}
		r.logger.Debug("Register %s = %s", register.Name, value)
	}

	r.logger.Info("=== SERIAL: Register read cycle completed in %v ===", time.Since(cycleStart))

	return results, nil
}

// PublishAll publishes valid register values. Invalid values are skipped: with
// retained MQTT messages, republishing a stale reading would make a dead
// sensor look alive. Home Assistant's expire_after handles the rest.
//
// Publishing 30+ registers is 30+ bounded broker round trips, so a write can
// preempt the cycle partway through. It stops there and reports the
// cancellation: the rest of the snapshot is older than the values that write
// has just published, and would overwrite them.
func (r *Reader) PublishAll(ctx context.Context, values map[string]*RegisterValue) error {
	var firstErr error
	skipped := 0

	for name, regVal := range values {
		if err := ctx.Err(); err != nil {
			return err
		}
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

	return formatByType(floatVal, register.Type)
}

// formatByType renders a value the same way processValue does, once the
// divisor - if any - has already been applied. It is also what a write
// register's commanded value is formatted through before being compared
// against a state_from reading: the command value is already in the read
// register's processed (post-divisor) units, so only the type formatting,
// not the divisor, applies to it.
func formatByType(floatVal float64, registerType string) string {
	if registerType == "float" {
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
		// Out-of-range values used to be clamped for luefterstatus. On a
		// shared bus most of them are corrupted frames, and clamping turned
		// garbage into a confident "highest stage".
		r.logger.Warn("Value %s out of range for %s (min: %d, max: %d)",
			value, register.Name, register.MinValue, register.MaxValue)
		return false, value
	}

	return true, value
}

// isCancellation reports whether err is a context cancellation or deadline.
func isCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
