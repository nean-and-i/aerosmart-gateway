package registers

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nean/aerosmart-gateway/internal/config"
	"github.com/nean/aerosmart-gateway/internal/logger"
	"github.com/nean/aerosmart-gateway/internal/mqtt"
)

// Writer applies incoming MQTT commands to device registers.
type Writer struct {
	dev       Transactor
	pub       Publisher
	logger    *logger.Logger
	registers []config.WriteRegisterConfig
	byTopic   map[string]config.WriteRegisterConfig
	reader    *Reader
	gate      *ReadGate

	retries int
	backoff time.Duration

	// settleDelay is how long to wait after writing before reading the value
	// back, giving the unit time to apply it.
	settleDelay time.Duration

	// lastCommand remembers the last command per topic that actually reached
	// the device, so a broker redelivery is dropped while a retry after a
	// failure, and a change back to an earlier value, both go through.
	dedupeMu     sync.Mutex
	lastCommand  map[string]lastWrite
	dedupeWindow time.Duration
}

// NewWriter creates a register writer. retries counts total attempts per
// command; the gate is used to preempt in-flight read cycles.
func NewWriter(dev Transactor, pub Publisher, log *logger.Logger, registers []config.WriteRegisterConfig, gate *ReadGate, retries int, backoff time.Duration, settleDelay time.Duration) *Writer {
	if retries < 1 {
		retries = 1
	}
	byTopic := make(map[string]config.WriteRegisterConfig, len(registers))
	for _, reg := range registers {
		if reg.SubscribeTopic != "" {
			byTopic[reg.SubscribeTopic] = reg
		}
	}
	return &Writer{
		dev:          dev,
		pub:          pub,
		logger:       log,
		registers:    registers,
		byTopic:      byTopic,
		gate:         gate,
		retries:      retries,
		backoff:      backoff,
		settleDelay:  settleDelay,
		lastCommand:  make(map[string]lastWrite),
		dedupeWindow: time.Second,
	}
}

// SetReader supplies the reader used to verify writes.
func (w *Writer) SetReader(reader *Reader) { w.reader = reader }

// GetSubscribeTopics returns the command topics to subscribe to.
func (w *Writer) GetSubscribeTopics() []string {
	topics := make([]string, 0, len(w.registers))
	for _, reg := range w.registers {
		if reg.SubscribeTopic != "" {
			topics = append(topics, reg.SubscribeTopic)
		}
	}
	return topics
}

// HandleMessage applies one command to the device.
//
// The command is fully validated before the read cycle is disturbed, so a
// malformed payload cannot cancel a readout. Once a valid command is accepted,
// the gate is released on every exit path: leaving it suppressed used to block
// reads until the suppression timeout expired.
func (w *Writer) HandleMessage(ctx context.Context, topic string, message string) error {
	received := time.Now()

	register, ok := w.byTopic[topic]
	if !ok {
		w.logger.Warn("MQTT: no write register configured for topic %s", topic)
		return nil
	}

	valueStr := strings.TrimSpace(message)
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		w.logger.Error("MQTT: %s expects an integer, got %q", register.Name, message)
		return fmt.Errorf("invalid value %q for %s: %w", message, register.Name, err)
	}
	if value < register.MinValue || value > register.MaxValue {
		w.logger.Error("MQTT: value %d out of range for %s (min: %d, max: %d)",
			value, register.Name, register.MinValue, register.MaxValue)
		return fmt.Errorf("value %d out of range for %s", value, register.Name)
	}

	if w.isDuplicate(topic, valueStr) {
		w.logger.Info("MQTT: dropping %s=%s, an identical command succeeded within %v",
			topic, valueStr, w.dedupeWindow)
		return nil
	}

	// From here on a real command is in flight: preempt reads, and always
	// hand control back.
	w.gate.Preempt()
	defer w.gate.Release()

	command := mqtt.ParseWriteRegisterCommand(register.CommandTemplate, valueStr)
	w.logger.Info("=== Writing %s = %d (%q) ===", register.Name, value, command)

	sendStart := time.Now()
	if err := w.sendCommand(ctx, command); err != nil {
		w.logger.SerialSend(command, time.Since(sendStart), err)
		w.logger.Error("Failed to send %s after %d attempt(s): %v", register.Name, w.retries, err)
		return fmt.Errorf("write %s: %w", register.Name, err)
	}
	w.logger.SerialSend(command, time.Since(sendStart), nil)

	// Give the unit a moment to apply the value before reading it back.
	if !sleepCtx(ctx, w.settleDelay) {
		return ctx.Err()
	}

	// Establish what the unit actually holds now, so the state topic carries
	// reality rather than the request.
	actual, resolved := w.readState(ctx, register)

	confirmed, checkable := w.verifyWrite(ctx, register)

	state := valueStr
	if resolved {
		state = actual
	}
	if register.Topic != "" {
		if err := w.pub.Publish(register.Topic, state); err != nil {
			w.logger.Error("Failed to publish state for %s to %s: %v", register.Name, register.Topic, err)
		}
	}

	switch {
	case resolved && actual != valueStr:
		// The command went out but the unit is holding a different value.
		w.logger.Warn("Wrote %s = %d but %s reports %s; the unit did not take the value",
			register.Name, value, register.StateFrom, actual)
		return fmt.Errorf("write %s: unit reports %s, not %d", register.Name, actual, value)
	case resolved:
		w.rememberSuccess(topic, valueStr)
		w.logger.Info("Wrote %s = %d, confirmed by %s", register.Name, value, register.StateFrom)
	case checkable && !confirmed:
		// The command went out, so it may still have taken effect; the next
		// read cycle will show the truth.
		w.logger.Warn("Could not confirm %s = %d by reading back %v",
			register.Name, value, register.VerifyRegisters)
		return fmt.Errorf("write %s: sent but could not confirm it took effect", register.Name)
	case checkable:
		w.rememberSuccess(topic, valueStr)
		w.logger.Info("Wrote %s = %d, confirmed by read-back", register.Name, value)
	default:
		w.rememberSuccess(topic, valueStr)
		w.logger.Warn("Wrote %s = %d; no state_from or verify_registers configured, "+
			"so it cannot be confirmed", register.Name, value)
	}

	w.logger.Info("=== Write of %s completed in %v ===", register.Name, time.Since(received))
	return nil
}

// readState reads the register that carries the write register's real value.
func (w *Writer) readState(ctx context.Context, register config.WriteRegisterConfig) (string, bool) {
	if w.reader == nil || register.StateFrom == "" {
		return "", false
	}

	stateReg, ok := w.reader.Register(register.StateFrom)
	if !ok {
		w.logger.Warn("state_from register %s is not configured for reading", register.StateFrom)
		return "", false
	}

	value, err := w.reader.ReadSingle(ctx, stateReg)
	if err != nil {
		w.logger.Warn("Failed to read %s back for %s: %v", register.StateFrom, register.Name, err)
		return "", false
	}
	return value, true
}

// sendCommand writes the command, retrying only if the write itself failed.
//
// A retry re-applies an absolute value, which is harmless, but it is reserved
// for a genuine write error: the unit does not reliably answer a write, so
// retrying on a missing response would re-send a command that already landed.
func (w *Writer) sendCommand(ctx context.Context, command string) error {
	var lastErr error
	for attempt := 1; attempt <= w.retries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if attempt > 1 && !sleepCtx(ctx, w.backoff) {
			return ctx.Err()
		}

		err := w.dev.Send(ctx, command)
		if err == nil {
			return nil
		}
		lastErr = err
		if isCancellation(err) {
			return err
		}
		w.logger.Warn("Send attempt %d/%d for %q failed: %v", attempt, w.retries, command, err)
	}
	return lastErr
}

// verifyWrite reads back the registers that confirm the write took effect and
// publishes their fresh values.
//
// It reports whether any register could be read (confirmed) and whether there
// was anything to check at all (checkable). A successful read-back is what
// makes a write trustworthy, since the unit does not reliably echo commands.
func (w *Writer) verifyWrite(ctx context.Context, register config.WriteRegisterConfig) (confirmed, checkable bool) {
	if w.reader == nil || len(register.VerifyRegisters) == 0 {
		return false, false
	}

	w.logger.Info("Verifying %s by reading %v", register.Name, register.VerifyRegisters)

	for _, name := range register.VerifyRegisters {
		verifyReg, ok := w.reader.Register(name)
		if !ok {
			w.logger.Warn("Verify register %s is not a configured read register, skipping", name)
			continue
		}

		value, err := w.reader.ReadSingle(ctx, verifyReg)
		if err != nil {
			w.logger.Warn("Failed to verify %s: %v", name, err)
			continue
		}

		valid, checked := w.reader.ValidateValue(value, verifyReg)
		if !valid {
			w.logger.Warn("Verified value %s for %s is out of range, not publishing", checked, name)
			continue
		}
		if verifyReg.Topic == "" {
			continue
		}
		if err := w.pub.Publish(verifyReg.Topic, checked); err != nil {
			w.logger.Error("Failed to publish verified %s: %v", name, err)
			continue
		}
		w.logger.Info("Verified %s = %s", name, checked)
		confirmed = true
	}

	return confirmed, true
}

// lastWrite records the most recent command accepted for a topic.
type lastWrite struct {
	value string
	at    time.Time
}

// isDuplicate reports whether this is a redelivery of the last command
// accepted for the topic, within the dedupe window.
//
// Only the most recent value is compared: keying on every recent (topic,value)
// pair meant a real 3 -> 0 -> 3 sequence inside one second left the device on
// 0, because the final command looked like a duplicate.
func (w *Writer) isDuplicate(topic, value string) bool {
	w.dedupeMu.Lock()
	defer w.dedupeMu.Unlock()

	last, ok := w.lastCommand[topic]
	if !ok || last.value != value {
		return false
	}
	return time.Since(last.at) < w.dedupeWindow
}

// rememberSuccess records a command that reached the device.
func (w *Writer) rememberSuccess(topic, value string) {
	w.dedupeMu.Lock()
	defer w.dedupeMu.Unlock()
	w.lastCommand[topic] = lastWrite{value: value, at: time.Now()}
}
