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

	// dedupe remembers commands that actually reached the device, so a
	// broker redelivery is dropped but a retry after a failure is not.
	dedupeMu     sync.Mutex
	dedupe       map[string]time.Time
	dedupeWindow time.Duration
}

// NewWriter creates a register writer. retries counts total attempts per
// command; the gate is used to preempt in-flight read cycles.
func NewWriter(dev Transactor, pub Publisher, log *logger.Logger, registers []config.WriteRegisterConfig, gate *ReadGate, retries int, backoff time.Duration) *Writer {
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
		dedupe:       make(map[string]time.Time),
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

	response, err := w.sendCommand(ctx, command)
	if err != nil {
		w.logger.SerialOperation("WRITE", command, "", 0, err)
		w.logger.Error("Failed to write %s after %d attempt(s): %v", register.Name, w.retries, err)
		return fmt.Errorf("write %s: %w", register.Name, err)
	}

	w.logger.SerialOperation("WRITE", command, response, 0, nil)
	w.logger.Info("Wrote %s = %d (device echoed %q)", register.Name, value, response)
	w.rememberSuccess(topic, valueStr)

	// Reflect the accepted command on the register's own state topic, so the
	// Home Assistant entity does not sit in an unknown state.
	if register.Topic != "" {
		if err := w.pub.Publish(register.Topic, valueStr); err != nil {
			w.logger.Error("Failed to publish state for %s to %s: %v", register.Name, register.Topic, err)
		}
	}

	w.verifyWrite(ctx, register)

	w.logger.Info("=== Write of %s completed in %v ===", register.Name, time.Since(received))
	return nil
}

// sendCommand writes the command, retrying a bounded number of times.
//
// Retrying re-sends the command, which is safe here because every write
// register sets an absolute value rather than applying a delta.
func (w *Writer) sendCommand(ctx context.Context, command string) (string, error) {
	var lastErr error
	for attempt := 1; attempt <= w.retries; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if attempt > 1 && !sleepCtx(ctx, w.backoff) {
			return "", ctx.Err()
		}

		response, err := w.dev.Transact(ctx, command)
		if err == nil {
			return response, nil
		}
		lastErr = err
		if isCancellation(err) {
			return "", err
		}
		w.logger.Warn("Write attempt %d/%d for %q failed: %v", attempt, w.retries, command, err)
	}
	return "", lastErr
}

// verifyWrite reads back the registers that confirm the write took effect and
// publishes their fresh values.
func (w *Writer) verifyWrite(ctx context.Context, register config.WriteRegisterConfig) {
	if w.reader == nil || len(register.VerifyRegisters) == 0 {
		return
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
	}
}

// isDuplicate reports whether an identical command recently succeeded.
func (w *Writer) isDuplicate(topic, value string) bool {
	key := topic + "=" + value

	w.dedupeMu.Lock()
	defer w.dedupeMu.Unlock()

	w.pruneLocked()
	_, exists := w.dedupe[key]
	return exists
}

// rememberSuccess records a command that reached the device.
func (w *Writer) rememberSuccess(topic, value string) {
	w.dedupeMu.Lock()
	defer w.dedupeMu.Unlock()
	w.pruneLocked()
	w.dedupe[topic+"="+value] = time.Now()
}

func (w *Writer) pruneLocked() {
	cutoff := time.Now().Add(-w.dedupeWindow)
	for key, at := range w.dedupe {
		if at.Before(cutoff) {
			delete(w.dedupe, key)
		}
	}
}
