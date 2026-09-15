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

	// resettle re-reads the read-back registers again later, for values that
	// trail the command.
	resettle *resettler

	// writeSeq counts accepted commands per register. A settle re-read carries
	// the sequence it was scheduled at and publishes nothing if a newer
	// command has since run, which would otherwise leave Home Assistant
	// showing the superseded value.
	seqMu    sync.Mutex
	writeSeq map[string]uint64

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
		writeSeq:     make(map[string]uint64),
		lastCommand:  make(map[string]lastWrite),
		dedupeWindow: time.Second,
	}
}

// SetReader supplies the reader used to verify writes.
func (w *Writer) SetReader(reader *Reader) { w.reader = reader }

// SetResettleDelay enables a second read-back this long after a write, for
// values that trail the command. Zero disables it. The returned function runs
// the re-reads and must be called in its own goroutine.
func (w *Writer) SetResettleDelay(delay time.Duration) func(context.Context) {
	w.resettle = newResettler(delay, w.refreshIfCurrent, w.logger)
	return w.resettle.run
}

// WriteSequence returns how many commands have been accepted for a register.
func (w *Writer) WriteSequence(name string) uint64 {
	w.seqMu.Lock()
	defer w.seqMu.Unlock()
	return w.writeSeq[name]
}

// nextWriteSequence records another accepted command and returns the new count.
func (w *Writer) nextWriteSequence(name string) uint64 {
	w.seqMu.Lock()
	defer w.seqMu.Unlock()
	w.writeSeq[name]++
	return w.writeSeq[name]
}

// refreshIfCurrent re-reads the register's read-back registers and publishes
// them, unless a newer command for that register has run in the meantime.
//
// readBack releases the wire between reads - to publish, and during a retry
// backoff - so a whole command can complete inside a refresh. Publishing the
// values read before it would put the superseded mode back on the state topic,
// where nothing would correct it until the next write.
func (w *Writer) refreshIfCurrent(ctx context.Context, register config.WriteRegisterConfig, seq uint64) {
	if w.WriteSequence(register.Name) != seq {
		w.logger.Debug("Skipping settle re-read of %s: a newer command has run", register.Name)
		return
	}

	// The re-read puts transactions on the wire and publishes the values it
	// gets back. A poll cycle running alongside it samples the register
	// before it settles and publishes that afterwards, so the state topic
	// would carry the stale value again until the next cycle.
	w.gate.Preempt()
	defer w.gate.Release()

	values, _, _ := w.readBackIfCurrent(ctx, register, seq)
	if values == nil {
		return
	}

	if register.StateFrom == "" || register.Topic == "" {
		return
	}
	actual, ok := values[register.StateFrom]
	if !ok {
		return
	}
	if w.WriteSequence(register.Name) != seq {
		return
	}
	if err := w.pub.Publish(register.Topic, actual); err != nil {
		w.logger.Error("Failed to publish settled state for %s: %v", register.Name, err)
	}
}

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

	// Count the command before it goes out, so a settle re-read scheduled by
	// an earlier command knows it has been superseded.
	seq := w.nextWriteSequence(register.Name)

	sendStart := time.Now()
	if err := w.sendCommand(ctx, command); err != nil {
		w.logger.SerialSend(command, time.Since(sendStart), err)
		w.logger.Error("Failed to send %s after %d attempt(s): %v", register.Name, w.retries, err)
		// The sequence bump above already superseded any re-read the previous
		// command scheduled, so returning here would drop it with nothing in
		// its place and leave a lagging value unpublished until the next full
		// cycle. A partial write may also have reached the unit, so reading
		// the register again is the only way to learn what it is holding.
		if w.resettle != nil {
			w.resettle.schedule(register, seq)
		}
		return fmt.Errorf("write %s: %w", register.Name, err)
	}
	w.logger.SerialSend(command, time.Since(sendStart), nil)

	// The command reached the wire, and this unit applies writes whether or
	// not it confirms them. Remembering it here rather than only on success
	// keeps deduplication comparing against what was actually sent; comparing
	// against an older value made a user's correction back to it look like a
	// redelivery and dropped it.
	w.rememberSent(topic, valueStr)

	// Give the unit a moment to apply the value before reading it back.
	if !sleepCtx(ctx, w.settleDelay) {
		return ctx.Err()
	}

	// One read per register: state_from is often also a verify register, and
	// reading it twice puts an avoidable transaction on the wire per write.
	values, confirmed, checkable := w.readBack(ctx, register)

	actual, resolved := "", false
	if register.StateFrom != "" {
		actual, resolved = values[register.StateFrom]
	}

	state := valueStr
	if resolved {
		state = actual
	}
	if register.Topic != "" {
		if err := w.pub.Publish(register.Topic, state); err != nil {
			w.logger.Error("Failed to publish state for %s to %s: %v", register.Name, register.Topic, err)
		}
	}

	// actual has gone through the read pipeline's divisor and type
	// formatting (e.g. "22.0" for a float register), while valueStr is the
	// untouched MQTT payload (e.g. "22", or "022"). Comparing them directly
	// only worked by coincidence, for every state_from register that happens
	// to be a divisor-1 integer. expected renders the commanded value through
	// the same type formatting state_from's reading went through, so the two
	// are compared in the same representation.
	expected := valueStr
	if resolved && w.reader != nil {
		if readReg, ok := w.reader.Register(register.StateFrom); ok {
			expected = formatByType(float64(value), readReg.Type)
		}
	}

	switch {
	case resolved && actual != expected:
		// The command went out but the unit is holding a different value.
		w.logger.Warn("Wrote %s = %d but %s reports %s; the unit did not take the value",
			register.Name, value, register.StateFrom, actual)
		return fmt.Errorf("write %s: unit reports %s, not %d", register.Name, actual, value)
	case resolved:
		w.logger.Info("Wrote %s = %d, confirmed by %s", register.Name, value, register.StateFrom)
	case checkable && !confirmed:
		// The command went out, so it may still have taken effect; the next
		// read cycle will show the truth.
		w.logger.Warn("Could not confirm %s = %d by reading back %v",
			register.Name, value, register.VerifyRegisters)
		return fmt.Errorf("write %s: sent but could not confirm it took effect", register.Name)
	case checkable:
		w.logger.Info("Wrote %s = %d, confirmed by read-back", register.Name, value)
	default:
		w.logger.Warn("Wrote %s = %d; no state_from or verify_registers configured, "+
			"so it cannot be confirmed", register.Name, value)
	}

	// Some values trail the command; read them again once things settle.
	if w.resettle != nil {
		w.resettle.schedule(register, seq)
	}

	w.logger.Info("=== Write of %s completed in %v ===", register.Name, time.Since(received))
	return nil
}

// readBack reads every register needed to confirm this write - state_from plus
// verify_registers - once each, and publishes each to its own topic.
//
// confirmed reports whether at least one register could be read; checkable
// whether there was anything to read at all. A successful read-back is what
// makes a write trustworthy, since the unit does not reliably echo commands.
func (w *Writer) readBack(ctx context.Context, register config.WriteRegisterConfig) (map[string]string, bool, bool) {
	return w.readBackIfCurrent(ctx, register, 0)
}

// readBackIfCurrent is readBack, abandoning the work if seq is non-zero and a
// newer command for the register has run.
func (w *Writer) readBackIfCurrent(ctx context.Context, register config.WriteRegisterConfig, seq uint64) (map[string]string, bool, bool) {
	if w.reader == nil {
		return nil, false, false
	}

	// state_from first, so its value is the one reported if it also appears in
	// verify_registers.
	names := make([]string, 0, len(register.VerifyRegisters)+1)
	if register.StateFrom != "" {
		names = append(names, register.StateFrom)
	}
	for _, name := range register.VerifyRegisters {
		names = append(names, name)
	}
	if len(names) == 0 {
		return nil, false, false
	}

	w.logger.Info("Reading back %s: %v", register.Name, names)

	values := make(map[string]string, len(names))
	confirmed := false

	for _, name := range names {
		if _, done := values[name]; done {
			continue
		}
		if seq != 0 && w.WriteSequence(register.Name) != seq {
			w.logger.Debug("Abandoning settle re-read of %s: a newer command has run", register.Name)
			return nil, false, false
		}

		readReg, ok := w.reader.Register(name)
		if !ok {
			w.logger.Warn("Read-back register %s is not a configured read register, skipping", name)
			continue
		}

		value, err := w.reader.ReadSingle(ctx, readReg)
		if err != nil {
			w.logger.Warn("Failed to read %s back: %v", name, err)
			continue
		}

		valid, checked := w.reader.ValidateValue(value, readReg)
		if !valid {
			w.logger.Warn("Read-back value %s for %s is out of range, not publishing", checked, name)
			continue
		}

		values[name] = checked
		confirmed = true

		if readReg.Topic == "" {
			continue
		}
		if err := w.pub.Publish(readReg.Topic, checked); err != nil {
			w.logger.Error("Failed to publish %s to %s: %v", name, readReg.Topic, err)
			continue
		}
		w.logger.Info("Published %s = %s to %s", name, checked, readReg.Topic)
	}

	return values, confirmed, true
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

// rememberSent records a command that reached the wire.
func (w *Writer) rememberSent(topic, value string) {
	w.dedupeMu.Lock()
	defer w.dedupeMu.Unlock()
	w.lastCommand[topic] = lastWrite{value: value, at: time.Now()}
}
