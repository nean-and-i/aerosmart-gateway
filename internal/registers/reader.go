package registers

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nean/aerosmart-gateway/internal/config"
	"github.com/nean/aerosmart-gateway/internal/logger"
	"github.com/nean/aerosmart-gateway/internal/mqtt"
	"github.com/nean/aerosmart-gateway/internal/serial"
)

// RegisterValue holds a register value with metadata
type RegisterValue struct {
	Name     string
	Value    string
	RawValue string
	Valid    bool
}

// Reader handles reading registers from the device
type Reader struct {
	serial    *serial.SerialPort
	mqtt      *mqtt.Client
	logger    *logger.Logger
	registers []config.ReadRegisterConfig
	values    map[string]*RegisterValue
	mu        sync.RWMutex

	// Context for read cancellation (write priority)
	readCtx    context.Context
	readCancel context.CancelFunc
	readCtxMu  sync.Mutex
	isReading  atomic.Bool
}

// NewReader creates a new register reader
func NewReader(serialPort *serial.SerialPort, mqttClient *mqtt.Client, log *logger.Logger, registers []config.ReadRegisterConfig) *Reader {
	ctx, cancel := context.WithCancel(context.Background())
	return &Reader{
		serial:     serialPort,
		mqtt:       mqttClient,
		logger:     log,
		registers:  registers,
		values:     make(map[string]*RegisterValue),
		readCtx:    ctx,
		readCancel: cancel,
	}
}

// ReadSingle reads a single register from the device with verbose logging
func (r *Reader) ReadSingle(register config.ReadRegisterConfig) (string, error) {
	maxRetries := r.serial.GetMaxRetries()

	for i := 0; i < maxRetries; i++ {
		startTime := time.Now()

		// Send command to device
		r.logger.Debug("REGISTER READ attempt %d/%d: register=%s cmd=%q", i+1, maxRetries, register.Name, register.Command)

		response, err := r.serial.SendAndReceive(register.Command, maxRetries)
		duration := time.Since(startTime)

		if err != nil {
			r.logger.SerialOperation("READ", register.Command, "", duration, err)
			r.logger.Warn("Serial read error for %s (attempt %d/%d): %v", register.Name, i+1, maxRetries, err)
			continue
		}

		r.logger.SerialOperation("READ", register.Command, response, duration, nil)

		// Parse response
		value, ok := serial.ParseResponse(response, register.Command)
		if !ok {
			r.logger.Warn("WARNING: out-of-range response for %s (attempt %d/%d): %s", register.Name, i+1, maxRetries, response)
			continue
		}

		// Apply divisor and type conversion
		processedValue := r.processValue(value, register)

		r.logger.RegisterOperation("READ", register.Name, register.Command, processedValue, true, nil)
		return processedValue, nil
	}

	return "", fmt.Errorf("failed to read register %s after %d retries", register.Name, maxRetries)
}

// ReadSingleWithTiming reads a single register and returns timing info
func (r *Reader) ReadSingleWithTiming(register config.ReadRegisterConfig) (string, time.Duration, error) {
	startTime := time.Now()

	value, err := r.ReadSingle(register)
	duration := time.Since(startTime)

	return value, duration, err
}

// ReadFanStatus reads the luefterstatus register specifically (for verification after fan control)
func (r *Reader) ReadFanStatus() (string, error) {
	// Find the luefterstatus register config
	var fanStatusReg config.ReadRegisterConfig
	for _, reg := range r.registers {
		if reg.Name == "luefterstatus" {
			fanStatusReg = reg
			break
		}
	}

	if fanStatusReg.Name == "" {
		return "", fmt.Errorf("luefterstatus register not found in config")
	}

	r.logger.Info("Reading luefterstatus for verification after fan control...")

	value, err := r.ReadSingle(fanStatusReg)
	if err != nil {
		r.logger.Error("Failed to read luefterstatus for verification: %v", err)
		return "", err
	}

	r.logger.Info("Fan status verified: luefterstatus = %s", value)
	return value, nil
}

// processValue applies divisor and type conversion to the raw value
func (r *Reader) processValue(rawValue string, register config.ReadRegisterConfig) string {
	// Convert to float for division
	floatVal, err := strconv.ParseFloat(rawValue, 64)
	if err != nil {
		r.logger.Warn("Failed to parse value %s as float: %v", rawValue, err)
		return rawValue
	}

	// Apply divisor
	if register.Divisor > 1 {
		floatVal = floatVal / float64(register.Divisor)
	}

	// Format based on type
	var result string
	if register.Type == "float" {
		result = fmt.Sprintf("%.1f", math.Round(floatVal*10)/10)
	} else {
		result = fmt.Sprintf("%.0f", math.Round(floatVal))
	}

	return result
}

// ValidateValue checks if the value is within the valid range
func (r *Reader) ValidateValue(value string, register config.ReadRegisterConfig) (bool, string) {
	floatVal, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return false, value
	}

	// Check range
	if floatVal < float64(register.MinValue) || floatVal > float64(register.MaxValue) {
		r.logger.Warn("Value %s out of range for %s (min: %d, max: %d)",
			value, register.Name, register.MinValue, register.MaxValue)

		// Special handling for luefterstatus - clamp to max value
		if register.Name == "luefterstatus" && floatVal > float64(register.MaxValue) {
			return true, fmt.Sprintf("%d", register.MaxValue)
		}

		return false, value
	}

	return true, value
}

// ReadAll reads all configured registers from the device with verbose logging
// If mqttPublishEnabled is false, MQTT publishing is skipped (for sequential read cycles)
func (r *Reader) ReadAll() (map[string]*RegisterValue, error) {
	// Get fresh context for this read cycle
	r.readCtxMu.Lock()
	ctx := r.readCtx
	r.readCtxMu.Unlock()

	return r.ReadAllWithContext(ctx)
}

// ReadAllWithContext reads all registers with context support for cancellation (write priority)
func (r *Reader) ReadAllWithContext(ctx context.Context) (map[string]*RegisterValue, error) {
	results := make(map[string]*RegisterValue)
	cycleStartTime := time.Now()

	r.isReading.Store(true)
	defer r.isReading.Store(false)

	r.logger.Info("=== SERIAL: Starting register read cycle ===")

	for _, register := range r.registers {
		// Check for cancellation before each register read
		select {
		case <-ctx.Done():
			r.logger.Info("=== SERIAL: Register read cycle cancelled (write priority) ===")
			return results, nil
		default:
		}

		value, err := r.ReadSingle(register)
		if err != nil {
			r.logger.Warn("Failed to read register %s: %v", register.Name, err)
			results[register.Name] = &RegisterValue{
				Name:     register.Name,
				Value:    "",
				RawValue: "",
				Valid:    false,
			}
			continue
		}

		// Validate value
		valid, processedValue := r.ValidateValue(value, register)

		results[register.Name] = &RegisterValue{
			Name:     register.Name,
			Value:    processedValue,
			RawValue: value,
			Valid:    valid,
		}

		r.logger.Debug("Register %s = %s (valid: %v)", register.Name, processedValue, valid)
	}

	cycleDuration := time.Since(cycleStartTime)
	r.logger.Info("=== SERIAL: Register read cycle completed in %v ===", cycleDuration)

	// Store values
	r.mu.Lock()
	r.values = results
	r.mu.Unlock()

	return results, nil
}

// GetValues returns the current register values
func (r *Reader) GetValues() map[string]*RegisterValue {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string]*RegisterValue)
	for k, v := range r.values {
		result[k] = v
	}
	return result
}

// Cancel cancels any ongoing read operation (used for write priority)
func (r *Reader) Cancel() {
	r.readCtxMu.Lock()
	defer r.readCtxMu.Unlock()
	if r.readCancel != nil {
		r.readCancel()
	}
}

// ResetContext creates a new context for the next read cycle
func (r *Reader) ResetContext() {
	r.readCtxMu.Lock()
	defer r.readCtxMu.Unlock()
	if r.readCancel != nil {
		r.readCancel()
	}
	r.readCtx, r.readCancel = context.WithCancel(context.Background())
}

// IsReading returns true if a read operation is currently in progress
func (r *Reader) IsReading() bool {
	return r.isReading.Load()
}

// PublishAll publishes all register values to MQTT with verbose logging
func (r *Reader) PublishAll(values map[string]*RegisterValue) error {
	for name, regVal := range values {
		if !regVal.Valid {
			continue
		}

		// Find the topic for this register
		var topic string
		for _, reg := range r.registers {
			if reg.Name == name {
				topic = reg.Topic
				break
			}
		}

		if topic != "" {
			err := r.mqtt.Publish(topic, regVal.Value)

			if err != nil {
				r.logger.MQTTOperation("PUBLISH", topic, regVal.Value, 0, false, err)
				r.logger.Error("Failed to publish %s to MQTT: %v", topic, err)
			} else {
				r.logger.MQTTOperation("PUBLISH", topic, regVal.Value, 0, false, nil)
				r.logger.Debug("Published %s = %s to %s", name, regVal.Value, topic)
			}
		}
	}

	return nil
}

// MessageInfo holds information about a processed MQTT message for deduplication
type MessageInfo struct {
	Topic     string
	Value     string
	Timestamp time.Time
}

// Writer handles writing registers to the device via MQTT
type Writer struct {
	serial    *serial.SerialPort
	mqtt      *mqtt.Client
	logger    *logger.Logger
	registers []config.WriteRegisterConfig
	reader    *Reader            // Reference to reader for verification
	derived   *DerivedCalculator // Optional: calculates derived registers after a full readout

	// Write priority mechanism - auto-expires after writePriorityTimeout
	writePriorityTime    time.Time
	writePriorityTimeout time.Duration
	writePriorityMu      sync.Mutex

	// Message deduplication - track recently processed messages
	recentMessages      map[string]MessageInfo // topic+value -> info
	recentMessagesMu    sync.Mutex
	messageDedupeWindow time.Duration // Time window for deduplication (default 1 second)

	// Message processing metrics
	lastMessageReceivedTime time.Time     // Timestamp when last MQTT message was received
	lastMessageProcessTime  time.Time     // Timestamp when last message processing started
	lastMessageCompleteTime time.Time     // Timestamp when last message processing completed
	lastMessageLatency      time.Duration // Latency from receive to complete
	metricsMu               sync.Mutex
}

// NewWriter creates a new register writer
func NewWriter(serialPort *serial.SerialPort, mqttClient *mqtt.Client, log *logger.Logger, registers []config.WriteRegisterConfig) *Writer {
	return &Writer{
		serial:               serialPort,
		mqtt:                 mqttClient,
		logger:               log,
		registers:            registers,
		writePriorityTimeout: 10 * time.Second,
		recentMessages:       make(map[string]MessageInfo),
		messageDedupeWindow:  1 * time.Second,
	}
}

// SetReader sets the reader for verification after write operations
func (w *Writer) SetReader(reader *Reader) {
	w.reader = reader
}

// SetDerivedCalculator sets the calculator used to compute and publish derived
// register values after each full readout. Optional; if unset, no derived values
// are calculated.
func (w *Writer) SetDerivedCalculator(derived *DerivedCalculator) {
	w.derived = derived
}

// HandleMessage handles an incoming MQTT message for write registers
// Includes immediate verification by reading luefterstatus after fan control
// Uses SendAndReceive with proper retry logic for reliable communication
// Signals write priority to preempt any ongoing read operations
func (w *Writer) HandleMessage(topic string, message string) error {
	// Record message receive time for metrics
	receiveTime := time.Now()
	w.updateLastMessageReceivedTime(receiveTime)

	// Message deduplication - check if we've recently processed this exact message
	if w.isDuplicateMessage(topic, message) {
		w.logger.Info("MQTT: Skipping duplicate message on %s: %s (within %v)", topic, message, w.messageDedupeWindow)
		return nil
	}

	// Record message processing start time
	w.updateLastMessageProcessStartTime()

	// Signal write priority to preempt any ongoing read operations
	w.SignalWritePriority()

	// Cancel any ongoing read operation immediately
	if w.reader != nil {
		w.reader.Cancel()
		w.reader.ResetContext()
	}

	// Find the register that matches this topic
	var targetRegister *config.WriteRegisterConfig
	for i := range w.registers {
		if w.registers[i].SubscribeTopic == topic {
			targetRegister = &w.registers[i]
			break
		}
	}

	if targetRegister == nil {
		w.logger.Warn("No register found for topic %s", topic)
		return nil
	}

	// Parse the message value
	valueStr := strings.TrimSpace(message)

	// Convert to int for validation
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		w.logger.Error("Invalid value for %s: %s", targetRegister.Name, message)
		return fmt.Errorf("invalid value: %s", message)
	}

	// Validate range
	if value < targetRegister.MinValue || value > targetRegister.MaxValue {
		w.logger.Error("Value %d out of range for %s (min: %d, max: %d)",
			value, targetRegister.Name, targetRegister.MinValue, targetRegister.MaxValue)
		return fmt.Errorf("value out of range")
	}

	// Build command
	command := mqtt.ParseWriteRegisterCommand(targetRegister.CommandTemplate, valueStr)

	w.logger.Info("=== Writing to device ===")
	w.logger.Info("Command: %s", command)

	// Open serial if not open
	if !w.serial.IsOpen() {
		if err := w.serial.Open(); err != nil {
			w.logger.Error("Failed to open serial: %v", err)
			return err
		}
		w.logger.SerialConnection("OPENED", w.serial.GetPortName(), w.serial.GetBaudRate(), nil)
	}

	// Use SendAndReceive with retry logic for reliable write + read cycle
	maxRetries := w.serial.GetMaxRetries()
	response, err := w.serial.SendAndReceive(command, maxRetries)
	if err != nil {
		w.logger.SerialOperation("WRITE", command, "", 0, err)
		w.logger.Error("Failed to write to serial after %d attempts: %v", maxRetries, err)
		// Don't return error - log and skip, let next scheduled read or control command handle it
		w.logger.Warn("Write operation failed, will retry on next scheduled read cycle")
		return nil
	}

	w.logger.SerialOperation("WRITE", command, response, 0, nil)
	w.logger.Info("Successfully wrote %s = %d to device (response: %s)", targetRegister.Name, value, response)

	// Flush the serial buffer after write to prevent stale data from affecting subsequent reads
	w.serial.FlushBuffer()

	// IMMEDIATE VERIFICATION: Read all verify registers after write
	if w.reader != nil && len(targetRegister.VerifyRegisters) > 0 {
		// Wait for device response delay (best practice)
		delayMs := w.serial.GetDeviceResponseDelay()
		if delayMs > 0 {
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
		}

		w.logger.Info("Verifying write by reading verify registers: %v", targetRegister.VerifyRegisters)

		for _, verifyRegName := range targetRegister.VerifyRegisters {
			// Find the corresponding read register config
			var verifyReg config.ReadRegisterConfig
			found := false
			for _, reg := range w.reader.registers {
				if reg.Name == verifyRegName {
					verifyReg = reg
					found = true
					break
				}
			}

			if !found {
				w.logger.Warn("Verify register %s not found in read registers, skipping", verifyRegName)
				continue
			}

			// Read the verified value
			verifiedValue, err := w.reader.ReadSingle(verifyReg)
			if err != nil {
				w.logger.Warn("Failed to verify %s: %v", verifyRegName, err)
				continue
			}

			// Publish the verified value to the read register's topic
			if err := w.mqtt.Publish(verifyReg.Topic, verifiedValue); err != nil {
				w.logger.Error("Failed to publish verified value for %s: %v", verifyRegName, err)
			} else {
				w.logger.Info("Verified and published %s = %s to %s", verifyRegName, verifiedValue, verifyReg.Topic)
			}
		}
	}

	w.logger.Info("=== Write operation completed ===")

	// Record message processing completion time and calculate latency
	w.updateLastMessageCompleteTime()

	// Clean up old deduplication entries periodically
	w.cleanOldMessages()

	// Clear write priority flag to allow reads to resume
	w.ClearWritePriority()

	return nil
}

// GetSubscribeTopics returns all subscribe topics for write registers
func (w *Writer) GetSubscribeTopics() []string {
	topics := make([]string, 0, len(w.registers))
	for _, reg := range w.registers {
		if reg.SubscribeTopic != "" {
			topics = append(topics, reg.SubscribeTopic)
		}
	}
	return topics
}

// TriggerFullReadout triggers a full register readout and publishes to MQTT
// This is called periodically (every 60 seconds) and after write operations
// It checks for write priority and will skip the read if a write is pending
func (w *Writer) TriggerFullReadout() error {
	if w.reader == nil {
		w.logger.Warn("Writer has no reader reference, cannot trigger full readout")
		return fmt.Errorf("no reader reference")
	}

	// Check for write priority - if a write is pending, skip this periodic read
	if w.isWritePriorityActive() {
		w.logger.Info("=== Write priority detected, skipping periodic read ===")
		w.reader.Cancel()
		w.reader.ResetContext()
		return nil
	}

	w.logger.Info("=== Triggering full register readout ===")

	// Read all registers
	values, err := w.reader.ReadAll()
	if err != nil {
		w.logger.Error("Failed to read registers during full readout: %v", err)
		return err
	}

	// Publish all values to MQTT
	if err := w.reader.PublishAll(values); err != nil {
		w.logger.Error("Failed to publish register values: %v", err)
	}

	// Calculate and publish derived register values from the freshly read values.
	// Derived values run only on cycles where a read actually happened (skipped
	// under write priority), keeping them consistent with their source registers.
	if w.derived != nil {
		derivedValues, err := w.derived.Calculate(values)
		if err != nil {
			w.logger.Error("Failed to calculate derived registers: %v", err)
		}
		if err := w.derived.PublishAll(derivedValues); err != nil {
			w.logger.Error("Failed to publish derived register values: %v", err)
		}
	}

	w.logger.Info("=== Full register readout completed ===")
	return nil
}

// SignalWritePriority signals that a write operation should take priority.
// This will cause the next TriggerFullReadout to skip the read cycle.
// The priority auto-expires after 10 seconds to prevent permanent blocking.
func (w *Writer) SignalWritePriority() {
	w.writePriorityMu.Lock()
	w.writePriorityTime = time.Now()
	w.writePriorityMu.Unlock()
	w.logger.Debug("Write priority signaled")
}

// ClearWritePriority clears the write priority flag, allowing reads to resume.
func (w *Writer) ClearWritePriority() {
	w.writePriorityMu.Lock()
	w.writePriorityTime = time.Time{}
	w.writePriorityMu.Unlock()
	w.logger.Debug("Write priority cleared")
}

// isWritePriorityActive returns true if write priority is active and has not expired.
func (w *Writer) isWritePriorityActive() bool {
	w.writePriorityMu.Lock()
	defer w.writePriorityMu.Unlock()
	if w.writePriorityTime.IsZero() {
		return false
	}
	if time.Since(w.writePriorityTime) >= w.writePriorityTimeout {
		// Expired - clear it and log
		w.writePriorityTime = time.Time{}
		w.logger.Warn("Write priority expired after %v timeout", w.writePriorityTimeout)
		return false
	}
	return true
}

// isDuplicateMessage checks if this exact message was recently processed
func (w *Writer) isDuplicateMessage(topic string, value string) bool {
	key := topic + ":" + value

	w.recentMessagesMu.Lock()
	defer w.recentMessagesMu.Unlock()

	if info, exists := w.recentMessages[key]; exists {
		// Check if within deduplication window
		if time.Since(info.Timestamp) < w.messageDedupeWindow {
			return true
		}
		// Clean up old entries
		delete(w.recentMessages, key)
	}

	// Store this message
	w.recentMessages[key] = MessageInfo{
		Topic:     topic,
		Value:     value,
		Timestamp: time.Now(),
	}

	return false
}

// cleanOldMessages removes old entries from the deduplication map
func (w *Writer) cleanOldMessages() {
	w.recentMessagesMu.Lock()
	defer w.recentMessagesMu.Unlock()

	cutoff := time.Now().Add(-w.messageDedupeWindow)
	for key, info := range w.recentMessages {
		if info.Timestamp.Before(cutoff) {
			delete(w.recentMessages, key)
		}
	}
}

// updateLastMessageReceivedTime records when an MQTT message was received
func (w *Writer) updateLastMessageReceivedTime(t time.Time) {
	w.metricsMu.Lock()
	defer w.metricsMu.Unlock()
	w.lastMessageReceivedTime = t
}

// updateLastMessageProcessStartTime records when message processing started
func (w *Writer) updateLastMessageProcessStartTime() {
	w.metricsMu.Lock()
	defer w.metricsMu.Unlock()
	w.lastMessageProcessTime = time.Now()
}

// updateLastMessageCompleteTime records when message processing completed and calculates latency
func (w *Writer) updateLastMessageCompleteTime() {
	w.metricsMu.Lock()
	defer w.metricsMu.Unlock()
	w.lastMessageCompleteTime = time.Now()

	// Calculate latency from receive to complete
	if !w.lastMessageReceivedTime.IsZero() && !w.lastMessageProcessTime.IsZero() {
		w.lastMessageLatency = w.lastMessageCompleteTime.Sub(w.lastMessageReceivedTime)
		w.logger.Info("MQTT: Message processing latency: %v (receive -> process -> complete)", w.lastMessageLatency)
	}
}

// GetMessageMetrics returns current message processing metrics
func (w *Writer) GetMessageMetrics() (receiveTime, processTime, completeTime time.Time, latency time.Duration) {
	w.metricsMu.Lock()
	defer w.metricsMu.Unlock()
	return w.lastMessageReceivedTime, w.lastMessageProcessTime, w.lastMessageCompleteTime, w.lastMessageLatency
}
