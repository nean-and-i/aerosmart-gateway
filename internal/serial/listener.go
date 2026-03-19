package serial

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nean/aerosmart-gateway/internal/config"
	"github.com/nean/aerosmart-gateway/internal/logger"
	"github.com/nean/aerosmart-gateway/internal/mqtt"
)

// RegisterValue holds a register value with metadata
type RegisterValue struct {
	Name     string
	Value    string
	RawValue string
	Valid    bool
}

// SerialListener continuously listens on serial port for register responses
type SerialListener struct {
	serial        *SerialPort
	mqtt          *mqtt.Client
	logger        *logger.Logger
	readRegisters []config.ReadRegisterConfig

	// Command to register mapping for quick lookup
	commandMap map[string]config.ReadRegisterConfig

	// In-memory store of register values
	values map[string]*RegisterValue
	mu     sync.RWMutex

	// Control channels
	stopChan chan struct{}
	running  bool
}

// NewSerialListener creates a new serial listener
func NewSerialListener(serialPort *SerialPort, mqttClient *mqtt.Client, log *logger.Logger, readRegisters []config.ReadRegisterConfig) *SerialListener {
	// Build command to register mapping
	commandMap := make(map[string]config.ReadRegisterConfig)
	for _, reg := range readRegisters {
		commandMap[reg.Command] = reg
	}

	return &SerialListener{
		serial:        serialPort,
		mqtt:          mqttClient,
		logger:        log,
		readRegisters: readRegisters,
		commandMap:    commandMap,
		values:        make(map[string]*RegisterValue),
		stopChan:      make(chan struct{}),
		running:       false,
	}
}

// Start starts the continuous serial listener in a goroutine
func (l *SerialListener) Start() error {
	if l.running {
		return fmt.Errorf("listener already running")
	}

	l.running = true
	l.stopChan = make(chan struct{})

	go l.listen()

	l.logger.Info("Serial listener started")
	return nil
}

// Stop stops the serial listener gracefully
func (l *SerialListener) Stop() {
	if !l.running {
		return
	}

	l.logger.Info("Stopping serial listener...")
	l.running = false
	if l.stopChan != nil {
		close(l.stopChan)
	}
	l.logger.Info("Serial listener stopped")
}

// IsRunning returns true if the listener is running
func (l *SerialListener) IsRunning() bool {
	return l.running
}

// GetValues returns a copy of current register values
func (l *SerialListener) GetValues() map[string]*RegisterValue {
	l.mu.RLock()
	defer l.mu.RUnlock()

	result := make(map[string]*RegisterValue)
	for k, v := range l.values {
		result[k] = &RegisterValue{
			Name:     v.Name,
			Value:    v.Value,
			RawValue: v.RawValue,
			Valid:    v.Valid,
		}
	}
	return result
}

// listen is the main loop that continuously reads from serial port
func (l *SerialListener) listen() {
	l.logger.Info("Serial listener: starting continuous read loop")

	for {
		select {
		case <-l.stopChan:
			l.logger.Info("Serial listener: received stop signal")
			return
		default:
			// Try to read from serial port
			l.processSerialRead()
		}
	}
}

// processSerialRead attempts to read and process a single response from serial
func (l *SerialListener) processSerialRead() {
	// Check if serial port is open
	if !l.serial.IsOpen() {
		l.logger.Warn("Serial listener: serial port not open, attempting to reopen...")
		if err := l.serial.Open(); err != nil {
			l.logger.Error("Serial listener: failed to reopen serial port: %v", err)
			time.Sleep(1 * time.Second)
			return
		}
		l.logger.Info("Serial listener: serial port reopened")
	}

	// Try to read a response
	response, err := l.serial.Read()
	if err != nil {
		// Timeout is expected when no data available
		l.logger.Debug("Serial listener: read timeout, no data available")
		return
	}

	if len(response) == 0 {
		return
	}

	l.logger.Debug("Serial listener: received raw response: %q", response)

	// Process the response
	l.handleResponse(response)
}

// handleResponse processes a single serial response
func (l *SerialListener) handleResponse(response string) {
	// Split response into lines (device may send multiple responses)
	lines := strings.Split(response, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		// Try to parse as a register response
		// Expected format: "command value" (e.g., "130 1067 3")
		parts := strings.Fields(line)
		if len(parts) < 3 {
			l.logger.Debug("Serial listener: response too short: %q", line)
			continue
		}

		// Reconstruct command from first two parts
		command := parts[0] + " " + parts[1]

		// Look up the register by command
		reg, ok := l.commandMap[command]
		if !ok {
			l.logger.Debug("Serial listener: unknown command: %s", command)
			continue
		}

		// Extract value (third part)
		value := parts[2]

		// Process the value (apply divisor and type conversion)
		processedValue := l.processValue(value, reg)

		// Validate value
		valid, finalValue := l.validateValue(processedValue, reg)

		// Update in-memory store
		l.mu.Lock()
		l.values[reg.Name] = &RegisterValue{
			Name:     reg.Name,
			Value:    finalValue,
			RawValue: value,
			Valid:    valid,
		}
		l.mu.Unlock()

		l.logger.Info("Serial listener: updated register %s = %s (valid: %v)", reg.Name, finalValue, valid)

		// Publish to MQTT
		if reg.Topic != "" {
			if err := l.mqtt.Publish(reg.Topic, finalValue); err != nil {
				l.logger.Error("Serial listener: failed to publish %s to MQTT: %v", reg.Topic, err)
			} else {
				l.logger.Debug("Serial listener: published %s = %s to %s", reg.Name, finalValue, reg.Topic)
			}
		}
	}
}

// processValue applies divisor and type conversion to the raw value
func (l *SerialListener) processValue(rawValue string, register config.ReadRegisterConfig) string {
	// Convert to float for division
	floatVal, err := strconv.ParseFloat(rawValue, 64)
	if err != nil {
		l.logger.Warn("Serial listener: failed to parse value %s as float: %v", rawValue, err)
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

// validateValue checks if the value is within the valid range
func (l *SerialListener) validateValue(value string, register config.ReadRegisterConfig) (bool, string) {
	floatVal, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return false, value
	}

	// Check range
	if floatVal < float64(register.MinValue) || floatVal > float64(register.MaxValue) {
		l.logger.Warn("Serial listener: value %s out of range for %s (min: %d, max: %d)",
			value, register.Name, register.MinValue, register.MaxValue)

		// Special handling for luefterstatus - clamp to max value
		if register.Name == "luefterstatus" && floatVal > float64(register.MaxValue) {
			return true, fmt.Sprintf("%d", register.MaxValue)
		}

		return false, value
	}

	return true, value
}
