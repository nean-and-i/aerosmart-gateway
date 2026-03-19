package serial

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tarm/serial"
)

// SerialPort represents a serial connection to the device
type SerialPort struct {
	port                *serial.Port
	config              *serial.Config
	mu                  sync.Mutex
	writeMu             sync.Mutex  // Dedicated mutex for write operations (ensures write priority)
	readInProgress      atomic.Bool // Flag to track if a read is in progress
	open                bool
	portName            string
	baudRate            int
	reopenCount         int
	maxReopens          int
	readTimeout         time.Duration
	deadlineTimeout     time.Duration
	writeWithRetryDelay time.Duration
	deviceResponseDelay time.Duration
	readWithRetryDelay  time.Duration
	readMaxRetries      int
	writeMaxRetries     int
	maxRetries          int
}

// NewSerialPort creates a new serial port instance
func NewSerialPort(port string, baudrate int, timeout int, writeTimeout int, xonxoff bool, readTimeoutSec int, maxReopens int, deadlineTimeoutMs int, writeWithRetryDelayMs int, deviceResponseDelayMs int, readWithRetryDelayMs int, readMaxRetries int, writeMaxRetries int, maxRetries int) *SerialPort {
	// Set default timeout to 200ms if not specified (prevents blocking forever)
	// Use milliseconds for faster response
	readTimeout := time.Duration(readTimeoutSec) * time.Millisecond
	if readTimeout == 0 {
		readTimeout = 200 * time.Millisecond // Default from config
	}

	return &SerialPort{
		portName:            port,
		baudRate:            baudrate,
		readTimeout:         readTimeout,
		maxReopens:          maxReopens,
		deadlineTimeout:     time.Duration(deadlineTimeoutMs) * time.Millisecond,
		writeWithRetryDelay: time.Duration(writeWithRetryDelayMs) * time.Millisecond,
		deviceResponseDelay: time.Duration(deviceResponseDelayMs) * time.Millisecond,
		readWithRetryDelay:  time.Duration(readWithRetryDelayMs) * time.Millisecond,
		readMaxRetries:      readMaxRetries,
		writeMaxRetries:     writeMaxRetries,
		maxRetries:          maxRetries,
		config: &serial.Config{
			Name:        port,
			Baud:        baudrate,
			ReadTimeout: readTimeout,
			Parity:      serial.ParityNone,
			StopBits:    serial.Stop1,
			Size:        8,
		},
		open:        false,
		reopenCount: 0,
	}
}

// Open opens the serial port connection
func (s *SerialPort) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.open {
		return nil
	}

	port, err := serial.OpenPort(s.config)
	if err != nil {
		return fmt.Errorf("failed to open serial port: %w", err)
	}

	s.port = port
	s.open = true
	s.reopenCount = 0
	return nil
}

// Close closes the serial port connection
func (s *SerialPort) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.open {
		return nil
	}

	if s.port != nil {
		err := s.port.Close()
		s.port = nil
		s.open = false
		return err
	}

	s.open = false
	return nil
}

// IsOpen returns true if the serial port is open
func (s *SerialPort) IsOpen() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.open
}

// GetReopenCount returns the number of times the port has been reopened
func (s *SerialPort) GetReopenCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reopenCount
}

// ResetReopenCount resets the reopen counter
func (s *SerialPort) ResetReopenCount() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reopenCount = 0
}

// GetPortName returns the serial port name
func (s *SerialPort) GetPortName() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.portName
}

// GetBaudRate returns the baud rate
func (s *SerialPort) GetBaudRate() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.baudRate
}

// GetMaxRetries returns the max retries for register operations
func (s *SerialPort) GetMaxRetries() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxRetries
}

// GetDeviceResponseDelay returns the device response delay in milliseconds
func (s *SerialPort) GetDeviceResponseDelay() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int(s.deviceResponseDelay.Milliseconds())
}

// FlushBuffer flushes the serial input buffer multiple times to clear stale data
// This is useful after write operations to ensure clean state for subsequent reads
func (s *SerialPort) FlushBuffer() {
	s.FlushInputMultiple(5)
}

// WaitForReadComplete waits for any ongoing read operation to complete
// Returns immediately if no read is in progress
// This is used by write operations to ensure exclusive access
func (s *SerialPort) WaitForReadComplete() {
	// Check if a read is in progress
	for s.readInProgress.Load() {
		// Wait a short time before checking again
		time.Sleep(10 * time.Millisecond)
	}
}

// Reopen closes and reopens the serial port (hybrid approach - step 2)
func (s *SerialPort) Reopen() error {
	s.mu.Lock()

	// Close existing port
	if s.port != nil {
		s.port.Close()
		s.port = nil
	}
	s.open = false

	// Increment counter BEFORE attempting reopen
	s.reopenCount++
	currentReopenCount := s.reopenCount

	s.mu.Unlock()

	// Small delay before reopening
	time.Sleep(2 * time.Millisecond)

	s.mu.Lock()
	port, err := serial.OpenPort(s.config)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to reopen serial port (attempt %d/%d): %w", currentReopenCount, s.maxReopens, err)
	}

	s.port = port
	s.open = true
	s.mu.Unlock()

	return nil
}

// FlushInput flushes the input buffer by reading all available data
func (s *SerialPort) FlushInput() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.open || s.port == nil {
		return fmt.Errorf("serial port not open")
	}

	// Set a short timeout for flushing (configurable)
	deadline := time.Now().Add(s.deadlineTimeout)

	// Use reflection or type assertion to set deadline if possible
	// Since SetReadDeadline may not be available, we'll read with timeout
	buf := make([]byte, 1024)
	flushed := 0
	for {
		// Check if we've exceeded deadline
		if time.Now().After(deadline) {
			break
		}

		// Try to read - this is a best-effort flush
		// The tarm/serial library doesn't support SetReadDeadline,
		// so we rely on the ReadTimeout in the config
		n, err := s.port.Read(buf)
		if err != nil || n == 0 {
			break
		}
		flushed += n
	}
	return nil
}

// FlushInputMultiple times to ensure buffer is clear
func (s *SerialPort) FlushInputMultiple(times int) {
	for i := 0; i < times; i++ {
		_ = s.FlushInput()
		time.Sleep(10 * time.Millisecond)
	}
}

// FlushOutput flushes the output buffer
func (s *SerialPort) FlushOutput() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.open || s.port == nil {
		return fmt.Errorf("serial port not open")
	}

	// In Go serial library, we don't have direct flush
	return nil
}

// Write writes data to the serial port
func (s *SerialPort) Write(data string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.open || s.port == nil {
		return fmt.Errorf("serial port not open")
	}

	_, err := s.port.Write([]byte(data))
	return err
}

// WriteCommand writes a command with CRLF terminator
func (s *SerialPort) WriteCommand(command string) error {
	return s.Write(command + "\r\n")
}

// Read reads a line from the serial port with proper timeout handling
func (s *SerialPort) Read() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.open || s.port == nil {
		return "", fmt.Errorf("serial port not open")
	}

	// Mark that a read is in progress
	s.readInProgress.Store(true)
	defer s.readInProgress.Store(false)

	// Use larger buffer and read with timeout
	buf := make([]byte, 256)
	var result []byte

	// Set deadline for the entire read operation
	deadline := time.Now().Add(s.readTimeout)

	for {
		// Check if we've exceeded deadline
		if time.Now().After(deadline) {
			if len(result) > 0 {
				return string(result), fmt.Errorf("read timeout after %d bytes", len(result))
			}
			return "", fmt.Errorf("read timeout")
		}

		// Set read deadline for each read operation
		// Note: tarm/serial doesn't support SetReadDeadline,
		// so we rely on the config ReadTimeout
		n, err := s.port.Read(buf)
		if err != nil {
			// Check for timeout error
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				if len(result) > 0 {
					return string(result), nil // Return partial data on timeout
				}
				return "", fmt.Errorf("read timeout")
			}
			return string(result), err
		}
		if n > 0 {
			result = append(result, buf[:n]...)
			// Check for newline in the accumulated result
			for i := len(result) - 1; i >= 0; i-- {
				if result[i] == '\n' {
					// Remove trailing CR if present
					if i > 0 && result[i-1] == '\r' {
						result = result[:i-1]
					} else {
						result = result[:i]
					}
					return string(result), nil
				}
			}
		}

		// No data available - add small delay to prevent CPU spinning
		// This helps with non-blocking reads when device hasn't responded yet
		// Reduced from 2ms to 1ms for faster response to write priority
		time.Sleep(1 * time.Millisecond)

		// Check if we've exceeded a reasonable buffer size
		if len(result) > 256 {
			break
		}
	}

	return string(result), nil
}

// ReadWithRetry reads from serial with retry logic
func (s *SerialPort) ReadWithRetry(maxRetries int, delay time.Duration) (string, error) {
	for i := 0; i < maxRetries; i++ {
		response, err := s.Read()
		if err == nil && len(response) > 0 {
			return response, nil
		}
		if i < maxRetries-1 {
			time.Sleep(delay)
		}
	}
	return "", fmt.Errorf("failed to read after %d retries", maxRetries)
}

// WriteWithRetry writes to serial with retry logic
func (s *SerialPort) WriteWithRetry(command string, maxRetries int, delay time.Duration) error {
	for i := 0; i < maxRetries; i++ {
		err := s.WriteCommand(command)
		if err == nil {
			return nil
		}
		if i < maxRetries-1 {
			time.Sleep(delay)
		}
	}
	return fmt.Errorf("failed to write after %d retries", maxRetries)
}

// SendAndReceive sends a command and reads the response with hybrid retry approach
// Step 1: Flush input, write command, try to read
// Step 2: If write fails, retry with delay
// Step 3: If read fails, retry with delay
// Step 4: If still failing, reopen port and retry
// Uses write lock to ensure write operations have priority and can preempt reads
func (s *SerialPort) SendAndReceive(command string, maxRetries int) (string, error) {
	// Acquire write lock to ensure exclusive access for write operations
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	// Try up to maxRetries with hybrid approach
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Wait for any ongoing read to complete before attempting write
		// This ensures write operations have priority by waiting for reads to finish
		s.WaitForReadComplete()

		// For write operations, force reopen port first to ensure clean state
		// This helps if a read operation left the port in a bad state
		// The force reopen ignores the reopen count limit
		if attempt == 0 {
			_ = s.ForceReopen()
		}

		// Flush input before sending (multiple times for thorough cleaning)
		s.FlushInputMultiple(3)

		// Write command with retry
		err := s.WriteWithRetry(command, s.writeMaxRetries, s.writeWithRetryDelay)
		if err != nil {
			// Try to reopen and retry
			if s.tryReopen() {
				continue
			}
			return "", fmt.Errorf("write error (attempt %d/%d): %w", attempt+1, maxRetries, err)
		}

		// Small delay to allow device to respond (configurable)
		time.Sleep(s.deviceResponseDelay)

		// Read response with retry
		response, err := s.ReadWithRetry(s.readMaxRetries, s.readWithRetryDelay)
		if err == nil && len(response) > 0 {
			return response, nil
		}

		// If read failed, try to reopen and retry
		if s.tryReopen() {
			continue
		}
	}

	return "", fmt.Errorf("failed to communicate after %d attempts", maxRetries)
}

// tryReopen attempts to reopen the serial port (part of hybrid approach)
func (s *SerialPort) tryReopen() bool {
	s.mu.Lock()
	canReopen := s.open && s.reopenCount < s.maxReopens
	s.mu.Unlock()

	if !canReopen {
		return false
	}

	if err := s.Reopen(); err != nil {
		return false
	}
	return true
}

// ForceReopen forces a reopen of the serial port, ignoring the reopen count limit
// This is used for write priority operations to ensure clean state
func (s *SerialPort) ForceReopen() error {
	s.mu.Lock()

	// Close existing port
	if s.port != nil {
		s.port.Close()
		s.port = nil
	}
	s.open = false

	s.mu.Unlock()

	// Small delay before reopening
	time.Sleep(5 * time.Millisecond)

	s.mu.Lock()
	port, err := serial.OpenPort(s.config)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to force reopen serial port: %w", err)
	}

	s.port = port
	s.open = true
	s.reopenCount = 0 // Reset counter for forced reopen
	s.mu.Unlock()

	return nil
}

// SendAndReceiveWithTiming sends a command and reads the response with timing info
func (s *SerialPort) SendAndReceiveWithTiming(command string, maxRetries int) (string, time.Duration, error) {
	startTime := time.Now()

	response, err := s.SendAndReceive(command, maxRetries)
	duration := time.Since(startTime)

	return response, duration, err
}

// ParseResponse parses a serial response and extracts the value
// Expected format: "command value" (e.g., "130 1067 3")
// It checks the register address in the response matches the expected register
func ParseResponse(response string, expectedCommand string) (string, bool) {
	// Split response into lines (device may send multiple responses)
	lines := strings.Split(response, "\n")

	for _, line := range lines {
		parts := strings.Fields(line)

		// Expected format: command response_value
		// e.g., "130 1067 3" -> parts[0]=130, parts[1]=1067, parts[2]=3
		if len(parts) >= 3 {
			// Check if the command matches (both device address and register address)
			cmd := parts[0] + " " + parts[1]
			if cmd == expectedCommand {
				return parts[2], true
			}
		}
	}

	return "", false
}
