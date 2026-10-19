// Package serial implements framed request/response communication with an
// Aerosmart unit over a serial link.
//
// The link is assumed to be lossy and shared: frames arrive split across
// reads, the unit's panel talks on the same bus, so frames from other
// conversations appear on the wire and a collision can corrupt one, and the
// USB adapter can disappear entirely. Five rules keep it stable:
//
//  1. Received bytes are buffered persistently, so a frame interrupted by a
//     timeout is completed rather than discarded (discarding it desyncs the
//     stream permanently).
//  2. A command is only transmitted once the bus has been quiet for a moment,
//     and is drained before its reply is awaited, so it neither collides with
//     a frame in progress nor is still queued when the next exchange begins.
//  3. One transaction reads until it sees a strictly well-formed frame
//     matching its own command, discarding anything else, and never re-sends
//     the command to resync.
//  4. Only input is ever discarded. Discarding output loses a command that
//     has been written but not yet transmitted.
//  5. Closing and reopening the port is reserved for genuine port failures and
//     performed by a single supervisor, because a reopen toggles DTR/RTS and
//     can reset the attached device.
package serial

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.bug.st/serial"
)

// ErrPortNotOpen is returned when an operation needs an open port and has none.
var ErrPortNotOpen = errors.New("serial: port not open")

// Logger is the subset of logging this package needs.
type Logger interface {
	Debug(format string, args ...any)
	Info(format string, args ...any)
	Warn(format string, args ...any)
	Error(format string, args ...any)
}

// Config holds serial link settings. It is passed as a struct rather than as
// positional arguments so an option cannot be silently dropped.
type Config struct {
	Port     string
	BaudRate int
	DataBits int
	Parity   string // "N", "O", "E"
	StopBits int    // 1 or 2

	// ReadTimeout is how long one read may block with no data.
	ReadTimeout time.Duration

	// ResponseTimeout is the total budget for one transaction: how long to
	// wait for the matching response frame after writing a command.
	ResponseTimeout time.Duration

	// ReopenBackoff is the initial delay between reopen attempts, doubling up
	// to MaxReopenBackoff.
	ReopenBackoff    time.Duration
	MaxReopenBackoff time.Duration

	// HealthCheckInterval is how often the supervisor polls port health.
	HealthCheckInterval time.Duration

	// BusQuietGap is how long the line must be silent before a command is
	// transmitted. Another device shares the bus, and transmitting into one of
	// its frames corrupts both.
	BusQuietGap time.Duration

	// BusQuietMaxWait bounds the wait for a quiet bus, so a device that never
	// pauses delays commands rather than blocking them.
	BusQuietMaxWait time.Duration

	// XonXoff and DsrDtr have no effect: the driver has no flow control
	// support. config.AppConfig.Warnings reports them at startup.
	XonXoff bool
	DsrDtr  bool

	// Opener overrides how the port is opened. When nil the configured serial
	// device is opened with the settings above.
	Opener func() (Port, error)
}

func (c *Config) applyDefaults(log Logger) {
	if c.BaudRate == 0 {
		c.BaudRate = 115200
	}
	if c.DataBits == 0 {
		c.DataBits = 8
	}
	if c.Parity == "" {
		c.Parity = "N"
	}
	if c.StopBits == 0 {
		c.StopBits = 1
	}
	if c.ResponseTimeout <= 0 {
		c.ResponseTimeout = 500 * time.Millisecond
	}
	if c.ReopenBackoff <= 0 {
		c.ReopenBackoff = 500 * time.Millisecond
	}
	if c.MaxReopenBackoff < c.ReopenBackoff {
		// The 30s default only applies when it does not itself invert the
		// bound: a ReopenBackoff configured above 30s must still get a
		// MaxReopenBackoff at or above it, or the doubling loop shrinks on
		// its very first step.
		c.MaxReopenBackoff = max(30*time.Second, c.ReopenBackoff)
	}
	if c.HealthCheckInterval <= 0 {
		c.HealthCheckInterval = time.Second
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = 100 * time.Millisecond
	}
	if c.BusQuietGap <= 0 {
		c.BusQuietGap = 20 * time.Millisecond
	}
	if c.BusQuietMaxWait <= 0 {
		c.BusQuietMaxWait = 100 * time.Millisecond
	}
	if c.BusQuietMaxWait < c.BusQuietGap {
		c.BusQuietMaxWait = c.BusQuietGap
	}

	// A response can never arrive faster than one read cycle.
	if c.ResponseTimeout < c.ReadTimeout {
		log.Warn("serial: response timeout %v is below read_timeout %v, raising it",
			c.ResponseTimeout, c.ReadTimeout)
		c.ResponseTimeout = c.ReadTimeout
	}
}

// SerialPort is a framed, request/response serial connection. It is safe for
// concurrent use: transactions are serialised, so only one command is on the
// wire at a time.
type SerialPort struct {
	cfg Config
	log Logger

	// opener creates the underlying port; replaced in tests.
	opener func() (Port, error)

	// txMu serialises whole transactions and excludes the supervisor's
	// close/reopen, so the port is never swapped mid-transaction.
	txMu sync.Mutex

	mu      sync.Mutex
	port    Port
	fr      *frameReader
	open    bool
	healthy bool
}

// New creates a serial port from cfg.
//
// Options this driver cannot honour (xonxoff, dsrdtr) are reported at startup
// by config.AppConfig.Warnings rather than ignored in silence. They are not
// errors: the driver has never implemented flow control, so a link configured
// that way has always run without it.
func New(cfg Config, log Logger) *SerialPort {
	cfg.applyDefaults(log)
	s := &SerialPort{cfg: cfg, log: log}
	s.opener = cfg.Opener
	if s.opener == nil {
		s.opener = s.openDevice
	}
	return s
}

// openDevice opens the real device with every configured line setting applied.
func (s *SerialPort) openDevice() (Port, error) {
	parity := serial.NoParity
	switch strings.ToUpper(s.cfg.Parity) {
	case "O":
		parity = serial.OddParity
	case "E":
		parity = serial.EvenParity
	}
	stop := serial.OneStopBit
	if s.cfg.StopBits == 2 {
		stop = serial.TwoStopBits
	}
	port, err := serial.Open(s.cfg.Port, &serial.Mode{
		BaudRate: s.cfg.BaudRate,
		DataBits: s.cfg.DataBits,
		Parity:   parity,
		StopBits: stop,
	})
	if err != nil {
		// The driver's errors do not name the device.
		return nil, fmt.Errorf("open %s: %w", s.cfg.Port, err)
	}
	return port, nil
}

// Open opens the port. It is idempotent.
func (s *SerialPort) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.openLocked()
}

func (s *SerialPort) openLocked() error {
	if s.open {
		return nil
	}
	port, err := s.opener()
	if err != nil {
		return err
	}
	s.port = port
	s.fr = newFrameReader(port, s.cfg.ReadTimeout)
	s.open = true
	s.healthy = true
	return nil
}

// Close closes the port. It is idempotent.
//
// It takes txMu so it waits for any in-flight transaction to finish rather
// than closing the device mid-exchange.
func (s *SerialPort) Close() error {
	s.txMu.Lock()
	defer s.txMu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeLocked()
}

func (s *SerialPort) closeLocked() error {
	s.open = false
	s.healthy = false
	port := s.port
	s.port = nil
	s.fr = nil
	if port == nil {
		return nil
	}
	return port.Close()
}

// IsOpen reports whether the port is open.
func (s *SerialPort) IsOpen() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.open
}

// IsHealthy reports whether the port is open and has not hit a port-level
// error. A read timeout does not make a port unhealthy.
func (s *SerialPort) IsHealthy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.open && s.healthy
}

// markUnhealthy records that the port needs to be reopened by the supervisor.
func (s *SerialPort) markUnhealthy(cause error) {
	s.mu.Lock()
	wasHealthy := s.healthy
	s.healthy = false
	s.mu.Unlock()
	if wasHealthy {
		s.log.Error("serial: port %s marked unhealthy: %v", s.cfg.Port, cause)
	}
}

// session returns the current port and frame reader. Callers must hold txMu,
// which guarantees the port is not swapped while they use it.
func (s *SerialPort) session() (Port, *frameReader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.open || s.port == nil || s.fr == nil {
		return nil, nil, ErrPortNotOpen
	}
	return s.port, s.fr, nil
}

// Transact writes command and returns the value from the response frame whose
// "<node> <register>" prefix matches it.
//
// Frames that do not match are discarded and reading continues until the
// response timeout: the device may have queued a stale reply or an unsolicited
// message, and re-sending the command to resync would re-apply writes.
func (s *SerialPort) Transact(ctx context.Context, command string) (string, error) {
	s.txMu.Lock()
	defer s.txMu.Unlock()

	if err := ctx.Err(); err != nil {
		return "", err
	}

	port, fr, err := s.session()
	if err != nil {
		return "", err
	}

	// Discarding stale input before asking means the first frame back belongs
	// to this command, or to someone else's conversation, never to an earlier
	// one of ours.
	if err := s.transmit(ctx, port, fr, command); err != nil {
		return "", err
	}

	want := commandPrefix(command)
	deadline := time.Now().Add(s.cfg.ResponseTimeout)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		frame, err := fr.readFrame(deadline)
		if err != nil {
			if IsPortError(err) {
				s.markUnhealthy(err)
			}
			return "", fmt.Errorf("read response to %q: %w", command, err)
		}
		if value, ok := ParseResponse(frame, want); ok {
			return value, nil
		}
		s.log.Debug("serial: discarding frame %q while waiting for %q", frame, want)
	}
}

// Send writes a command without waiting for a matching response frame.
//
// The unit applies register writes but does not reliably echo them: on a real
// gateway most writes produced no matching frame within a second, while the
// bus carried the resulting status change, proving the write had landed.
// Waiting for an echo therefore failed writes that had in fact succeeded, and
// retrying re-applied them. Callers confirm a write by reading the affected
// registers back instead.
func (s *SerialPort) Send(ctx context.Context, command string) error {
	s.txMu.Lock()
	defer s.txMu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	port, fr, err := s.session()
	if err != nil {
		return err
	}

	// Discarding stale input means the reply to the following read-back
	// cannot be satisfied by a frame that predates this command.
	return s.transmit(ctx, port, fr, command)
}

// transmit puts one command on the wire: it waits for a quiet bus, discards
// stale input, writes the command and waits until it has left the port.
// Callers must hold txMu.
func (s *SerialPort) transmit(ctx context.Context, port Port, fr *frameReader, command string) error {
	fail := func(what string, err error) error {
		if IsPortError(err) {
			s.markUnhealthy(err)
		}
		return fmt.Errorf("%s %q: %w", what, command, err)
	}

	quiet, err := fr.waitQuiet(ctx, s.cfg.BusQuietGap, s.cfg.BusQuietMaxWait)
	if err != nil {
		if isCancellation(err) {
			return err
		}
		return fail("wait for a quiet bus before", err)
	}
	if !quiet {
		s.log.Debug("serial: bus still busy after %v, transmitting %q anyway", s.cfg.BusQuietMaxWait, command)
	}

	fr.reset()
	if err := port.ResetInputBuffer(); err != nil {
		return fail("discard stale input before", err)
	}
	if _, err := port.Write([]byte(command + "\r\n")); err != nil {
		return fail("write", err)
	}
	// Until the command has actually been transmitted, the next exchange
	// could start, and "sent" would only mean "queued in the kernel".
	if err := port.Drain(); err != nil {
		return fail("drain", err)
	}
	return nil
}

// isCancellation reports whether err is a context cancellation or deadline.
func isCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// Supervise reopens the port whenever it becomes unhealthy, with exponential
// backoff, until ctx is done. It is the only place that reopens the port, so a
// transient read failure cannot trigger a device-resetting reopen.
func (s *SerialPort) Supervise(ctx context.Context) {
	backoff := s.cfg.ReopenBackoff
	var healthySince time.Time

	for {
		if ctx.Err() != nil {
			return
		}
		if s.IsHealthy() {
			if healthySince.IsZero() {
				healthySince = time.Now()
			}
			// Only treat the port as genuinely recovered once it has stayed
			// healthy for a while; otherwise a port that opens but keeps
			// failing I/O would reset the backoff and be reopened in a loop.
			if time.Since(healthySince) >= s.cfg.MaxReopenBackoff {
				backoff = s.cfg.ReopenBackoff
			}
			if !sleepCtx(ctx, s.cfg.HealthCheckInterval) {
				return
			}
			continue
		}
		healthySince = time.Time{}

		if err := s.reopen(); err != nil {
			s.log.Warn("serial: reopen of %s failed, retrying in %v: %v", s.cfg.Port, backoff, err)
		} else {
			s.log.Info("serial: port %s reopened, holding off %v before another attempt", s.cfg.Port, backoff)
		}

		// Wait after every attempt, successful or not: reopening toggles
		// DTR/RTS, which many devices treat as a reset.
		if !sleepCtx(ctx, backoff) {
			return
		}
		if backoff *= 2; backoff > s.cfg.MaxReopenBackoff {
			backoff = s.cfg.MaxReopenBackoff
		}
	}
}

// reopen closes and reopens the port. It takes txMu so it never runs while a
// transaction is using the port.
func (s *SerialPort) reopen() error {
	s.txMu.Lock()
	defer s.txMu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.closeLocked(); err != nil {
		s.log.Debug("serial: close during reopen: %v", err)
	}
	return s.openLocked()
}

// sleepCtx waits for d, reporting false if ctx finished first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// commandPrefix returns the "<node> <register>" part of a command, which is
// what a response frame echoes back.
func commandPrefix(command string) string {
	fields := strings.Fields(command)
	if len(fields) < 2 {
		return strings.TrimSpace(command)
	}
	return fields[0] + " " + fields[1]
}

// ParseResponse extracts the value from a response frame, verifying that the
// frame answers the expected command.
//
// Frames look like "130 1067 3": node address, register address, integer
// value. The protocol has no checksum and the bus is shared, so a collision
// can splice two frames or corrupt bytes while the prefix still matches. Only
// a frame of exactly that shape is accepted. Line noise around a frame is
// tolerated, since it cannot change the value, but anything unexpected inside
// one rejects it.
func ParseResponse(response string, expectedCommand string) (string, bool) {
	for line := range strings.SplitSeq(response, "\n") {
		line = strings.TrimFunc(line, func(r rune) bool { return r == ' ' || !isPrintableASCII(r) })
		if strings.IndexFunc(line, func(r rune) bool { return !isPrintableASCII(r) }) >= 0 {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 3 || !isInteger(parts[2]) {
			continue
		}
		if parts[0]+" "+parts[1] == expectedCommand {
			return parts[2], true
		}
	}
	return "", false
}

// isPrintableASCII reports whether r is a printable ASCII character. Invalid
// UTF-8 decodes to utf8.RuneError, which is not.
func isPrintableASCII(r rune) bool { return r >= 0x20 && r <= 0x7e }

// isInteger reports whether s is an optionally negative run of digits, the
// only value format the unit sends.
func isInteger(s string) bool {
	s = strings.TrimPrefix(s, "-")
	if s == "" {
		return false
	}
	for _, c := range []byte(s) {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
