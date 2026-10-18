package serial

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net"
	"os"
	"syscall"
	"time"

	"go.bug.st/serial"
)

// maxFrameLen bounds one protocol frame. Real frames are tiny ("130 1067 3"),
// so anything longer means the stream has desynced and must be resynced.
const maxFrameLen = 512

// idlePoll is how long to wait after a read that produced nothing. The driver
// already blocks for the read timeout, so this only guards against a port that
// returns immediately.
const idlePoll = time.Millisecond

var (
	// ErrReadTimeout means no complete frame arrived before the deadline. Any
	// bytes already received stay buffered for the next attempt, so a frame
	// split across reads is completed rather than lost.
	ErrReadTimeout = errors.New("serial: read timeout")
	// ErrFrameOverflow means maxFrameLen was exceeded with no terminator. The
	// buffer is dropped so the next frame starts clean.
	ErrFrameOverflow = errors.New("serial: frame overflow")
)

// Port is the subset of a serial device the framing layer needs.
//
// go.bug.st/serial's Port satisfies it. Config.Opener can supply any other
// implementation, which allows alternate transports (a networked serial
// bridge, for instance) and scripted devices in tests.
//
// A Read that times out with nothing received returns zero bytes and a nil
// error (or an error IsTimeout recognises).
type Port interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)

	// ResetInputBuffer discards received bytes not yet read (TCIFLUSH). It
	// must leave output alone: discarding output as well (TCIOFLUSH) threw
	// away a command that had been written but not yet transmitted.
	ResetInputBuffer() error

	// Drain blocks until everything written has left the port (tcdrain).
	Drain() error

	// SetReadTimeout sets how long one Read may block with no data.
	SetReadTimeout(t time.Duration) error

	Close() error
}

// frameReader turns a byte stream into newline-delimited frames. Its buffer is
// persistent: bytes received but not yet terminated survive across readFrame
// calls, which is what keeps a lossy link from desyncing permanently.
type frameReader struct {
	port    Port
	buf     []byte
	scratch [256]byte

	// readTimeout is the most a single read may block.
	readTimeout time.Duration
	// portTimeout is the read timeout the port is currently set to, so it is
	// only changed when a read needs a different one. Zero means unknown.
	portTimeout time.Duration
}

func newFrameReader(p Port, readTimeout time.Duration) *frameReader {
	return &frameReader{port: p, readTimeout: readTimeout}
}

// read reads from the port, blocking for at most wait.
func (f *frameReader) read(wait time.Duration) (int, error) {
	if wait != f.portTimeout {
		if err := f.port.SetReadTimeout(wait); err != nil {
			f.portTimeout = 0
			return 0, err
		}
		f.portTimeout = wait
	}
	return f.port.Read(f.scratch[:])
}

// readFrame returns the next complete frame, without its line terminator.
// On ErrReadTimeout the partial frame is retained for the next call.
func (f *frameReader) readFrame(deadline time.Time) (string, error) {
	for {
		// Serve a buffered frame before touching the port, so a burst
		// containing several frames is delivered one at a time.
		if i := bytes.IndexByte(f.buf, '\n'); i >= 0 {
			frame := string(bytes.TrimRight(f.buf[:i], "\r"))
			f.buf = f.buf[:copy(f.buf, f.buf[i+1:])]
			return frame, nil
		}
		if len(f.buf) > maxFrameLen {
			f.reset()
			return "", ErrFrameOverflow
		}
		// A read that started just before the deadline would otherwise block
		// for a whole read timeout past it.
		wait := min(f.readTimeout, time.Until(deadline))
		if wait <= 0 {
			return "", ErrReadTimeout
		}

		n, err := f.read(wait)
		if n > 0 {
			// Consume the data before judging err: a read can return bytes
			// and io.EOF together.
			f.buf = append(f.buf, f.scratch[:n]...)
			continue
		}
		if err != nil && !IsTimeout(err) {
			return "", err
		}
		time.Sleep(idlePoll)
	}
}

// waitQuiet reads and discards until no byte has arrived for gap, or maxWait
// has passed.
//
// The unit's panel shares the bus and does not coordinate with us. Starting a
// command while it is mid-frame corrupts both, so the caller listens first.
// The bytes discarded here cannot be a reply to anything of ours: the previous
// exchange has finished and the next command has not been sent yet.
//
// It reports whether the bus went quiet.
func (f *frameReader) waitQuiet(ctx context.Context, gap, maxWait time.Duration) (bool, error) {
	deadline := time.Now().Add(maxWait)
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		n, err := f.read(gap)
		if err != nil && !IsTimeout(err) {
			return false, err
		}
		if n == 0 {
			// A whole gap passed with nothing on the line.
			return true, nil
		}
		if !time.Now().Before(deadline) {
			return false, nil
		}
	}
}

// reset drops any buffered bytes. Used after an overflow and whenever the
// hardware input buffer is flushed, so the two stay consistent.
func (f *frameReader) reset() { f.buf = f.buf[:0] }

// IsTimeout reports whether err just means "no data yet".
//
// go.bug.st/serial reports a read timeout as zero bytes and no error, but
// other transports report it as io.EOF (a tty read with VMIN=0), a deadline or
// EAGAIN. Treating any of those as a broken port would make the supervisor
// reopen on every idle poll.
func IsTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrReadTimeout) || errors.Is(err, io.EOF) ||
		errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, syscall.EAGAIN) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// IsPortError reports whether err means the port itself is gone or unusable,
// which is the only condition that justifies closing and reopening it.
// A mere timeout never qualifies.
func IsPortError(err error) bool {
	if err == nil || IsTimeout(err) {
		return false
	}
	// The driver's own errors do not wrap an errno, so errors.Is cannot see
	// through them.
	var pe *serial.PortError
	if errors.As(err, &pe) {
		switch pe.Code() {
		case serial.PortClosed, serial.PortNotFound, serial.InvalidSerialPort:
			return true
		}
	}
	return errors.Is(err, syscall.EIO) || errors.Is(err, syscall.ENODEV) ||
		errors.Is(err, syscall.ENXIO) || errors.Is(err, syscall.EBADF) ||
		errors.Is(err, os.ErrClosed) || errors.Is(err, fs.ErrNotExist) ||
		errors.Is(err, ErrPortNotOpen)
}
