package serial

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"net"
	"os"
	"syscall"
	"time"
)

// maxFrameLen bounds one protocol frame. Real frames are tiny ("130 1067 3"),
// so anything longer means the stream has desynced and must be resynced.
const maxFrameLen = 512

// idlePoll is how long to wait after a read that produced nothing. The kernel
// already blocks for VTIME (100ms granularity), so this only guards against a
// port that returns immediately.
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
// *tarm/serial.Port satisfies it. Config.Opener can supply any other
// implementation, which allows alternate transports (a networked serial
// bridge, for instance) and scripted devices in tests.
type Port interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Flush() error
	Close() error
}

// frameReader turns a byte stream into newline-delimited frames. Its buffer is
// persistent: bytes received but not yet terminated survive across readFrame
// calls, which is what keeps a lossy link from desyncing permanently.
type frameReader struct {
	port    Port
	buf     []byte
	scratch [256]byte
}

func newFrameReader(p Port) *frameReader { return &frameReader{port: p} }

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
		if time.Now().After(deadline) {
			return "", ErrReadTimeout
		}

		n, err := f.port.Read(f.scratch[:])
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

// reset drops any buffered bytes. Used after an overflow and whenever the
// hardware input buffer is flushed, so the two stay consistent.
func (f *frameReader) reset() { f.buf = f.buf[:0] }

// IsTimeout reports whether err just means "no data yet".
//
// tarm/serial opens the tty with VMIN=0, so a read whose VTIME expires returns
// zero bytes, which os.File reports as io.EOF. Treating that as a broken port
// would make the supervisor reopen on every idle poll.
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
	return errors.Is(err, syscall.EIO) || errors.Is(err, syscall.ENODEV) ||
		errors.Is(err, syscall.ENXIO) || errors.Is(err, syscall.EBADF) ||
		errors.Is(err, os.ErrClosed) || errors.Is(err, fs.ErrNotExist) ||
		errors.Is(err, ErrPortNotOpen)
}
