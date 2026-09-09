package serial

import (
	"errors"
	"io"
	"syscall"
	"testing"
	"time"
)

// readResult is one scripted return from scriptedPort.Read.
type readResult struct {
	data string
	err  error
}

// scriptedPort is a Port whose reads are pre-programmed, so framing
// behaviour can be tested without hardware. Once the script is exhausted it
// behaves like an idle tty: zero bytes and io.EOF (what tarm/serial returns
// when VTIME expires with VMIN=0).
type scriptedPort struct {
	reads   []readResult
	idx     int
	writes  []string
	flushes int
	closed  bool
}

func (s *scriptedPort) Read(p []byte) (int, error) {
	if s.idx >= len(s.reads) {
		return 0, io.EOF
	}
	r := &s.reads[s.idx]
	n := copy(p, r.data)
	r.data = r.data[n:]
	if r.data != "" {
		// Entry not fully delivered yet; hold back its error, as a real port
		// would hand over the rest on the following read.
		return n, nil
	}
	s.idx++
	return n, r.err
}

func (s *scriptedPort) Write(p []byte) (int, error) {
	s.writes = append(s.writes, string(p))
	return len(p), nil
}

func (s *scriptedPort) Flush() error { s.flushes++; return nil }
func (s *scriptedPort) Close() error { s.closed = true; return nil }

func shortDeadline() time.Time { return time.Now().Add(30 * time.Millisecond) }

func TestReadFrameSingleRead(t *testing.T) {
	fr := newFrameReader(&scriptedPort{reads: []readResult{{data: "130 1067 3\r\n"}}})

	got, err := fr.readFrame(shortDeadline())
	if err != nil {
		t.Fatalf("readFrame() error = %v, want nil", err)
	}
	if got != "130 1067 3" {
		t.Errorf("readFrame() = %q, want %q", got, "130 1067 3")
	}
}

func TestReadFrameReassemblesSplitFrame(t *testing.T) {
	fr := newFrameReader(&scriptedPort{reads: []readResult{
		{data: "130 10"},
		{data: "67 3\r\n"},
	}})

	got, err := fr.readFrame(shortDeadline())
	if err != nil {
		t.Fatalf("readFrame() error = %v, want nil", err)
	}
	if got != "130 1067 3" {
		t.Errorf("readFrame() = %q, want %q", got, "130 1067 3")
	}
}

// The device may emit several frames in one burst. The second must not be lost.
func TestReadFrameKeepsSecondFrameBuffered(t *testing.T) {
	fr := newFrameReader(&scriptedPort{reads: []readResult{
		{data: "130 1067 3\r\n130 5003 2\r\n"},
	}})

	first, err := fr.readFrame(shortDeadline())
	if err != nil {
		t.Fatalf("first readFrame() error = %v, want nil", err)
	}
	if first != "130 1067 3" {
		t.Errorf("first readFrame() = %q, want %q", first, "130 1067 3")
	}

	second, err := fr.readFrame(shortDeadline())
	if err != nil {
		t.Fatalf("second readFrame() error = %v, want nil", err)
	}
	if second != "130 5003 2" {
		t.Errorf("second readFrame() = %q, want %q", second, "130 5003 2")
	}
}

// This is the desync bug: a frame interrupted by a timeout must not be thrown
// away, or the tail arrives alone and every later frame is one behind.
func TestReadFrameRetainsPartialDataAcrossTimeout(t *testing.T) {
	sp := &scriptedPort{reads: []readResult{{data: "130 10"}}}
	fr := newFrameReader(sp)

	if _, err := fr.readFrame(shortDeadline()); !errors.Is(err, ErrReadTimeout) {
		t.Fatalf("readFrame() error = %v, want ErrReadTimeout", err)
	}

	// The tail arrives on a later read; the frame must complete.
	sp.reads = append(sp.reads, readResult{data: "67 3\r\n"})
	got, err := fr.readFrame(shortDeadline())
	if err != nil {
		t.Fatalf("readFrame() after timeout error = %v, want nil", err)
	}
	if got != "130 1067 3" {
		t.Errorf("readFrame() after timeout = %q, want %q", got, "130 1067 3")
	}
}

func TestReadFrameBareLF(t *testing.T) {
	fr := newFrameReader(&scriptedPort{reads: []readResult{{data: "130 1067 3\n"}}})

	got, err := fr.readFrame(shortDeadline())
	if err != nil {
		t.Fatalf("readFrame() error = %v, want nil", err)
	}
	if got != "130 1067 3" {
		t.Errorf("readFrame() = %q, want %q", got, "130 1067 3")
	}
}

func TestReadFrameOverflowResyncs(t *testing.T) {
	long := make([]byte, maxFrameLen+1)
	for i := range long {
		long[i] = 'x'
	}
	sp := &scriptedPort{reads: []readResult{{data: string(long)}}}
	fr := newFrameReader(sp)

	if _, err := fr.readFrame(shortDeadline()); !errors.Is(err, ErrFrameOverflow) {
		t.Fatalf("readFrame() error = %v, want ErrFrameOverflow", err)
	}

	// After an overflow the buffer must be dropped so the next frame is clean.
	sp.reads = append(sp.reads, readResult{data: "130 1067 3\r\n"})
	got, err := fr.readFrame(shortDeadline())
	if err != nil {
		t.Fatalf("readFrame() after overflow error = %v, want nil", err)
	}
	if got != "130 1067 3" {
		t.Errorf("readFrame() after overflow = %q, want %q", got, "130 1067 3")
	}
}

func TestReadFrameSurfacesIOError(t *testing.T) {
	fr := newFrameReader(&scriptedPort{reads: []readResult{{err: syscall.EIO}}})

	_, err := fr.readFrame(shortDeadline())
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("readFrame() error = %v, want syscall.EIO", err)
	}
}

// A zero-byte read is how a VTIME expiry surfaces; it must not be reported as
// a broken port, or the supervisor would reopen on every idle poll.
func TestReadFrameTreatsEOFAsTimeout(t *testing.T) {
	fr := newFrameReader(&scriptedPort{})

	_, err := fr.readFrame(shortDeadline())
	if !errors.Is(err, ErrReadTimeout) {
		t.Fatalf("readFrame() error = %v, want ErrReadTimeout", err)
	}
	if IsPortError(err) {
		t.Error("IsPortError(ErrReadTimeout) = true, want false")
	}
}

// Data delivered alongside io.EOF in the same read must still be consumed.
func TestReadFrameConsumesDataDeliveredWithEOF(t *testing.T) {
	fr := newFrameReader(&scriptedPort{reads: []readResult{
		{data: "130 1067 3\r\n", err: io.EOF},
	}})

	got, err := fr.readFrame(shortDeadline())
	if err != nil {
		t.Fatalf("readFrame() error = %v, want nil", err)
	}
	if got != "130 1067 3" {
		t.Errorf("readFrame() = %q, want %q", got, "130 1067 3")
	}
}
