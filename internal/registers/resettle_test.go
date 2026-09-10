package registers

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nean/aerosmart-gateway/internal/config"
)

// refreshRecorder counts refreshes and remembers which register was asked for.
type refreshRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *refreshRecorder) refresh(_ context.Context, reg config.WriteRegisterConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, reg.Name)
}

func (r *refreshRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

// A lagging value must reach MQTT without waiting for the next full read
// cycle: the actual fan stage trails a stage change by seconds.
func TestResettlerRefreshesAfterTheDelay(t *testing.T) {
	rec := &refreshRecorder{}
	rs := newResettler(20*time.Millisecond, rec.refresh, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { rs.run(ctx); close(done) }()

	rs.schedule(config.WriteRegisterConfig{Name: "luefterstufe"})

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && len(rec.snapshot()) == 0 {
		time.Sleep(time.Millisecond)
	}
	if got := rec.snapshot(); len(got) != 1 || got[0] != "luefterstufe" {
		t.Fatalf("refreshes = %v, want one for luefterstufe", got)
	}

	cancel()
	<-done
}

// Changing stage repeatedly must not queue a refresh per command; the wait
// restarts so one read happens after the changes stop.
func TestResettlerCoalescesRapidWrites(t *testing.T) {
	rec := &refreshRecorder{}
	rs := newResettler(60*time.Millisecond, rec.refresh, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { rs.run(ctx); close(done) }()

	// Five writes, each well inside the delay.
	for i := 0; i < 5; i++ {
		rs.schedule(config.WriteRegisterConfig{Name: "luefterstufe"})
		time.Sleep(10 * time.Millisecond)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(rec.snapshot()) == 0 {
		time.Sleep(2 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // allow any extras to appear

	if got := rec.snapshot(); len(got) != 1 {
		t.Errorf("refreshes = %v, want exactly 1 after the writes stopped", got)
	}

	cancel()
	<-done
}

// The latest write wins, so the register refreshed is the one last commanded.
func TestResettlerRefreshesTheLatestRegister(t *testing.T) {
	rec := &refreshRecorder{}
	rs := newResettler(30*time.Millisecond, rec.refresh, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { rs.run(ctx); close(done) }()

	rs.schedule(config.WriteRegisterConfig{Name: "luefterstufe"})
	rs.schedule(config.WriteRegisterConfig{Name: "boilerheizstab"})

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && len(rec.snapshot()) == 0 {
		time.Sleep(time.Millisecond)
	}
	if got := rec.snapshot(); len(got) != 1 || got[0] != "boilerheizstab" {
		t.Fatalf("refreshes = %v, want one for boilerheizstab", got)
	}

	cancel()
	<-done
}

// schedule runs on the command path and must never block, even with nothing
// consuming it.
func TestResettlerScheduleNeverBlocks(t *testing.T) {
	rs := newResettler(time.Hour, func(context.Context, config.WriteRegisterConfig) {}, testLogger())

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			rs.schedule(config.WriteRegisterConfig{Name: "luefterstufe"})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("schedule() blocked; the write path would stall")
	}
}

func TestResettlerDisabledWithZeroDelay(t *testing.T) {
	rec := &refreshRecorder{}
	rs := newResettler(0, rec.refresh, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { rs.run(ctx); close(done) }()

	rs.schedule(config.WriteRegisterConfig{Name: "luefterstufe"})
	time.Sleep(100 * time.Millisecond)

	if got := rec.snapshot(); len(got) != 0 {
		t.Errorf("refreshes = %v, want none when the delay is zero", got)
	}

	cancel()
	<-done
}
