package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type countingCycler struct {
	mu     sync.Mutex
	starts int
	ends   []time.Time
	err    error
	delay  time.Duration
}

func (c *countingCycler) RunCycle(ctx context.Context) error {
	c.mu.Lock()
	c.starts++
	c.mu.Unlock()

	if c.delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.delay):
		}
	}

	c.mu.Lock()
	c.ends = append(c.ends, time.Now())
	c.mu.Unlock()
	return c.err
}

func (c *countingCycler) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.starts
}

func TestPollLoopRunsImmediatelyThenStopsOnCancel(t *testing.T) {
	c := &countingCycler{}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() { pollLoop(ctx, c, time.Hour, testMainLogger()); close(done) }()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && c.count() == 0 {
		time.Sleep(time.Millisecond)
	}
	if c.count() != 1 {
		t.Fatalf("cycles run = %d, want 1 immediately at startup", c.count())
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pollLoop did not return after cancellation")
	}
}

// A failing cycle must not end the loop; a flaky link recovers on a later one.
func TestPollLoopContinuesAfterCycleError(t *testing.T) {
	c := &countingCycler{err: errors.New("boom")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() { pollLoop(ctx, c, 10*time.Millisecond, testMainLogger()); close(done) }()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && c.count() < 3 {
		time.Sleep(2 * time.Millisecond)
	}
	if c.count() < 3 {
		t.Fatalf("cycles run = %d, want at least 3 despite errors", c.count())
	}
	cancel()
	<-done
}

// The interval must be measured from the end of a cycle, so a readout that
// overruns still leaves the device an idle gap before the next one.
func TestPollLoopWaitsIntervalAfterCycleEnds(t *testing.T) {
	const interval = 60 * time.Millisecond
	c := &countingCycler{delay: 80 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() { pollLoop(ctx, c, interval, testMainLogger()); close(done) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		n := len(c.ends)
		c.mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done

	c.mu.Lock()
	ends := append([]time.Time(nil), c.ends...)
	c.mu.Unlock()

	if len(ends) < 2 {
		t.Fatalf("completed cycles = %d, want at least 2", len(ends))
	}
	// Second cycle ends at least one interval plus its own duration after the
	// first, which only holds if the wait starts when the cycle finishes.
	if gap := ends[1].Sub(ends[0]); gap < interval {
		t.Errorf("gap between cycle completions = %v, want at least the %v interval", gap, interval)
	}
}
