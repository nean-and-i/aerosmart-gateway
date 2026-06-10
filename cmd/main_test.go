package main

import (
	"testing"
	"time"
)

// TestCalculateBackoffWithJitter_ExponentialBaseAndCap verifies the deterministic
// part of the backoff (jitterPercent=0): exponential growth capped at maxDelay.
func TestCalculateBackoffWithJitter_ExponentialBaseAndCap(t *testing.T) {
	const (
		initial = 100
		maxD    = 1000
	)
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{2, 400 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{4, 1000 * time.Millisecond},  // 1600 capped at max
		{10, 1000 * time.Millisecond}, // far above max -> capped
	}
	for _, c := range cases {
		if got := calculateBackoffWithJitter(c.attempt, initial, maxD, 0); got != c.want {
			t.Errorf("attempt %d: got %v, want %v", c.attempt, got, c.want)
		}
	}
}

// TestCalculateBackoffWithJitter_WithinJitterBounds verifies that, with jitter
// enabled, every delay stays within +/- jitterPercent of the exponential base.
func TestCalculateBackoffWithJitter_WithinJitterBounds(t *testing.T) {
	const (
		initial = 100
		maxD    = 10000
		jitter  = 25
		base    = 100 * time.Millisecond
	)
	lower := time.Duration(float64(base) * (1 - float64(jitter)/100)) // 75ms
	upper := time.Duration(float64(base) * (1 + float64(jitter)/100)) // 125ms
	for i := 0; i < 200; i++ {
		got := calculateBackoffWithJitter(0, initial, maxD, jitter)
		if got < lower || got > upper {
			t.Fatalf("delay %v out of jitter bounds [%v, %v]", got, lower, upper)
		}
	}
}

// TestCalculateBackoffWithJitter_ProducesVariation is a regression guard for the
// former bug where math.Float64frombits(timestamp) made the jitter effectively
// constant. A correct implementation must vary across calls.
func TestCalculateBackoffWithJitter_ProducesVariation(t *testing.T) {
	first := calculateBackoffWithJitter(2, 100, 10000, 25)
	for i := 0; i < 200; i++ {
		if calculateBackoffWithJitter(2, 100, 10000, 25) != first {
			return // observed variation -> pass
		}
	}
	t.Error("expected jitter to vary across calls, but all 200 values were identical")
}
