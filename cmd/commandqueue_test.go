package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestCommandQueueProcessesInOrder(t *testing.T) {
	q := newCommandQueue(8, testMainLogger())

	var mu sync.Mutex
	var seen []string
	done := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		q.run(ctx, func(_ context.Context, topic, payload string) error {
			mu.Lock()
			seen = append(seen, topic+"="+payload)
			mu.Unlock()
			return nil
		})
		close(done)
	}()

	for _, v := range []string{"3", "0", "5"} {
		q.enqueue("dw/aerosmart/luefterstufe", v)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n == 3 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()

	want := []string{
		"dw/aerosmart/luefterstufe=3",
		"dw/aerosmart/luefterstufe=0",
		"dw/aerosmart/luefterstufe=5",
	}
	if len(got) != len(want) {
		t.Fatalf("processed %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			// Order matters: two rapid commands must not be applied backwards.
			t.Errorf("processed[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not return after cancellation")
	}
}

// enqueue runs on paho's router goroutine. Blocking there stops inbound
// packets being read, including the PUBACKs for the handler's own publishes,
// so a full queue must be dropped rather than waited on.
func TestCommandQueueEnqueueNeverBlocks(t *testing.T) {
	q := newCommandQueue(2, testMainLogger()) // nothing consuming

	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			q.enqueue("dw/aerosmart/luefterstufe", "3")
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("enqueue() blocked on a full queue; paho's router would stall")
	}
}

func TestCommandQueueRunStopsOnCancel(t *testing.T) {
	q := newCommandQueue(4, testMainLogger())
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		q.run(ctx, func(context.Context, string, string) error { return nil })
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not return after cancellation")
	}
}
