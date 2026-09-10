package main

import (
	"context"

	"github.com/nean/aerosmart-gateway/internal/logger"
)

// command is one MQTT message destined for a write register.
type command struct {
	topic   string
	payload string
}

// commandQueue moves incoming MQTT commands off paho's router goroutine.
//
// paho invokes subscription callbacks on that goroutine and, with its default
// ordered delivery, a callback that blocks stops inbound packets being read -
// including the PUBACKs for the handler's own publishes. Applying a command
// takes serial I/O and publishes several topics, so running it inline made
// those publishes time out and stalled every queued command behind it. paho
// documents the constraint on Subscribe: the callback must not block or call
// Publish other than in a new goroutine.
//
// A single worker drains the queue, so commands stay in the order they
// arrived: two rapid commands must not be applied backwards.
type commandQueue struct {
	ch     chan command
	logger *logger.Logger
}

func newCommandQueue(depth int, log *logger.Logger) *commandQueue {
	if depth < 1 {
		depth = 1
	}
	return &commandQueue{ch: make(chan command, depth), logger: log}
}

// enqueue accepts a command without blocking. It runs on paho's router
// goroutine, so a full queue is reported and dropped rather than waited on.
func (q *commandQueue) enqueue(topic, payload string) {
	select {
	case q.ch <- command{topic: topic, payload: payload}:
	default:
		q.logger.Error("Command queue full, dropping %s=%s", topic, payload)
	}
}

// run drains the queue until ctx is done, applying one command at a time.
func (q *commandQueue) run(ctx context.Context, handle func(context.Context, string, string) error) {
	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-q.ch:
			if ctx.Err() != nil {
				q.logger.Warn("Ignoring command on %s during shutdown", cmd.topic)
				return
			}
			q.logger.Debug("MQTT message on %s: %s", cmd.topic, cmd.payload)
			if err := handle(ctx, cmd.topic, cmd.payload); err != nil {
				q.logger.Error("Command on %s failed: %v", cmd.topic, err)
			}
		}
	}
}
