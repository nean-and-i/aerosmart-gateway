# MQTT Message Delay Fix - Technical Documentation

This document explains the MQTT message delay issue that affected the Aerosmart
Gateway and the **write-priority mechanism** that resolves it. The mechanism
described here reflects the current implementation in
[`internal/registers/reader.go`](../internal/registers/reader.go) and
[`internal/serial/serial.go`](../internal/serial/serial.go).

## Table of Contents

1. [Issue Description](#issue-description)
2. [Root Cause Analysis](#root-cause-analysis)
3. [The Write-Priority Mechanism](#the-write-priority-mechanism)
4. [Implementation Details](#implementation-details)
5. [Performance](#performance)
6. [Testing and Verification](#testing-and-verification)
7. [Configuration](#configuration)

---

## Issue Description

### Problem Statement

The gateway used to experience significant delays in detecting and processing
MQTT messages for write register topics:
- `dw/aerosmart/luefterstufe` - Fan stage control (0-5)
- `dw/aerosmart/boilerheizstab` - Boiler heating element control (0-1)

### Impact

- Control commands from Home Assistant were not executed in a timely manner.
- Users experienced lag when adjusting fan speed or boiler settings.
- A write could be delayed until the next periodic read cycle completed.

### Symptoms

1. MQTT message published to a write topic.
2. Message not processed immediately — delays of tens of seconds observed.
3. The write executed only after the in-progress periodic read finished.

---

## Root Cause Analysis

### Root Cause 1: race in the original channel-based write-priority signal

The **original** implementation (since replaced) signalled write priority over a
channel and checked it from `TriggerFullReadout()` with a `select`/`default`.
When an MQTT message and the periodic ticker fired nearly simultaneously, the
non-blocking `select` could miss the signal and proceed with the read, so the
write only ran after that read completed.

### Root Cause 2: unnecessary serial port reopen on the first attempt

`SendAndReceive` used to call `ForceReopen()` on **every first** write attempt,
adding a close/reopen (~5 ms plus device settling) and disrupting communication
on every command.

---

## The Write-Priority Mechanism

The current design replaces the channel with a **timestamp guarded by a mutex**,
plus an automatic expiry so a failed write can never block reads indefinitely.

### State ([`Writer`](../internal/registers/reader.go))

```go
type Writer struct {
    // Write priority mechanism - auto-expires after writePriorityTimeout
    writePriorityTime    time.Time
    writePriorityTimeout time.Duration // 10s (set in NewWriter)
    writePriorityMu      sync.Mutex
    // ...
}
```

### Signalling priority

`SignalWritePriority()` simply records the current time:

```go
func (w *Writer) SignalWritePriority() {
    w.writePriorityMu.Lock()
    w.writePriorityTime = time.Now()
    w.writePriorityMu.Unlock()
    w.logger.Debug("Write priority signaled")
}
```

### Detecting priority (with auto-expiry)

`isWritePriorityActive()` reports whether a write is pending and **clears stale
priority** once `writePriorityTimeout` (10 s) has elapsed — this guards against a
write that failed without clearing priority:

```go
func (w *Writer) isWritePriorityActive() bool {
    w.writePriorityMu.Lock()
    defer w.writePriorityMu.Unlock()
    if w.writePriorityTime.IsZero() {
        return false
    }
    if time.Since(w.writePriorityTime) >= w.writePriorityTimeout {
        w.writePriorityTime = time.Time{}
        w.logger.Warn("Write priority expired after %v timeout", w.writePriorityTimeout)
        return false
    }
    return true
}
```

`TriggerFullReadout()` checks it and skips the periodic read when a write is
pending:

```go
func (w *Writer) TriggerFullReadout() error {
    if w.isWritePriorityActive() {
        w.logger.Info("=== Write priority detected, skipping periodic read ===")
        w.reader.Cancel()
        w.reader.ResetContext()
        return nil
    }
    // ... read all registers, publish, then compute/publish derived registers
}
```

### Preemption is cooperative, not interrupt-based

When a write arrives, `HandleMessage()` calls `reader.Cancel()` to cancel the
read context. The read loop (`Reader.ReadAllWithContext`) checks `ctx.Done()`
**between registers**, and all serial I/O is serialized by the serial layer's
`writeMu`. So an in-flight single-register read runs to completion, the write
then acquires `writeMu`, and the remaining registers in that cycle are skipped.
Detection is immediate; the write completes once the current register read
finishes (typically within a couple of seconds).

### Clearing priority

On a **successful** write, `HandleMessage()` calls `ClearWritePriority()` (resets
the timestamp to zero) so reads resume immediately. On a **failed** write,
`HandleMessage()` logs the error, returns `nil`, and deliberately leaves priority
set — it auto-expires via the 10 s timeout above, so reads resume even if the
serial device is unresponsive.

### Supporting fixes

- **Message deduplication** — `HandleMessage()` ignores an identical
  topic+value seen within a 1 s window (`isDuplicateMessage`), preventing
  duplicate retained/republished messages from triggering redundant writes.
- **Timing metrics** — receive → process → complete timestamps yield a logged
  end-to-end latency for monitoring.
- **ForceReopen optimization** — `SendAndReceive` now reopens the port only on
  retry attempts (`attempt > 0`), not on the first attempt:

  ```go
  if attempt > 0 {
      _ = s.ForceReopen()
  }
  ```

---

## Implementation Details

### File: [`internal/registers/reader.go`](../internal/registers/reader.go)

**`Writer` fields:** `writePriorityTime`, `writePriorityTimeout`,
`writePriorityMu`; `recentMessages` / `messageDedupeWindow` for dedup; and the
message-timing metric fields.

**Key methods:** `SignalWritePriority()`, `ClearWritePriority()`,
`isWritePriorityActive()`, `isDuplicateMessage()`, `cleanOldMessages()`, the
`updateLastMessage*Time()` metric helpers, and `GetMessageMetrics()`.
`HandleMessage()` runs the dedup check, signals priority, cancels the read, writes
+ verifies, and clears priority on success. `TriggerFullReadout()` skips the read
when priority is active.

### File: [`internal/serial/serial.go`](../internal/serial/serial.go)

`SendAndReceive` serializes all device I/O under `writeMu` and force-reopens the
port only on retry attempts.

### Key log messages

| Log Message | Meaning |
|-------------|---------|
| `Write priority signaled` | MQTT message received, priority timestamp set |
| `=== Write priority detected, skipping periodic read ===` | Periodic read skipped due to a pending write |
| `Write priority expired after 10s timeout` | Priority auto-cleared after the timeout (e.g. after a failed write) |
| `MQTT: Message processing latency: X.XXs ...` | End-to-end processing time |
| `MQTT: Skipping duplicate message ...` | Deduplication prevented redundant processing |

---

## Performance

| Metric | Before | After |
|--------|--------|-------|
| MQTT message detection | delayed until next read cycle | immediate (<1 s) |
| Write completion | tens of seconds | ~1-3 s |
| ForceReopen on writes | every first attempt | only on retry attempts |

> The "before/after" figures are illustrative; actual read-cycle and write
> durations depend on the serial config (`device_response_delay`,
> `read_max_retries`, register count) and are reported in the logs per cycle.

---

## Testing and Verification

1. Start the application (debug logging helps):
   ```bash
   ./aerosmart-gateway -config config.yaml -registers registers.yaml
   ```
2. Publish to a write topic:
   ```bash
   mosquitto_pub -t 'dw/aerosmart/luefterstufe' -m '2' -q 1
   ```
3. Expect logs similar to:
   ```
   Received MQTT message on dw/aerosmart/luefterstufe: 2
   Write priority signaled
   === Writing to device ===
   SERIAL WRITE: cmd="130 5002 2" response="..."
   Successfully wrote luefterstufe = 2 to device
   === Write operation completed ===
   MQTT: Message processing latency: 2.18s (receive -> process -> complete)
   ```

---

## Configuration

The mechanism needs no configuration; the two tunables are currently **hardcoded
constants** set in `NewWriter`:

| Setting | Value | Where |
|---------|-------|-------|
| Message deduplication window | 1 second | `messageDedupeWindow` in `NewWriter` |
| Write priority timeout | 10 seconds | `writePriorityTimeout` in `NewWriter` |

Exposing these via `config.yaml` is a possible future enhancement; today they are
compile-time constants.

---

## Related Documentation

- [Application Flow Analysis](APPLICATION_FLOW.md)
- [Timing Diagrams](TIMING_DIAGRAMS.md)
- [README.md](../README.md)
