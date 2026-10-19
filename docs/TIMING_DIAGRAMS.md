# Aerosmart Gateway — Timing Diagrams

Sequence and timing views of the flows described in
[Application Flow](APPLICATION_FLOW.md). Figures use the shipped defaults:
`read_timeout: 200`, `response_timeout: 300`, `bus_quiet_gap: 20`,
`bus_quiet_max_wait: 100`, `retries: 3`, `retry_backoff: 100`, `read_interval: 120`.

## Table of Contents

- [Startup](#startup)
- [Frame reassembly](#frame-reassembly)
- [One transaction](#one-transaction)
- [Periodic read cycle](#periodic-read-cycle)
- [Write with verification](#write-with-verification)
- [Write preempting a read cycle](#write-preempting-a-read-cycle)
- [Retry and give-up](#retry-and-give-up)
- [Port loss and recovery](#port-loss-and-recovery)
- [Broker loss and recovery](#broker-loss-and-recovery)
- [Shutdown](#shutdown)
- [Timing summary](#timing-summary)

## Startup

```
 t        main                    config      serial            mqtt
 │
 0ms  ├─ load + validate ────────▶│
      │  (rejects unusable options, checks the cycle budget)
      │◀─ ok ────────────────────┤
      │
      ├─ open port ──────────────────────────▶│
      │   retries with backoff 500ms, 1s, 2s... indefinitely
      │◀─ open ──────────────────────────────┤
      ├─ start Supervise ────────────────────▶│ (health poll every 1s)
      │
      ├─ connect ───────────────────────────────────────────▶│
      │   retries with backoff + jitter, 10 attempts
      │◀─ connected ────────────────────────────────────────┤
      │                                       │◀─ publish "online" (LWT armed)
      ├─ subscribe to command topics ──────────────────────▶│
      ├─ publish discovery (retained) ─────────────────────▶│
      │
      ├─ first read cycle immediately
      └─ then every 120s, measured from the END of each cycle
```

## Frame reassembly

The property that keeps a lossy link from desyncing. The buffer survives across calls.

```
device sends:  "130 1067 3\r\n"

read #1 ──▶ "130 10"              buffer: "130 10"        no '\n' yet
read #2 ──▶ (read times out, 0 bytes) buffer: "130 10"    RETAINED, reported as timeout
read #3 ──▶ "67 3\r\n"            buffer: "130 1067 3\r\n"
                                   ──▶ frame "130 1067 3", buffer emptied
```

Several frames in one burst are delivered one at a time:

```
read #1 ──▶ "130 1067 3\r\n130 5003 2\r\n"
   readFrame() ──▶ "130 1067 3"     buffer keeps "130 5003 2\r\n"
   readFrame() ──▶ "130 5003 2"     buffer empty
```

## One transaction

```
 t=0        Transact("130 1067")
            ├─ take txMu                          (serialises the wire)
            ├─ listen: panel bytes arrive ──▶     discard, keep listening
 ~20ms      ├─ 20ms with nothing on the line      bus is quiet (gives up waiting at 100ms)
            ├─ reset software buffer
            ├─ port.ResetInputBuffer()            TCIFLUSH: input only, ~0ms
            ├─ write "130 1067\r\n"               13 bytes, ~1ms at 115200
            ├─ port.Drain()                       returns once the bytes are out
            │  deadline = now + 300ms
            │
            ├─ readFrame ──▶ "140 9999 1"         unsolicited: DISCARD, keep reading
            ├─ readFrame ──▶ "130 1067 3 1"       not exactly 3 fields: DISCARD
            ├─ readFrame ──▶ "130 1067 3"         well-formed, prefix matches: return "3"
 ~60ms      └─ release txMu
```

The command is written **once** per transaction. A non-matching or malformed frame is
discarded rather than triggering a re-send.

Silent device:

```
 t=0    write, deadline = t + 300ms
        readFrame blocks up to read_timeout (200ms), returns 0 bytes
        readFrame again, blocking only the 100ms left until the deadline
 300ms  ErrReadTimeout  (any partial bytes stay buffered for the next attempt)

        Each read is cut short at the deadline, so an attempt never overruns
        response_timeout.
```

## Periodic read cycle

Healthy link, 29 read registers:

```
 t          Poller              Reader             SerialPort        MQTT
 │
 0ms   ├─ health ok, gate ok ──▶│
       │                        ├─ register 1 ────▶│ ~60ms   unchanged: trusted
       │                        │◀─ value ─────────┤
       │                        ├─ register 2 ────▶│ ~60ms   changed...
       │                        ├─ register 2 ────▶│ ~60ms   ...confirmed by a 2nd read
       │                        │ ...
 ~2-3s │                        │◀─ 29 registers done
       │◀─ values ──────────────┤
       ├─ publish valid values ──────────────────────────────────▶│
       ├─ republish write-register states from state_from ───────▶│
       ├─ derived values from the same snapshot ─────────────────▶│
       │
       └─ wait 120s from HERE, so the device always gets an idle gap
```

A reading is trusted on its own only when it is within `confirm_step` of the last trusted
value; a changed one costs a second read (and the very first cycle reads everything twice).
Invalid or unconfirmed readings are skipped, not republished — with retained messages a
stale value would make a dead sensor look alive.

## Write with verification

```
 t     paho router    queue     worker/Writer   ReadGate    SerialPort   MQTT
 │
 0ms ──▶│ "3" on dw/aerosmart/luefterstufe
        ├─ enqueue ──▶│         (never blocks; the router must keep reading
        └─ returns    │          packets, including our own PUBACKs)
                      └──────▶│
         ├─ resolve register
         ├─ parse + range-check         (BEFORE touching the read cycle)
         ├─ dedupe check                 (on the parsed value)
         ├─ Preempt ──────────────▶│    cancels any in-flight cycle
         ├─ bump write sequence          (supersedes an earlier settle re-read)
 ~20ms   ├─ "130 5002 3" ───────────────────────────▶│  after a quiet bus; no echo awaited
         ├─ remember as sent             (dedupe compares against what went out)
 ~70ms   ├─ wait write_settle_delay
         ├─ read lueftermode ───────────────────────▶│ ~120ms  state_from, changed: 2 reads
         ├─   publish it to its own topic ──────────────────────────▶│
         ├─ read luefterstatus ─────────────────────▶│ ~60ms   verify_registers
         ├─   publish it to its own topic ──────────────────────────▶│
         │                                  (each read-back register read ONCE)
         │  disagrees or unreadable? ── yes ──▶ send once more, read back again
 ~250ms  ├─ publish lueftermode's value to the state topic ──────▶│
         │                                  (unreadable state_from: publish nothing)
         │  still refused or unconfirmed? ──▶ forget it for dedupe, report the error
         └─ Release (deferred: happens on EVERY exit path)
         ┆
         ┆  schedule(register, seq)                 write_resettle_delay
         ┆
 ~5.1s   ├─ still the newest command for this register? ── no ──▶ skip
         ├─ Preempt, re-read lueftermode + luefterstatus ──▶│
         └─ republish ──────────────────────────────────────────▶│
```

Nothing waits for an echo: the unit applies a write whether or not it confirms it, so the
read-back of `state_from` and `verify_registers` is what makes a write trustworthy. The
command is re-sent exactly once, and only when that read-back disputes it: a collision on
the shared bus can corrupt a command on its way to the unit. The settle re-read is
scheduled whatever the outcome. The value published to the state topic is `state_from`'s — what
the unit actually holds — not the value that was asked for, so a rejected command shows up
as the unit's own value rather than a lie.

The second pass exists because some values trail the command: 60 ms after a stage change,
`luefterstatus` still reported the previous stage. A further command for the same register
bumps the sequence and cancels the pending re-read, so a burst of changes costs one extra
read after it stops rather than one per command.

A malformed payload (`"ON"`, out of range, unknown topic) returns before `Preempt`, so it
never disturbs a readout.

## Write preempting a read cycle

```
 t         pollLoop        Poller/Reader        ReadGate       Writer
 │
 0ms   ├─ RunCycle ──────▶│ reading register 7...
       │                  │                        │
 300ms │                  │                   ◀────┤ Preempt (command arrives)
       │                  │◀─ cycleCtx cancelled
       │                  │  current Transact returns at its next read,
       │                  │  at most read_timeout (200ms) later
 ≤500ms│◀─ context.Canceled
       │  ctx.Err() == nil, so this is NOT shutdown
       │  wait skipDelay (5s), not the full 120s
       │                                           │ write completes, Release
 ~5.5s ├─ RunCycle ──────▶│ full cycle runs
```

Two things this pins down: a preempted cycle must not end the poll loop, and it must be
retried in seconds rather than costing a whole interval of data.

## Retry and give-up

One unresponsive register:

```
 t=0     attempt 1 ──▶ wait up to 100ms for a quiet bus, write,
                        read until the 300ms deadline ──▶ ErrReadTimeout
 ≤550ms  attempt 2 ──▶ (after a 100-150ms jittered backoff) ──▶ ErrReadTimeout
 ≤1.1s   attempt 3 ──▶ ──▶ ErrReadTimeout
 ≤1.5s   give up: register marked invalid, nothing published, cycle continues
```

The budget check charges `max(bus_quiet_gap, bus_quiet_max_wait) + response_timeout + 1.5 x
retry_backoff` per attempt, and at least two attempts, since confirming a changed reading
needs a second one: 3 x 550ms = 1.65s per register. That is slightly pessimistic, since no
backoff follows the last attempt, so a real give-up comes at about 1.5s. Confirmation shares
these attempts with retries, so it never lengthens the worst case.

A dead port short-circuits, because the supervisor owns recovery:

```
 t=0     attempt 1 ──▶ EIO ──▶ mark unhealthy ──▶ stop immediately
```

Whole cycle against a silent device: about 29 x 1.5s = 43.5s, which the startup check
charges as 29 x 1.65s = 47.9s, inside the 120s interval (and the 60s default). Startup
rejects a configuration where that budget does not fit.

## Port loss and recovery

```
 t          Transact            SerialPort           Supervise
 │
 0s    ├─ read ──────────────▶│ EIO (adapter unplugged)
       │                       ├─ healthy = false
       │◀─ error ──────────────┤
       │
       │  cycles now return ErrCycleSkipped and retry in 5s
       │  rather than grinding 29 registers against a dead port
       │
 1s    │                       │◀── health poll: unhealthy ──┤
       │                       │◀── reopen (fails) ──────────┤ wait 500ms
       │                       │◀── reopen (fails) ──────────┤ wait 1s
       │                       │◀── reopen (fails) ──────────┤ wait 2s ... up to 30s
       │
 Ns    │                       │◀── reopen succeeds ─────────┤ still waits the backoff
       │                       ├─ healthy = true              before another attempt
       │
       │  cycles resume
```

The backoff is applied after a *successful* reopen too. A port that opens cleanly but keeps
failing I/O would otherwise be reopened once per health poll, toggling DTR/RTS — which many
devices treat as a reset.

## Broker loss and recovery

```
 t         Gateway                    paho                   Broker
 │
 0s    ├─ publish ──────────────────▶│ ──────────────────────▶│
       │                              │                        X network drops
 ≤30s  │                              │ KeepAlive detects it
       │                              ├─ ConnectionLost logged
       │                              │  LWT: broker publishes "offline"
       │  until paho notices, each publish waits op_timeout (5s) per
       │  attempt, up to publish_retry_count (3) attempts: ~16s, or
       │  ~31s if the socket write stalls too;
       │  once it has, publishes fail at once, so the read loop is
       │  slowed, never wedged
       │
       │                              ├─ reconnect attempts, backing off to 60s
 Ns    │                              │◀───────── reconnected ─┤
       │                              ├─ OnConnect (off the callback goroutine):
       │                              │    publish "online"
       │                              │    re-subscribe every tracked topic,
       │                              │    continuing past individual failures
```

A clean session is used, so the broker does **not** queue commands while the gateway is
offline. Without that, an hours-old fan command would be replayed at startup.

## Shutdown

```
 t         signal handler        main               MQTT          Serial
 │
 0ms   ──▶│ SIGTERM
          ├─ cancel root ctx ──▶│
          ├─ arm watchdog (10s; a second signal exits at once)
          │                      ├─ Disconnect ───▶│ publish "offline"
          │                      │                  │ (ONE bounded attempt)
          │                      │                  │ retries disabled from here
 ~50ms    │                      ├─ wait for loops
          │                      │   in-flight read sees the cancellation and
          │                      │   returns at its next read (≤ read_timeout)
 ~350ms   │                      ├─ Close ────────────────────────▶│
          │                      │   takes txMu, so a write already on the wire
          │                      │   finishes before the device is released
          │                      ├─ close the log file
 ~360ms   │◀─ shutdownDone ──────┤
          └─ watchdog stood down
```

Completion is signalled *after* teardown, so the watchdog covers the real work.

## Timing summary

| Operation | Typical | Worst case |
|-----------|---------|-----------|
| One transaction (device answers) | ~60ms | `max(bus_quiet_gap, bus_quiet_max_wait) + response_timeout` (400ms) |
| One register including retries | ~60ms, ~120ms if changed | ~1.5s; budgeted as `max(retries, 2) x (max(bus_quiet_gap, bus_quiet_max_wait) + response_timeout + 1.5 x retry_backoff)` = 1.65s |
| Full cycle, 29 registers, healthy | ~2-3s | — |
| Full cycle, silent device | — | ~43.5s; budgeted as 47.9s, which must fit `read_interval` |
| Write with read-back | ~250ms | two sends, each followed by one register per read-back, each up to 1.65s |
| Settle re-read after a write | ~120ms, `write_resettle_delay` later | restarted by a further command for the same register |
| Write preempting a cycle | well under 200ms to yield | one `read_timeout` (200ms) |
| Cycle skipped (write pending / port recovering) | immediate | retried after 5s |
| MQTT publish | ~1ms | `op_timeout` (hand-off) + `op_timeout` (ack), x `publish_retry_count` |
| Port reopen | ~5ms | backoff to 30s between attempts |
| Shutdown | ~360ms | 10s, then forced exit |
