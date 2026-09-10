# Aerosmart Gateway — Timing Diagrams

Sequence and timing views of the flows described in
[Application Flow](APPLICATION_FLOW.md). Figures use the shipped defaults:
`read_timeout: 200`, `response_timeout: 300`, `retries: 3`, `retry_backoff: 100`,
`read_interval: 120`.

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
      ├─ publish discovery (retained) ─────────────────────▶│
      ├─ subscribe to command topics ──────────────────────▶│
      │
      ├─ first read cycle immediately
      └─ then every 120s, measured from the END of each cycle
```

## Frame reassembly

The property that keeps a lossy link from desyncing. The buffer survives across calls.

```
device sends:  "130 1067 3\r\n"

read #1 ──▶ "130 10"              buffer: "130 10"        no '\n' yet
read #2 ──▶ (VTIME expires, io.EOF)  buffer: "130 10"     RETAINED, reported as timeout
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
            ├─ reset software buffer
            ├─ port.Flush()                       TCIOFLUSH, ~0ms
 ~0ms       ├─ write "130 1067\r\n"               13 bytes, ~1ms at 115200
            │  deadline = t + 300ms
            │
            ├─ readFrame ──▶ "140 9999 1"         unsolicited: DISCARD, keep reading
            ├─ readFrame ──▶ "130 1067 3"         prefix matches: return "3"
 ~40ms      └─ release txMu
```

The command is written **once**. A non-matching frame is discarded rather than triggering a
re-send, which is what keeps writes idempotent.

Silent device:

```
 t=0    write, deadline = t + 300ms
        readFrame blocks in the kernel up to read_timeout (200ms), returns io.EOF
        readFrame again; deadline passes
 300ms  ErrReadTimeout  (any partial bytes stay buffered for the next attempt)

        A read that starts just before the deadline still blocks a full
        read_timeout, so one attempt can take up to 500ms.
```

## Periodic read cycle

Healthy link, 29 read registers:

```
 t          Poller              Reader             SerialPort        MQTT
 │
 0ms   ├─ health ok, gate ok ──▶│
       │                        ├─ register 1 ────▶│ ~40ms
       │                        │◀─ value ─────────┤
       │                        ├─ register 2 ────▶│ ~40ms
       │                        │ ...
 ~1.2s │                        │◀─ 29 registers done
       │◀─ values ──────────────┤
       ├─ publish valid values ──────────────────────────────────▶│
       ├─ derived values from the same snapshot ─────────────────▶│
       │
       └─ wait 120s from HERE, so the device always gets an idle gap
```

Invalid readings are skipped, not republished — with retained messages a stale value would
make a dead sensor look alive.

## Write with verification

```
 t       MQTT           Writer          ReadGate        SerialPort      MQTT
 │
 0ms  ──▶│ "3" on dw/aerosmart/luefterstufe
         ├─ resolve register
         ├─ parse + range-check         (BEFORE touching the read cycle)
         ├─ dedupe check
         ├─ Preempt ──────────────▶│    cancels any in-flight cycle
 ~1ms    ├─ "130 5002 3" ───────────────────────────▶│
         │◀─ echo "130 5002 3" ─────────────────────┤ ~40ms
 ~45ms   ├─ publish own state topic ──────────────────────────────▶│
         ├─ read luefterstatus ─────────────────────▶│ ~40ms
         ├─ publish verified value ──────────────────────────────▶│
 ~130ms  └─ Release (deferred: happens on EVERY exit path)
```

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
       │                  │  current Transact returns within one response timeout
 ~600ms│◀─ context.Canceled
       │  ctx.Err() == nil, so this is NOT shutdown
       │  wait skipDelay (5s), not the full 120s
       │                                           │ write completes, Release
 5.6s  ├─ RunCycle ──────▶│ full cycle runs
```

Two things this pins down: a preempted cycle must not end the poll loop, and it must be
retried in seconds rather than costing a whole interval of data.

## Retry and give-up

One unresponsive register:

```
 t=0     attempt 1 ──▶ write, then read until the 300ms deadline
                        a read starting at 299ms still blocks 200ms,
                        so one attempt costs up to 500ms
 600ms   attempt 2 ──▶ (after 100ms backoff) ──▶ ErrReadTimeout
 1.2s    attempt 3 ──▶ ──▶ ErrReadTimeout
 1.8s    give up: register marked invalid, nothing published, cycle continues
```

The budget check charges `response_timeout + read_timeout + retry_backoff` per attempt for
exactly that reason.

A dead port short-circuits, because the supervisor owns recovery:

```
 t=0     attempt 1 ──▶ EIO ──▶ mark unhealthy ──▶ stop immediately
```

Whole cycle against a silent device: 29 x 1.8s = 52.2s, inside the 120s interval. Startup
rejects a configuration where that does not hold.

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
       │  publishes now fail after op_timeout (5s) each, bounded,
       │  so the read loop is never wedged
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
          │                      │   returns within one response timeout
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
| One transaction (device answers) | ~40ms | `response_timeout` (300ms) |
| One register including retries | ~40ms | `retries x (response_timeout + read_timeout + retry_backoff)` = 1.8s |
| Full cycle, 29 registers, healthy | ~1.2s | — |
| Full cycle, silent device | — | 52.2s (must fit `read_interval`) |
| Write with verification | ~130ms | ~2.4s (command + one verify register) |
| Write preempting a cycle | ~300ms to yield | one `response_timeout` |
| Cycle skipped (write pending / port recovering) | immediate | retried after 5s |
| MQTT publish | ~1ms | `op_timeout` (hand-off) + `op_timeout` (ack), x `publish_retry_count` |
| Port reopen | ~5ms | backoff to 30s between attempts |
| Shutdown | ~360ms | 10s, then forced exit |
