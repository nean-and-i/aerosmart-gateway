# Aerosmart Gateway — Application Flow

How the gateway is put together, and why the serial layer is shaped the way it is.

## Table of Contents

- [Design premise](#design-premise)
- [Architecture](#architecture)
- [Startup](#startup)
- [Serial layer](#serial-layer)
- [Register reads](#register-reads)
- [Register writes](#register-writes)
- [Write priority](#write-priority)
- [MQTT](#mqtt)
- [Home Assistant discovery](#home-assistant-discovery)
- [Failure handling](#failure-handling)
- [Shutdown](#shutdown)
- [Configuration reference](#configuration-reference)

## Design premise

The serial link is assumed to be unreliable. Three consequences drive the design:

1. **Frames arrive split across reads.** A USB serial adapter delivers whatever it has when
   its latency timer fires, so `"130 1067 3\r\n"` can arrive as `"130 10"` then `"67 3\r\n"`.
   A reader that discards the first part leaves the tail to arrive alone, and every later
   frame is then one behind — permanently. Received bytes are therefore buffered
   persistently, across read calls.

2. **Frames on the wire may not be the answer to your question.** The device can have a
   stale reply queued, or emit something unsolicited. A transaction reads until it sees the
   frame matching its own command and discards the rest. It never re-sends the command to
   resynchronise, because re-sending a write command re-applies it.

3. **Reopening the port is destructive.** Closing and reopening a tty resets termios and,
   on most USB bridges, toggles DTR/RTS — which many devices treat as a reset. Reopening is
   reserved for genuine port errors and performed by one supervisor, never as a remedy for
   a late reply.

## Architecture

```
                   ┌──────────────┐
                   │  MQTT broker │
                   └──────┬───────┘
                          │ commands / state / discovery
                   ┌──────┴───────┐
                   │ internal/mqtt│  Client (bounded ops, LWT)
                   │              │  Discovery (HA payloads)
                   └──────┬───────┘
                          │ Publisher interface
   ┌──────────────────────┴───────────────────────┐
   │            internal/registers                │
   │                                              │
   │  Poller ── RunCycle ──▶ Reader ──▶ Publisher │
   │     │                     │                  │
   │  ReadGate ◀── Preempt ── Writer              │
   │                           │                  │
   │              DerivedCalculator               │
   └──────────────────────┬───────────────────────┘
                          │ Transactor interface
                   ┌──────┴───────┐
                   │internal/serial│ SerialPort (transactions, health)
                   │               │ frameReader  (persistent buffer)
                   └──────┬───────┘
                          │ Port interface
                   ┌──────┴───────┐
                   │ Aerosmart unit│
                   └──────────────┘
```

Packages depend on interfaces, not concrete types: `registers` needs a `Transactor` and a
`Publisher`, and `serial` needs a `Port`. That is what lets the whole stack be tested
against a scripted device (see `internal/integration`).

| Package | Responsibility |
|---------|----------------|
| `internal/serial` | Framing, one command/response transaction at a time, port health, supervised reopen |
| `internal/registers` | Read/publish cycles, command handling, write priority, derived values |
| `internal/mqtt` | Broker connection with bounded operations, availability, HA discovery payloads |
| `internal/config` | Loading, defaults, and startup validation of both YAML files |
| `internal/logger` | Levelled logging to console and/or file |
| `cmd` | Wiring, signal handling, the poll loop, the command worker |

## Startup

1. Load `config.yaml`, apply defaults, and validate. Only configurations that cannot work
   are refused: an invalid parity, byte size, stop bits or QoS, `retries` below 1, a
   non-positive `read_interval`, a negative timeout, a backoff maximum below its initial
   value, a jitter percentage outside 0-100, no MQTT broker, no log sink.
2. Load `registers.yaml` and validate: duplicate names, missing commands or topics, an
   inverted or all-zero range, a negative `confirm_step`, command templates without
   `{value}`, two write registers on one `subscribe_topic`, and `verify_registers`,
   `state_from` or derived `sources` naming read registers that do not exist.
3. Check the retry budget: the worst-case full cycle
   (`max(retries, 2) x (max(bus_quiet_gap, bus_quiet_max_wait) + response_timeout + 1.5 x retry_backoff) x read
   register count`) must fit inside `read_interval`. Reads are cut short at the response
   deadline, so `read_timeout` adds nothing; the backoff is jittered by up to half again;
   and confirming a changed reading needs a second attempt, so there are always at least
   two.
4. Check that every configured derived register has an implementation and lists every
   source its formula reads.
5. Report anything that works but should be fixed as a warning — settings the driver cannot
   honour (`xonxoff`, `dsrdtr`), options removed with the serial rework, and write registers
   sharing a read register's state topic. These are warnings rather than errors because each
   was equally inert in earlier releases, so refusing to start would break an upgrade
   without making anything safer.
6. Open the serial port, retrying with backoff indefinitely — on an embedded gateway the
   USB adapter may appear after boot, so exiting would only crash-loop under systemd.
7. Start the serial supervisor.
8. Connect to MQTT with backoff and jitter, subscribe to command topics, then publish
   discovery. Subscribing first matters: with a clean session the broker holds nothing for
   the gateway, so a command sent between an entity appearing and the subscription would be
   lost.
9. Run the first read cycle, then enter the poll loop.

`-validate` performs steps 1-5 and exits, so a configuration can be checked without
touching hardware.

## Serial layer

### Framing

`frameReader` owns a persistent buffer:

```
readFrame(deadline):
  loop:
    if buffer contains '\n':
        return the line, KEEP the remainder buffered
    if buffer > maxFrameLen:
        drop the buffer, report ErrFrameOverflow      # resync
    wait = min(read_timeout, time left until deadline)
    if wait <= 0:
        return ErrReadTimeout                          # partial data stays buffered
    n, err = port.Read(scratch) blocking at most wait  # never overruns the deadline
    if n > 0:
        append to buffer; continue                     # consume before judging err
    if err is a real I/O error:
        return err
```

Two details matter:

- The scan is forward and keeps the remainder, so a burst carrying several frames yields
  them one at a time instead of only the last.
- `go.bug.st/serial` reports a read timeout as zero bytes and no error; other transports
  report it as `io.EOF` or a deadline. That is a timeout, not a broken port — it is
  classified by `IsTimeout`, not `IsPortError`. The driver's own `PortError` does not wrap
  an errno, so `IsPortError` recognises its closed / not-found / invalid-port codes
  explicitly.

### Transactions

```
Transact(ctx, command):
  hold txMu                       # one exchange on the wire at a time
  read and discard until the bus has been silent for bus_quiet_gap,
      or bus_quiet_max_wait has passed   # the panel shares the bus
  reset the software buffer
  port.ResetInputBuffer()         # TCIFLUSH: discard stale input only
  write command + "\r\n"
  port.Drain()                    # wait until it has actually been transmitted;
                                  # resumed if a signal interrupts it (EINTR)
  deadline = now + response_timeout
  loop:
    check ctx                     # a write is waiting
    frame = readFrame(deadline)
    if frame is exactly "<node> <register> <integer>" for this command:
        return its value
    discard the frame and keep reading
```

`Send`, used for register writes, does the same up to and including the drain.

The unit's control panel talks on the same bus and does not coordinate with the gateway, and
the protocol has no checksum. Three details follow from that:

- **Listen before talking.** Transmitting into one of the panel's frames corrupts both. The
  bytes discarded while waiting cannot be a reply to anything of ours: the previous
  exchange has finished and this command has not been sent yet. The wait is bounded, so a
  device that never pauses delays commands rather than blocking them.
- **Only input is discarded.** The previous driver's `Flush` was `TCIOFLUSH`, which also
  threw away output not yet transmitted — so a transaction starting right after `Send`
  could delete the write `Send` had just reported as sent. Draining makes "sent" mean sent.
  The drain is a blocking `tcdrain`, and the Go runtime signals its own threads, so it can
  return `EINTR` while the command is still leaving the port. That is resumed rather than
  treated as a failure: the command has been written, and failing the exchange would waste
  a read attempt or send a write twice.
- **Strict frames.** A collision can splice two frames (`130 1067 3 130 5003 4`) or corrupt
  a byte inside one while the prefix still matches. Anything but exactly three fields with
  an integer value is rejected. Non-printable line noise around a frame is trimmed, since
  it cannot change the value; inside a frame it rejects it.

### Port health

A port-level error (`EIO`, `ENODEV`, `ENXIO`, `EBADF`, closed, missing) marks the port
unhealthy. A timeout never does. `Supervise` is the only thing that reopens:

```
loop:
  if healthy:
      sleep health_check_interval
      reset the backoff only once the port has stayed healthy a while
      continue
  attempt a reopen
  sleep the backoff, whether or not it succeeded, then double it up to the maximum
```

Waiting after a *successful* reopen matters: a port that opens cleanly but keeps failing
I/O would otherwise be reopened once per health check, toggling DTR/RTS each time.

## Register reads

```
Poller.RunCycle(ctx):
  if the port is unhealthy         -> ErrCycleSkipped
  if a write is in flight          -> ErrCycleSkipped     # checked atomically with the start
  cycleCtx = gate.TryBeginCycle(ctx)
  values = Reader.ReadAll(cycleCtx)
  if cancelled                     -> return, publishing nothing
  Reader.PublishAll(values)        # valid values only
  DerivedCalculator on the same snapshot, then publish
```

`ReadSingle` is the only retry level, and it also confirms readings:

```
for attempt in 1..max(retries, 2):
    check ctx; back off (jittered by up to half again) between attempts
    value = Transact(ctx, register.command)
    on context cancellation: return
    on a port error: stop -- the supervisor owns recovery
    on another error: next attempt
    apply divisor, format by type, validate range
    out of range: next attempt                     # most likely a corrupted frame
    within confirm_step of the last trusted value: trust it, return
    within confirm_step of an earlier reading in this call: trust it, return
    otherwise keep it and read again
readings that never agreed: ErrUnconfirmed (the register is invalid this cycle)
```

A corrupted reply can still parse as a plausible number — a dropped digit turns 21.5 degrees
into 2.15 — and passes a wide range check. So a reading is only trusted on its own when it
is within `confirm_step` (default `0`) of the last trusted value; any other change needs a
second read that agrees with it to within `confirm_step`. A measured value that drifts
between reads 60 ms apart — a fan speed, CO2 — needs a step for that, or its readings rarely
agree and it goes unpublished; the shipped `registers.yaml` sets one for each. Failures, confirmations and tie-breaks share one budget of
attempts, so confirming never lengthens the worst case. Out-of-range readings are never
clamped: `luefterstatus` used to be, which turned a corrupted frame into a confident
"highest stage".

A register that cannot be read is recorded as invalid and **not published**. With retained
MQTT messages, republishing the last good value would make a dead sensor look alive;
`expire_after` in the discovery payload lets Home Assistant mark it unavailable instead.

A cancelled cycle publishes nothing at all, so a partial readout cannot mix fresh values
with stale ones, and derived values are never computed from sources that were not read.
Cancellation is checked before publishing starts and again between every published value,
because the publish phase is 30+ broker round trips bounded only by `op_timeout`: a write
landing partway through would otherwise have its fresher values overwritten by the rest of
the snapshot, including the write-register state topics republished at the end.

## Register writes

Commands are applied by a dedicated worker, not on paho's router goroutine.
paho delivers subscription callbacks in order by default, and a callback that
blocks stops inbound packets being read — including the PUBACKs for the
handler's own publishes. Applying a command takes serial I/O and publishes
several topics, so running it inline made those publishes time out and stalled
every command queued behind it. A single worker drains a bounded queue, which
keeps commands in arrival order; if the queue is full the command is dropped
with an error rather than blocking the router.

```
HandleMessage(ctx, topic, payload):
  resolve the register from the topic          -> unknown: log and return
  parse the payload as an integer              -> invalid: return an error
  range-check it                               -> out of range: return an error
  drop it if identical to the last accepted command for this topic within 1s
                                               # compared as parsed: "03" == "3"
  gate.Preempt(); defer gate.Release()         # released on EVERY exit path
  defer: schedule one more read after write_resettle_delay
                                               # whatever the outcome
  up to twice:
      send the command                         # the parsed value; retried on a write error
      wait write_settle_delay
      read state_from                          # what the unit actually holds
      read back verify_registers               # each confirmed like any other read
      stop if the unit holds what was asked
  publish state_from's value as the register's state, plus the verified values
      (state_from configured but unreadable: publish nothing)
  report an error if the unit still holds something else, or nothing could be read
```

A disagreeing or missing read-back earns exactly one re-send. On the shared bus a collision
can corrupt the command on its way to the unit, and since a write sets an absolute value,
sending it again is harmless. A unit that genuinely refuses the value is reported after the
second attempt, with the value it actually holds published.

Retained messages on command topics are ignored. The broker hands a retained message to
every new subscription, and the gateway re-subscribes after every reconnect, so a command
published with the retain flag would otherwise be applied again after each connection
blip.

The command is remembered for deduplication once it reaches the wire, and
forgotten again if the write is refused or cannot be confirmed. Deduplication is
for broker redeliveries of a command that worked: a user retrying a value the
unit refused must reach the wire, and so must a correction back to an earlier
value, which only ever compares against the most recent command.

### Why a write is not confirmed by its echo

The unit applies register writes but does not reliably answer them. Measured on
a real gateway, 4 of 6 writes produced no frame matching the command within
three 300ms windows — while the bus carried the resulting status change, so the
write had plainly landed. One `boilerheizstab = 1` reported three failed
attempts and yet `140 1038 1`, the heating-element request status, appeared on
the wire.

Waiting for an echo therefore failed writes that had succeeded, and each retry
re-applied a setting that was already in effect. A write is confirmed by reading
`state_from` and `verify_registers` back instead, which the unit answers
reliably (measured 14-48ms, every read in a 58-read sample), and is re-sent only
when that read-back disputes it.

A write register with no `state_from` or `verify_registers` cannot be confirmed;
the command is still sent and a warning says so.

### Values that trail the command

`luefterstufe` is a mode, not a fan speed:

| Value | Meaning | What `luefterstatus` (`130 1067`) does |
|-------|---------|----------------------------------------|
| 0-3 | manual stage | mirrors the commanded stage |
| 4 | auto | follows the `co2luefterstufe*` thresholds |
| 5 | party | goes to the highest stage (3) for ~90 min, then reverts |

For 4 and 5 the running stage is therefore decided by the unit, not by the
command, and it is not known at the moment the write returns. Even for a manual
stage the value trails: 60ms after a change, `130 1067` still returned the
previous stage, so the read-back published a number the unit was already moving
away from. Home Assistant then held that stale value until the next full read
cycle.

So the read-back registers are read once more after `write_resettle_delay`
(5s by default), which is what gives `aerosmart/luefterstatus` the stage the
unit actually settled on. Setting it to `0` switches the second read off. Each
write register keeps its own pending re-read, and a re-read publishes nothing
if a newer command for that register has run since it was scheduled -
`readBack` releases the wire between reads, so a whole command can complete
inside one, and publishing the older values would put the superseded mode back
on the state topic.

The re-read holds the read gate for its duration, exactly as a write does. It
runs on its own goroutine, so without that a read cycle could start after the
write, sample the register before it settled, and publish that stale value over
the re-read's fresher one. Because the gate then has two independent holders,
suppression is reference-counted: whichever finishes first releases only its
own hold, and the `write_priority_timeout` deadline lifts suppression even
while holds are outstanding, so a lost release cannot wedge polling.

A command that never reaches the wire schedules a re-read too. Accepting a
command bumps the register's write sequence before the send, which supersedes
any re-read the previous command scheduled; returning on a send failure without
replacing it would drop that earlier command's lagging value until the next
full read cycle. A partial write may also have reached the unit, so reading the
register again is the only way to learn what it is holding.

A mode the unit leaves by itself - party reverting after ~90 minutes, or a
change made at its own panel - is not a command, so nothing schedules a
re-read. Every read cycle therefore republishes each write register's state
topic from its `state_from` register, which is also what restores those topics
after a gateway restart. The wait restarts on every write, so changing stage several
times in a row produces one read after the changes stop rather than one per
command, and scheduling it never blocks the write path.

### state_from

The unit reports a setpoint on a different address than the one it is written
to: `luefterstufe` is written on `130 5002` and read back on `130 5003`
(`lueftermode`), which was measured updating within ~30ms of the write. Naming
that register in `state_from` means the write register's state topic carries
what the unit actually did. Without it the requested value is published, which
diverges silently whenever the unit declines a value.

Validation happens **before** the read cycle is disturbed, so a malformed payload cannot
cancel a readout. Everything after `Preempt` releases the gate on the way out; leaving it
set used to block reads until the suppression timeout expired.

Deduplication remembers only the *last* command accepted per topic. Keying on every recent
`(topic, value)` pair meant a real `3 -> 0 -> 3` sequence inside one second left the device
on `0`, because the final command looked like a redelivery. A command that failed to send,
was refused, or could not be confirmed is not remembered, so it stays retryable.

## Write priority

`ReadGate` coordinates the two:

| Method | Effect |
|--------|--------|
| `TryBeginCycle(parent)` | Starts a cycle unless a write is pending. Checking and starting are one atomic step; done separately, a write arriving in between would preempt a cycle that had not begun and then let it run unimpeded. |
| `Preempt()` | Cancels the in-flight cycle and suppresses further ones. |
| `Release()` | Gives up one hold; suppression lifts once the last hold is released. |
| `Suppressed()` | Whether reads are held off; expired suppression clears itself. |

Suppression always carries a deadline (`write_priority_timeout`), so a release that never
happens cannot wedge polling. A finished cycle deregisters its own cancel via a generation
counter, so a later `Preempt` cannot cancel a cycle that already ended.

`Transact` checks the context before and between frame reads, so a cancelled read stops
within one read (at most `read_timeout`, and never past its response deadline) rather than
running on through the cycle.

## MQTT

- **Every operation is bounded**, on both sides of the call. Publishes and subscribes use
  `WaitTimeout(op_timeout)` for the acknowledgement, and `SetWriteTimeout(op_timeout)` for
  the hand-off. Both are needed: paho's outgoing channel is unbuffered and, without a write
  timeout, `Publish` itself blocks for a hardcoded 30s before any token is waited on, with
  no deadline on the socket write. Publishing happens on the serial read path, so an
  unbounded wait would stall polling and overrun the shutdown grace period.
- **Clean session.** A persistent session let the broker queue commands while the gateway
  was offline and replay them on reconnect — an hours-old fan command applied at startup.
  Subscriptions are restored by the connect handler instead.
- **paho's connect-retry is off.** With it enabled, `Connect()`'s token never completes
  while the broker is down, which makes the caller's own backoff and cancellation
  unreachable.
- **Recovery runs off the callback goroutine** and continues past individual failures, so
  one bad topic cannot leave the rest unsubscribed.
- **Availability.** `online` is published on connect; `offline` is registered as the last
  will and published on a clean shutdown, with a single bounded attempt so shutdown stays
  inside its grace period.

## Home Assistant discovery

`mqtt.Discovery` builds the payloads. Entity type follows the register's range:

| Register range | Entity | Notes |
|----------------|--------|-------|
| `0..1` | `switch` | With explicit `payload_on`/`payload_off` and `state_on`/`state_off` of `"1"`/`"0"`. Home Assistant's defaults are `"ON"`/`"OFF"`, which the device rejects. |
| anything wider | `number` | With `min`, `max`, `step` and `mode: box`. |
| read + derived | `sensor` | With `availability_topic` and `expire_after`. |

The command topic is always the writer's `subscribe_topic`, so the topic Home Assistant is
told to publish to cannot drift from the one the gateway listens on.

## Failure handling

| Failure | Response |
|---------|----------|
| Frame split across reads | Buffered and reassembled |
| Stale or unsolicited frame | Discarded; reading continues within the response budget |
| Panel transmitting when a command is due | Wait for `bus_quiet_gap` of silence, up to `bus_quiet_max_wait` |
| Spliced or corrupted frame | Rejected by the strict frame format |
| Drain interrupted by a signal (`EINTR`) | Resumed, up to ten times; the command is not re-sent |
| Corrupted reply that still parses | Not trusted until a second read agrees; never clamped |
| No reply | Retried up to `retries`, then the register is marked invalid |
| Frame with no terminator | Buffer dropped, stream resynced |
| `EIO` / adapter unplugged | Port marked unhealthy; supervisor reopens with backoff; cycles are skipped meanwhile |
| Port missing at startup | Retried with backoff indefinitely |
| Broker unreachable | Startup retries with backoff and jitter; later drops are handled by paho with re-subscription |
| Broker stops acknowledging | Each operation fails after `op_timeout` |
| Command arrives mid-cycle | Cycle cancelled, command applied, cycle retried shortly after |
| Write not echoed by the unit | Expected; the write is confirmed by reading `state_from` and `verify_registers` back |
| Write read-back disagrees or is unreadable | Re-sent once; then reported as an error, with the actual value (if any) published and a settle re-read scheduled |
| Retained message on a command topic | Ignored with a warning |
| Unsolicited broadcast frames | Discarded by the frame matcher, including from node addresses that are not configured registers |
| Commands arriving faster than they can be applied | Queued up to 16, then dropped with an error; the MQTT router is never blocked |
| Log file cannot be opened | Falls back to console logging, which is enabled for the fallback |
| Nothing read this cycle | Reported as skipped, retried in 5s rather than a whole interval |

## Shutdown

1. `SIGINT`/`SIGTERM` cancels the root context. A watchdog is armed; a second signal, or
   exceeding the grace period, exits immediately.
2. MQTT disconnects first, so no further commands arrive, and publishes stop retrying.
3. The poll loop, command worker and supervisor finish. Cancellation reaches in-flight
   reads, and a command already being applied stops at its next cancellation check; a
   command already sent is not withdrawn, but its read-back and state publish are skipped.
   This takes at most about one `read_timeout`.
4. The serial port closes. `Close` takes the transaction lock, so a write already on the
   wire completes before the device is released.
5. The log file is closed.

## Configuration reference

### Serial (milliseconds unless stated)

| Option | Default | Purpose |
|--------|---------|---------|
| `port` | `/dev/ttyUSB0` | Device path |
| `baudrate` | `115200` | Line speed |
| `bytesize` / `parity` / `stopbits` | `8` / `N` / `1` | Line settings, all applied |
| `read_timeout` | `200` | Per-read block time; cut short at the response deadline |
| `response_timeout` | `300` | Budget for one exchange; raised to `read_timeout` if smaller |
| `bus_quiet_gap` | `20` | Silence required on the shared bus before transmitting |
| `bus_quiet_max_wait` | `100` | Upper bound on the wait for a quiet bus |
| `retries` | `3` | Attempts per register operation, shared with confirmation; at least 2 |
| `retry_backoff` | `100` | Delay between attempts, jittered by up to half again |
| `reopen_backoff` / `reopen_max_backoff` | `500` / `30000` | Supervised reopen backoff |
| `health_check_interval` | `1000` | Port health poll interval |
| `write_priority_timeout` | `10000` | Maximum time a write may hold reads off |
| `write_settle_delay` | `50` | Wait after a write before reading the register back |
| `write_resettle_delay` | `5000` | Wait before a second read-back, for values that lag; `0` disables it |
| `xonxoff` / `dsrdtr` | `false` | Not supported by the driver; `true` is warned about and has no effect |

### MQTT

| Option | Default | Purpose |
|--------|---------|---------|
| `broker` / `port` | — / `1883` | Broker address |
| `username` / `password` | — | Credentials |
| `client_id` | `aerosmart-gateway` | Client identifier |
| `qos` / `retain` | `0` / `false` | Delivery and retention; the shipped configs set `1` / `true` |
| `availability_topic` | `<device_id>/status` | Online/offline topic and last will |
| `op_timeout` | `5000` | Bound on every broker interaction |
| `publish_retry_count` | `3` | Publish attempts |
| `connect_retry_*` | `500` / `30000` / `25` | Startup backoff, cap, jitter percent |

### Application

| Option | Default | Purpose |
|--------|---------|---------|
| `device_id` | `aerosmart` | Home Assistant device identifier |
| `read_interval` | `60` | Seconds between cycles, measured from the end of the previous one |
| `log_level` | `info` | `debug`, `info`, `warn`, `error` |
| `logging.console_logging` | `true` | Console output; disabling both sinks is rejected |
| `logging.file_logging` / `log_file` | `false` / `-` | File output; `-` or empty disables it |
| `ha_discovery.enabled` / `prefix` | `false` / `homeassistant` | Discovery; the shipped configs enable it |
| `ha_discovery.expire_after` | `2.5 x read_interval` | Seconds before an entity is shown unavailable; `0` omits it, so entities never expire |

Options removed with the serial rework are reported as warnings at startup rather than
silently ignored: `max_retries`, `read_max_retries`, `write_max_retries`,
`device_response_delay`, `read_with_retry_delay`, `write_with_retry_delay`,
`deadline_timeout`, `max_reopens`, `timeout`, `write_timeout`, and `serial.connect_retry_*`.
