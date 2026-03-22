# Aerosmart Gateway - Timing Diagrams

This document contains detailed timing diagrams illustrating the interactions between the different components of the application.

## Table of Contents

1. [System Overview](#system-overview)
2. [Initialization Sequence](#initialization-sequence)
3. [Periodic Read Cycle](#periodic-read-cycle)
4. [Write Operation with Verification](#write-operation-with-verification)
5. [Retry and Recovery](#retry-and-recovery)
6. [Write Priority Preemption](#write-priority-preemption)
7. [MQTT Message Flow](#mqtt-message-flow)
8. [Graceful Shutdown](#graceful-shutdown)

---

## System Overview

```
┌────────────────────────────────────────────────────────────────────────────┐
│                           AEROSMART GATEWAY                                │
│                                                                            │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                  │
│  │    main.go   │    │   serial/    │    │    mqtt/     │                  │
│  │              │    │  serial.go   │    │   client.go  │                  │
│  │  - Config    │    │              │    │              │                  │
│  │  - Init      │    │  - Open/Close│    │  - Connect   │                  │
│  │  - Main Loop │    │  - Write     │    │  - Publish   │                  │
│  └──────┬───────┘    │  - Read      │    │  - Subscribe │                  │
│         │            └──────┬───────┘    └──────┬───────┘                  │
│         │                   │                   │                          │
│         └───────────────────┼───────────────────┘                          │
│                             ▼                                              │
│                    ┌────────────────┐                                      │
│                    │   registers/   │                                      │
│                    │   reader.go    │                                      │
│                    │                │                                      │
│                    │ - ReadSingle   │◄──────────┐                          │
│                    │ - ReadAll      │           │                          │
│                    │ - PublishAll   │           │                          │
│                    └────────────────┘           │                          │
│                             │                   │                          │
│                             ▼                   │                          │
│                    ┌────────────────┐           │                          │
│                    │   registers/   │           │                          │
│                    │   reader.go    │           │                          │
│                    │   (Writer)     │           │                          │
│                    │ - HandleMessage│───────────┘                          │
│                    │ - TriggerFull  │                                      │
│                    └────────────────┘                                      │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## Initialization Sequence

```
Time (ms)   Component          Action
─────────────────────────────────────────────────────────────────────────────
0           main              Load config.yaml
10          main              Load registers.yaml
15          main              Initialize logger (level, file/console)
20          serial            NewSerialPort("/dev/ttyUSB0", 115200, ...)
25          serial            connectSerialWithRetry() - exponential backoff
30          serial            Open() - connect to device
35          mqtt              NewClient(broker, port, ...)
40          mqtt              connectMQTTWithRetry() - exponential backoff
45          mqtt              Connect() - connect to broker
50          mqtt              publishHADiscovery() - sensor & switch configs
55          registers         NewReader(serial, mqtt, readRegisters)
60          registers         NewWriter(serial, mqtt, writeRegisters)
65          registers         writer.SetReader(reader)
70          mqtt              Subscribe("dw/aerosmart/luefterstufe")
75          mqtt              Subscribe("dw/aerosmart/boilerheizstab")
80          main              Start periodic ticker (60s interval)
85          main              writer.TriggerFullReadout() - initial read
90          registers         reader.ReadAll() - read all registers
95          serial            SendAndReceive("130 1067", 10) - reg 1
100         serial            SendAndReceive("130 5003", 10) - reg 2
...         ...               ... (more registers)
200         registers         reader.PublishAll(values) - publish to MQTT
205         main              Application running, waiting for ticker
```

---

## Periodic Read Cycle

```
Time (ms)   Component          Action
─────────────────────────────────────────────────────────────────────────────
0           main              Ticker fires - writer.TriggerFullReadout()
5           writer            Try send to writePriorityChan (succeeds = empty)
6           writer            Drain channel, no write pending
10          reader            ReadAll() - start read cycle
15          reader            Mark isReading = true
20          serial            SendAndReceive("130 1067", 10)
25          serial            ForceReopen() - close, wait 5ms, reopen
30          serial            FlushInputMultiple(3)
35          serial            WriteWithRetry("130 1067\r\n", 10, 10ms)
40          serial            Sleep(deviceResponseDelay=40ms)
80          serial            ReadWithRetry(10, 100ms) - wait for response
85          serial            <-- "130 1067 3" (fan speed 3)
90          reader            Parse & validate value (3 in range 0-5)
95          serial            SendAndReceive("130 5003", 10)
                              (ForceReopen only on attempt 0 of first call;
                               subsequent calls: FlushInputMultiple(3) + write)
100         serial            WriteWithRetry("130 5003\r\n", 10, 10ms)
105         serial            Sleep(deviceResponseDelay=40ms)
145         serial            ReadWithRetry(10, 100ms)
150         serial            <-- "130 5003 3"
155         reader            Parse & validate value
...         ...               ... (continue for all registers)
500         reader            Mark isReading = false
505         reader            Store values in reader.values
510         reader            PublishAll(values) - publish all to MQTT
515         mqtt              Publish("aerosmart/luefterstatus", "3")
520         mqtt              Publish("aerosmart/lueftermode", "3")
...         ...               ... (publish all valid register values)
550         main              Wait for next ticker
```

### Detailed SendAndReceive Flow

```
SendAndReceive(command, maxRetries=10)
    │
    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ Acquire writeMu lock (ensures write operations have priority)            │
└──────────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ Attempt 0:                                                               │
│  1. WaitForReadComplete() - wait for any ongoing read to finish          │
│  2. ForceReopen() - close and reopen port for clean state                │
│  3. FlushInputMultiple(3) - clear stale data                             │
│  4. WriteWithRetry("130 1067\r\n", 10, 10ms)                             │
│     - Write attempt 1: SUCCESS                                           │
│  5. Sleep(40ms) - device response delay                                  │
│  6. ReadWithRetry(10, 100ms)                                             │
│     - Read attempt 1: SUCCESS - "130 1067 3"                             │
│  7. Parse response - extract value "3"                                   │
│  8. Return response                                                      │
└──────────────────────────────────────────────────────────────────────────┘
    │
    ▼
Release writeMu lock
```

---

## Write Operation with Verification

```
Time (ms)   Component          Action
─────────────────────────────────────────────────────────────────────────────
0           MQTT Broker       Message published to "dw/aerosmart/luefterstufe"
5           mqtt              Callback invoked with message "3"
10          writer            HandleMessage("dw/aerosmart/luefterstufe", "3")
15          writer            SignalWritePriority() - send to channel
20          writer            reader.Cancel() - cancel any ongoing read
25          writer            reader.ResetContext() - create new context
30          writer            Find matching register, validate value (3 in 0-5)
35          writer            Build command: "130 5002 3"
40          serial            SendAndReceive("130 5002 3", 10)
45          serial            ForceReopen() + FlushInputMultiple(3)
50          serial            WriteWithRetry("130 5002 3\r\n", 10, 10ms)
55          serial            Sleep(deviceResponseDelay=40ms)
95          serial            ReadWithRetry(10, 100ms) - get response
100         serial            <-- "130 5002 OK"
105         writer            FlushBuffer() - FlushInputMultiple(5)
110         writer            Sleep(deviceResponseDelay=40ms) - wait before verify
150         writer            Read verify register: luefterstatus
155         serial            reader.ReadSingle(luefterstatus)
160         serial            SendAndReceive("130 1067", 10)
165         serial            <-- "130 1067 3"
170         mqtt              Publish("aerosmart/luefterstatus", "3")
175         writer            Read verify register: lueftermode
180         serial            reader.ReadSingle(lueftermode)
185         serial            SendAndReceive("130 5003", 10)
190         serial            <-- "130 5003 3"
195         mqtt              Publish("aerosmart/lueftermode", "3")
200         writer            Complete - return nil
```

### Write Priority Signaling

```
Writer.SignalWritePriority()
    │
    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ select {                                                                 │
│ case writePriorityChan <- struct{}{}:                                    │
│     // Channel was empty - write priority signaled                       │
│ default:                                                                 │
│     // Channel already has value - write already pending                 │
│ }                                                                        │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Retry and Recovery

### Serial Communication Retry Flow

```
SendAndReceive fails
    │
    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ For attempt = 0 to maxRetries-1:                                         │
│                                                                          │
│  1. WriteWithRetry(command, writeMaxRetries=10, delay=10ms)              │
│     - attempt 0: FAIL (timeout)                                          │
│     - sleep 10ms                                                         │
│     - attempt 1: FAIL (timeout)                                          │
│     - ...                                                                │
│     - attempt 9: FAIL                                                    │
│                                                                          │
│  2. tryReopen() - check if can reopen                                    │
│     - reopenCount < maxReopens (10)                                      │
│     - Reopen() - close port, wait 2ms, reopen                            │
│                                                                          │
│  3. Continue to next attempt                                             │
│                                                                          │
│  4. ReadWithRetry(readMaxRetries=10, delay=100ms)                        │
│     - attempt 0: FAIL (timeout)                                          │
│     - sleep 100ms                                                        │
│     - attempt 1: FAIL                                                    │
│     - ...                                                                │
│     - attempt 9: FAIL                                                    │
│                                                                          │
│  5. tryReopen() - reopen port                                            │
│                                                                          │
│  6. If all attempts fail: return error                                   │
└──────────────────────────────────────────────────────────────────────────┘
```

### Connection Retry with Exponential Backoff

```
Initial delay: 2ms (serial), 500ms (MQTT)
Max delay: 400ms (serial), 30s (MQTT)
Jitter: 25%

Attempt 0: delay = 2ms * (1 ± 0.25) = 1.5-2.5ms
Attempt 1: delay = 4ms * (1 ± 0.25) = 3-5ms
Attempt 2: delay = 8ms * (1 ± 0.25) = 6-10ms
Attempt 3: delay = 16ms * (1 ± 0.25) = 12-20ms
Attempt 4: delay = 32ms * (1 ± 0.25) = 24-40ms
Attempt 5: delay = 64ms * (1 ± 0.25) = 48-80ms
Attempt 6: delay = 128ms * (1 ± 0.25) = 96-160ms
Attempt 7: delay = 256ms * (1 ± 0.25) = 192-320ms
Attempt 8: delay = 400ms * (1 ± 0.25) = 300-400ms (capped)
Attempt 9: delay = 400ms * (1 ± 0.25) = 300-400ms (capped)
```

---

## Write Priority Preemption

### Scenario: Periodic read in progress, write command arrives

```
Time (ms)   Component          Action
─────────────────────────────────────────────────────────────────────────────
0           main              Ticker fires - TriggerFullReadout()
5           writer            Check writePriorityChan - EMPTY
10          reader            ReadAll() - start reading register 1
15          reader            isReading = true
20          serial            SendAndReceive("130 1067", 10)
25          serial            Write("130 1067\r\n")
30          serial            Sleep(40ms)
35          serial            Read() - waiting for response...
40

MQTT Broker       Message published: "dw/aerosmart/luefterstufe" = "3"
45          mqtt              Callback invoked
50          writer            HandleMessage() called
55          writer            SignalWritePriority() - chan <- {}
60          writer            Cancel() - reader.Cancel()
65          writer            ResetContext()
70          reader            Context cancelled - ctx.Done() signaled
75          serial            Read() returns (interrupted)
80          main              select: ctx.Done() case selected
85          main              Skip remaining registers
90          main              Close(forceQuit)
95          writer            HandleMessage() continues
100         serial            SendAndReceive("130 5002 3", 10)
105         serial            Write command
110         serial            Read response
115         serial            FlushBuffer()
120         serial            Verify registers
125         mqtt              Publish verified values
130         main              Next ticker fires - normal read proceeds
```

---

## MQTT Message Flow

### Publishing Sensor Data

```
Reader.PublishAll(values)
    │
    ▼
For each register value:
    │
    ├─> mqtt.Publish("aerosmart/luefterstatus", "3")
    │       │
    │       ▼
    │   ┌────────────────────────────────────┐
    │   │ Check IsConnected()                │
    │   │ client.Publish(topic, QOS=0,       │
    │   │             Retain=true, "3")      │
    │   │ token.Wait()                       │
    │   └────────────────────────────────────┘
    │
    ├─> mqtt.Publish("aerosmart/lueftermode", "3")
    │       │
    │       ▼
    │   ┌────────────────────────────────────┐
    │   │ Check IsConnected()                │
    │   │ client.Publish(topic, QOS=0,       │
    │   │             Retain=true, "3")      │
    │   │ token.Wait()                       │
    │   └────────────────────────────────────┘
    │
    └─> ... (more registers)
```

### Subscribing to Write Commands

```
mqttClient.Subscribe("dw/aerosmart/luefterstufe", handler)
    │
    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ 1. Define callback:                                                      │
│    callback := func(client mqtt.Client, msg mqtt.Message) {              │
│        handler(msg.Topic(), string(msg.Payload()))                       │
│    }                                                                     │
│                                                                          │
│ 2. Subscribe:                                                            │
│    token := client.Subscribe(topic, QOS=0, callback)                     │
│    token.Wait()                                                          │
│                                                                          │
│ 3. When message arrives:                                                 │
│    - MQTT broker sends message                                           │
│    - callback invoked                                                    │
│    - handler(msg.Topic(), string(msg.Payload()))                         │
│    - Writer.HandleMessage() called                                       │
└──────────────────────────────────────────────────────────────────────────┘
```

### Home Assistant Discovery

```
publishHADiscovery()
    │
    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ For each read register with HA config:                                   │
│  1. Create HASensorConfig{                                               │
│       Name: "Aerosmart luefterstatus",                                   │
│       StateTopic: "aerosmart/luefterstatus",                             │
│       Unit: nil,                                                         │
│       DeviceClass: nil,                                                  │
│       UniqueID: "aerosmart_luefterstatus",                               │
│       Device: {...}                                                      │
│     }                                                                    │
│  2. Publish to                                                           │
│          "homeassistant/sensor/aerosmart/aerosmart_luefterstatus/config" │
│  3. Sleep(2ms) - rate limiting                                           │
│                                                                          │
│ For each write register with HA config:                                  │
│  1. Create HASwitchConfig{                                               │
│       Name: "Aerosmart Lüfterstufe",                                     │
│       CommandTopic: "dw/aerosmart/luefterstufe",                         │
│       StateTopic: "aerosmart/lueftermode",                               │
│       UniqueID: "aerosmart_luefterstufe",                                │
│       Device: {...}                                                      │
│     }                                                                    │
│  2. Publish to                                                           │
│          "homeassistant/switch/aerosmart/aerosmart_luefterstufe/config"  │
│  3. Sleep(2ms) - rate limiting                                           │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Graceful Shutdown

```
Signal (SIGINT/SIGTERM) received
    │
    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ 1. main: <-sigChan                                                       │
│ 2. log.SignalReceived(sig)                                               │
│ 3. cancel() - ctx cancelled                                              │
│                                                                          │
│ 4. Main loop:                                                            │
│    select {                                                              │
│    case <-ctx.Done():                                                    │
│        close(forceQuit)                                                  │
│        return                                                            │
│    }                                                                     │
│                                                                          │
│ 5. <-forceQuit (after 5 second timeout or immediate)                     │
│ 6. log.Info("Force shutdown initiated...")                               │
│                                                                          │
│ 7. mqttClient.Disconnect()                                               │
│    - client.Disconnect(250ms)                                            │
│    - connected = false                                                   │
│                                                                          │
│ 8. serialPort.Close()                                                    │
│    - port.Close()                                                        │
│    - open = false                                                        │
│                                                                          │
│ 9. log.Info("Aerosmart Gateway stopped")                                 │
└──────────────────────────────────────────────────────────────────────────┘
```

### Shutdown Timeout Handling

```
┌──────────────────────────────────────────────────────────────────────────┐
│ go func() {                                                              │
│     sig := <-sigChan                                                     │
│     log.SignalReceived(sig)                                              │
│                                                                          │
│     // Cancel context for graceful shutdown                              │
│     cancel()                                                             │
│                                                                          │
│     // Wait for graceful shutdown with timeout                           │
│     select {                                                             │
│     case <-forceQuit:                                                    │
│         // Already shut down gracefully                                  │
│         return                                                           │
│     case <-time.After(5 * time.Second):                                  │
│         // Force shutdown after timeout                                  │
│         log.Warn("Graceful shutdown timed out, forcing exit...")         │
│         os.Exit(1)                                                       │
│     }                                                                    │
│ }()                                                                      │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Summary

The timing diagrams above illustrate:

1. **Initialization**: Application startup sequence with connection establishment
2. **Periodic Read**: Regular register reading with MQTT publishing
3. **Write Operations**: Control command handling with verification
4. **Retry Logic**: Exponential backoff and port reopening on failures
5. **Write Priority**: Preemption mechanism for time-sensitive control commands
6. **MQTT Flow**: Message publishing and subscription handling
7. **Shutdown**: Graceful shutdown with timeout handling

These diagrams help understand the data flow and timing characteristics of the application, which is useful for debugging and performance optimization.
