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
│                    │   writer.go    │           │                          │
│                    │                │           │                          │
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
15          main              Initialize logger
20          serial            NewSerialPort("/dev/ttyUSB0", 115200, ...)
25          serial            Open() - connect to device
30          mqtt              NewClient(broker, port, ...)
35          mqtt              Connect() - connect to broker
40          mqtt              Publish HA discovery configs
45          registers         NewReader(serial, mqtt, readRegisters)
50          registers         NewWriter(serial, mqtt, writeRegisters)
55          mqtt              Subscribe("dw/aerosmart/luefterstufe")
60          mqtt              Subscribe("dw/aerosmart/boilerheizstab")
65          main              TriggerFullReadout() - initial read
70          registers         ReadAll() - read all registers
75          serial            SendAndReceive("130 1067", 10) - reg 1
80          serial            SendAndReceive("130 5003", 10) - reg 2
...         ...               ... (more registers)
200         mqtt              Publish all values to topics
205          main              Start periodic ticker (60s interval)
210          main              Application running
```

---

## Periodic Read Cycle

```
Time (ms)   Component          Action
─────────────────────────────────────────────────────────────────────────────
0           main              Ticker fires - TriggerFullReadout()
5           writer            Check writePriorityChan (empty)
10          reader            ReadAll() - start read cycle
15          reader            Mark isReading = true
20          serial            SendAndReceive("130 1067", 10)
25          serial            ForceReopen() - clean state
30          serial            FlushInputMultiple(3)
35          serial            Write("130 1067\r\n")
40          serial            Sleep(deviceResponseDelay=40ms)
45          serial            Read() - wait for response
50          serial            <-- "130 1067 3" (fan speed 3)
55          serial            Parse response
60          reader            Validate value (3 in range 0-5)
65          mqtt              Publish("aerosmart/luefterstatus", "3")
70          serial            SendAndReceive("130 5003", 10)
75          serial            Write("130 5003\r\n")
80          serial            Sleep(deviceResponseDelay)
85          serial            Read()
90          serial            <-- "130 5003 3"
95          mqtt              Publish("aerosmart/lueftermode", "3")
...         ...               ... (continue for all registers)
500         reader            Mark isReading = false
505         reader            Store values in reader.values
510         main              Wait for next ticker
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
20          writer            Cancel() - cancel any ongoing read
25          writer            ResetContext() - create new context
30          writer            Validate value (3 in range 0-5)
35          writer            Build command: "130 5002 3"
40          serial            SendAndReceive("130 5002 3", 10)
45          serial            Write("130 5002 3\r\n")
50          serial            Sleep(40ms)
55          serial            Read() - get response
60          serial            <-- "130 5002 OK"
65          serial            FlushBuffer() - clear stale data
70          writer            Read verify register: luefterstatus
75          serial            SendAndReceive("130 1067", 10)
80          serial            <-- "130 1067 3"
85          mqtt              Publish("aerosmart/luefterstatus", "3")
90          writer            Read verify register: lueftermode
95          serial            SendAndReceive("130 5003", 10)
100         serial            <-- "130 5003 3"
105         mqtt              Publish("aerosmart/lueftermode", "3")
110         writer            Complete - return nil
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
│  2. Publish to "homeassistant/sensor/aerosmart_luefterstatus/config"     │
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
│  2. Publish to "homeassistant/switch/aerosmart_luefterstufe/config"      │
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
│ 10. os.Exit(0)                                                           │
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
