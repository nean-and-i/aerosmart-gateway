# Aerosmart Gateway - Application Flow Analysis

This document provides a detailed analysis of the Aerosmart Gateway application flow, including the sequence of operations for reading and writing registers, handling MQTT messages, and managing serial communication.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Component Interactions](#component-interactions)
3. [Serial Communication Flow](#serial-communication-flow)
4. [Register Read Operations](#register-read-operations)
5. [Register Write Operations](#register-write-operations)
6. [MQTT Integration](#mqtt-integration)
7. [Write Priority Mechanism](#write-priority-mechanism)
8. [Timing Diagram](#timing-diagram)
9. [Retry and Recovery Mechanisms](#retry-and-recovery-mechanisms)
10. [Areas for Improvement](#areas-for-improvement)

---

## Architecture Overview

The application follows a layered architecture with the following main components:

```
┌─────────────────────────────────────────────────────────────────┐
│                        main.go                                  │
│  - Initialization & Configuration                               │
│  - Connection Management                                        │
│  - Main Loop (Periodic Reads)                                   │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌───────────────┐   ┌─────────────────┐   ┌───────────────┐
│   serial/     │   │    mqtt/        │   │   registers/  │
│   serial.go   │   │    client.go    │   │   reader.go   │
│               │   │                 │   │               │
│ - Open/Close  │   │ - Connect       │   │ - ReadSingle  │
│ - Write       │   │ - Publish       │   │ - ReadAll     │
│ - Read        │   │ - Subscribe     │   │ - PublishAll  │
│ - SendAnd     │   │ - Discovery     │   │               │
│   Receive     │   │                 │   │               │
└───────────────┘   └─────────────────┘   └───────────────┘
        │                     │                     │
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              ▼
                    ┌─────────────────┐
                    │   registers/    │
                    │   reader.go     │
                    │   (Writer)      │
                    │ - HandleMessage │
                    │ - TriggerFull   │
                    │ - Verification  │
                    └─────────────────┘
```

---

## Component Interactions

### Main Initialization Flow

```
1. Load Configuration (config.yaml)
   └─> Load Registers Config (registers.yaml)

2. Initialize Logger
   └─> Configure log level, file/console output

3. Initialize Serial Port
   └─> NewSerialPort() with all timing parameters

4. Connect to Serial Device (with retry)
   └─> connectSerialWithRetry() - exponential backoff + jitter

5. Initialize MQTT Client
   └─> NewClient() with broker config

6. Connect to MQTT Broker (with retry)
   └─> connectMQTTWithRetry() - exponential backoff + jitter

7. Publish Home Assistant Discovery (if enabled)
   └─> publishHADiscovery() - sensor & switch configs

8. Initialize Register Reader
   └─> NewReader() with read registers config

9. Initialize Register Writer
   └─> NewWriter() with write registers config
   └─> SetDerivedCalculator() — derived registers computed & published each readout

10. Subscribe to Write Register Topics
    └─> For each write register: mqttClient.Subscribe()

11. Start Periodic Read Loop
    └─> TriggerFullReadout() every N seconds
```

---

## Serial Communication Flow

### Serial Port Configuration

The serial port is configured with the following parameters (from `config.yaml`):

| Parameter | Default | Description |
|-----------|---------|-------------|
| Baud Rate | 115200 | Communication speed |
| Read Timeout | 2ms (config), 200ms (fallback) | Max wait for serial read (treated as milliseconds in code) |
| Write Timeout | 10s | Max wait for write |
| Device Response Delay | 40ms | Wait after write before read |
| Write Retry Delay | 10ms | Delay between write retries |
| Read Retry Delay | 100ms | Delay between read retries |
| Deadline Timeout | 150ms | Flush input deadline timeout |
| Max Retries | 10 | Max attempts for register operations |
| Max Reopens | 10 | Max port reopen attempts |

### Serial Port Methods

```go
// Core serial operations
Open()              // Open serial port connection
Close()             // Close serial port connection
Write(data string)  // Write raw data to port
WriteCommand(cmd)   // Write command with \r\n terminator
Read()              // Read line from port with timeout

// Retry-enabled operations
WriteWithRetry(cmd, maxRetries, delay)  // Write with retry
ReadWithRetry(maxRetries, delay)        // Read with retry

// Combined operation with hybrid retry
SendAndReceive(command, maxRetries)     // Write + Read with retry logic
  - Flush input buffer
  - Write command
  - Wait for device response (deviceResponseDelay)
  - Read response with retry
  - On failure: tryReopen() and retry
```

---

## Register Read Operations

### Read Single Register

```
Reader.ReadSingle(register)
    │
    ▼
┌─────────────────────────────────────┐
│ For each retry attempt (maxRetries) │
│  1. Send command via SendAndReceive │
│  2. Parse response                  │
│  3. Apply divisor & type conversion │
│  4. Validate value range            │
│  5. Return processed value          │
└─────────────────────────────────────┘
```

### Read All Registers

```
Reader.ReadAll()
    │
    ▼
┌─────────────────────────────────────┐
│ 1. Mark isReading = true            │
│ 2. For each register:               │
│    a. Check for cancellation        │
│    b. ReadSingle(register)          │
│    c. Validate value                │
│    d. Store in results map          │
│ 3. Mark isReading = false           │
│ 4. Store values in reader.values    │
│ 5. Return results                   │
└─────────────────────────────────────┘
```

### SendAndReceive Detailed Flow

```
SerialPort.SendAndReceive(command, maxRetries)
    │
    ▼
┌─────────────────────────────────────────────────────┐
│ Acquire writeMu lock (ensures write priority)       │
└─────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────┐
│ For each attempt (0 to maxRetries-1)                │
│  1. WaitForReadComplete() - wait for ongoing reads  │
│  2. ForceReopen() on retry attempts only (>0)       │
│  3. FlushInputMultiple(3) - clear stale data        │
│  4. WriteWithRetry() - write command with retries   │
│     └─> If fails: tryReopen() and continue          │
│  5. Sleep(deviceResponseDelay) - wait for device    │
│  6. ReadWithRetry() - read response with retries    │
│     └─> If fails: tryReopen() and continue          │
│  7. If success: return response                     │
└─────────────────────────────────────────────────────┘
    │
    ▼
Release writeMu lock
```

---

## Register Write Operations

### Write Operation Flow

```
MQTT Message Received on write topic
    │
    ▼
Writer.HandleMessage(topic, message)
    │
    ▼
┌─────────────────────────────────────────────────────┐
│ 1. SignalWritePriority() - signal write priority    │
│ 2. Cancel ongoing read (reader.Cancel())            │
│ 3. Reset read context (reader.ResetContext())       │
│ 4. Find matching write register config              │
│ 5. Parse and validate value                         │
│ 6. Build command from template                      │
│ 7. Open serial port if not open                     │
│ 8. SendAndReceive(command) - write + read response  │
│ 9. FlushBuffer() - FlushInputMultiple(5)            │
│ 10. Wait deviceResponseDelay before verification    │
│ 11. Read verify registers (if configured)           │
│     - ReadSingle() for each verify register         │
│     - Publish verified values to MQTT               │
│ 12. ClearWritePriority() - allow reads to resume    │
└─────────────────────────────────────────────────────┘
```

### Write Priority Mechanism

The application implements a write priority mechanism to ensure control commands are processed with minimal latency. The mechanism uses a timestamp-based approach with automatic expiry to prevent permanent blocking.

#### Timestamp-Based Implementation

The current implementation uses a mutex-protected timestamp (`writePriorityTime time.Time`) with a 10-second auto-expiry timeout:

```
Write Priority Detection (Timestamp-Based)
    │
    ▼
┌─────────────────────────────────────────────────────┐
│ Writer.writePriorityTime (time.Time)                │
│ Writer.writePriorityTimeout = 10 seconds            │
│ Writer.writePriorityMu (sync.Mutex)                 │
│                                                     │
│ When a write message arrives (HandleMessage):       │
│   1. SignalWritePriority()                          │
│      - Lock mutex                                   │
│      - writePriorityTime = time.Now()              │
│      - Unlock mutex                                 │
│   2. Cancel() ongoing read + ResetContext()         │
│   ... (write + verify) ...                          │
│   N. ClearWritePriority()                           │
│      - Lock mutex                                   │
│      - writePriorityTime = time.Time{} (zero)      │
│      - Unlock mutex                                 │
│                                                     │
│ During periodic read (TriggerFullReadout):          │
│   1. Call isWritePriorityActive()                   │
│      - If writePriorityTime is zero: return false  │
│      - If time.Since >= 10s: expire + return false │
│      - Otherwise: return true (skip read)          │
└─────────────────────────────────────────────────────┘
```

**Why Timestamp with Auto-Expiry?**
- The original channel-based approach had an inverted logic bug
- The atomic flag approach could block reads permanently on serial errors
- The timestamp approach auto-expires after 10 seconds, ensuring reads always resume
- Explicit `ClearWritePriority()` after successful writes allows immediate read resumption

#### Performance

| Metric | Value |
|--------|-------|
| MQTT detection | <1 second |
| Write completion | 1-3 seconds |
| Auto-expiry timeout | 10 seconds |
| Reads resume after success | Immediately (ClearWritePriority) |
| Reads resume on serial error | After 10 seconds (auto-expiry) |

#### Message Deduplication

The Writer also implements message deduplication to prevent processing duplicate MQTT messages:

```
Message Deduplication
    │
    ▼
┌─────────────────────────────────────────────────────┐
│ HandleMessage(topic, message)                       │
│   │                                                 │
│   ▼                                                 │
│ isDuplicateMessage(topic, message)?                │
│   │                                                 │
│   ├─> Check recentMessages map                     │
│   │   - Key: "topic:value"                         │
│   │   - Value: MessageInfo{Topic, Value, Time}     │
│   │                                                 │
│   ├─> If exists AND timestamp within window:       │
│   │   - Return true (skip duplicate)               │
│   │                                                 │
│   └─> If not exists or outside window:             │
│       - Store in map with current timestamp        │
│       - Return false (process message)             │
│                                                     │
│ Deduplication window: 1 second (default)           │
└─────────────────────────────────────────────────────┘
```

#### Timing Metrics

The Writer tracks message processing latency for monitoring:

```
Timing Metrics
    │
    ▼
┌─────────────────────────────────────────────────────┐
│ 1. Message received:                                │
│    lastMessageReceivedTime = Now()                 │
│                                                     │
│ 2. Processing started:                              │
│    lastMessageProcessTime = Now()                  │
│                                                     │
│ 3. Processing completed:                            │
│    lastMessageCompleteTime = Now()                 │
│    lastMessageLatency =                             │
│      lastMessageCompleteTime -                     │
│      lastMessageReceivedTime                       │
│                                                     │
│ Log: "MQTT: Message processing latency:            │
│       X.XXs (receive -> process -> complete)"      │
└─────────────────────────────────────────────────────┘
```

> **Note:** The write priority mechanism uses timestamps with automatic 10-second expiry to ensure system recovery from serial errors.

---

## MQTT Integration

### MQTT Client Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| Broker | MQTT broker IP/hostname | - |
| Port | MQTT broker port | 1883 |
| Username | Authentication username | - |
| Password | Authentication password | - |
| ClientID | Unique client identifier | `aerosmart-gateway` |
| QOS | Quality of Service | Configurable (default: 1) |
| Retain | Retain messages | `true` |
| KeepAlive | KeepAlive interval (seconds) | 30 |
| MaxReconnectInterval | Maximum reconnection interval | 60s |
| CleanSession | Preserve sessions across reconnects | `false` |
| PublishRetryCount | Number of publish retries | 3 |

### MQTT Operations

```
Publish(topic, value)
    │
    ▼
┌─────────────────────────────────────┐
│ 1. Check IsConnected()              │
│ 2. For retry_count attempts:        │
│    a. client.Publish(topic, QOS,     │
│       Retain, value)                │
│    b. Wait for completion           │
│    c. If failed: sleep + retry      │
│       (1s, 2s, 4s exponential)      │
│ 3. Return success or error          │
└─────────────────────────────────────┘

Subscribe(topic, handler)
    │
    ▼
┌─────────────────────────────────────┐
│ 1. Check IsConnected()              │
│ 2. Define callback function         │
│ 3. client.Subscribe(topic, QOS,     │
│    callback)                        │
│ 4. Wait for completion              │
│ 5. Track subscription for recovery  │
└─────────────────────────────────────┘
```

### MQTT Resilience Features

The gateway implements comprehensive MQTT resilience:

#### Persistent Session (CleanSession=false)
- Subscriptions are preserved across reconnects
- Broker maintains subscription state
- No manual re-subscription needed after connection loss

#### Auto-Reconnect
- Reconnect retry interval: 5 seconds (fixed, `SetConnectRetryInterval`)
- Maximum reconnect interval: 60 seconds (`SetMaxReconnectInterval`)
- Handled by the paho client. The `connect_retry_*` config values apply only to the *initial* connect loop in `main.go`, not to steady-state reconnects.

#### KeepAlive Monitoring
- Sends PINGREQ every 30 seconds
- Detects network failures within 30-60 seconds
- Triggers immediate reconnection on failure

#### Subscription Recovery
- On reconnect: `resubscribeAll()` is called
- Re-subscribes to all tracked topics
- Logs recovery progress:
  - "MQTT recovering X subscriptions..."
  - "MQTT all subscriptions recovered"

#### Connection State Handlers
- `OnConnectionLost`: Sets connected=false, logs error
- `OnConnect`: Sets connected=true, triggers subscription recovery
- `SetReconnectingHandler`: Logs reconnection attempts
- `SetConnectionNotificationHandler`: Logs all state changes

#### Publish Retry
- Failed publishes are retried up to 3 times
- Exponential backoff: 1s, 2s, 4s
- Uses configured QoS level (default: 1) for at-least-once delivery

### Home Assistant Discovery

When `ha_discovery.enabled: true`, the gateway publishes:

- **Sensor Discovery**: For each read register with HA config
  - Topic: `{ha_discovery.prefix}/sensor/{device_id}/{device_id}_{register_name}/config`
  - Payload: JSON with name, state_topic, unit, device_class, unique_id, device

- **Switch Discovery**: For each write register with HA config
  - Topic: `{ha_discovery.prefix}/switch/{device_id}/{device_id}_{register_name}/config`
  - Payload: JSON with name, command_topic, state_topic, unique_id, device

---

## Connection State Flow

### MQTT Connection Lifecycle

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Initial Connection                                       │
│    - Connect with CleanSession=false                        │
│    - Set KeepAlive=30s                                      │
│    - Set AutoReconnect=true                                 │
│    - Set MaxReconnectInterval=60s                           │
│    - Set OnConnectHandler: resubscribeAll()                 │
│    - Set OnConnectionLostHandler                            │
│    - Set ReconnectingHandler                                │
│    - Set ConnectionNotificationHandler                      │
│    - OnConnect: resubscribeAll()                            │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Connection Lost                                          │
│    - Network failure or broker restart                      │
│    - OnConnectionLost fires                                 │
│    - Auto-reconnect starts                                  │
│    - ConnectionNotification: ConnectionLost                 │
│    - ReconnectingHandler logs: "MQTT attempting to          │
│      reconnect..."                                          │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Reconnection                                             │
│    - ConnectionNotification: Connecting                     │
│    - Fixed 5s retry interval, capped at 60s max            │
│    - ReconnectingHandler logs each attempt                  │
│    - ConnectionNotification: Connected                      │
│    - OnConnect: resubscribeAll()                            │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Subscription Recovery                                    │
│    - resubscribeAll() called                                │
│    - Get all tracked subscriptions from internal map        │
│    - Re-subscribe to each topic with configured QoS       │
│    - Logs: "MQTT recovering X subscriptions..."             │
│    - For each topic: client.Subscribe(topic, QOS, callback)  │
│    - Logs: "MQTT all subscriptions recovered"              │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Normal Operation                                         │
│    - Continue periodic read cycle                           │
│    - Handle incoming MQTT messages                          │
│    - Publish sensor values to MQTT                          │
└─────────────────────────────────────────────────────────────┘
```

### Connection State Log Messages

| Event | Log Message |
|-------|-------------|
| Reconnecting | `MQTT attempting to reconnect...` |
| State: Connecting | `MQTT connection state: Connecting` |
| State: Connected | `MQTT connection state: Connected` |
| State: ConnectionLost | `MQTT connection state: ConnectionLost` |
| State: Failed | `MQTT connection state: Failed` |
| Subscription Recovery | `MQTT recovering X subscriptions...` |
| Recovery Complete | `MQTT all subscriptions recovered` |

---

## Timing Diagram

### Periodic Read Cycle Timing

```
Time ─────────────────────────────────────────────────────────────────────►

Component:    Main Loop              SerialPort          MQTT
              │                      │                   │
              │                      │                   │
   0ms        │                      │                   │
              │ TriggerFullReadout() │                   │
              │─────────────────────>│                   │
              │                      │                   │
   0ms        │                      │ SendAndReceive    │
              │                      │ (Register 1)      │
              │                      │───────────────────> Device
              │                      │                   │
   40ms       │                      │ <─────────────────│ Response
              │                      │                   │
              │                      │ Parse & Validate  │
              │                      │                   │
   45ms       │                      │                   │ Publish
              │                      │                   │──────────> Topic
              │                      │                   │
              │                      │ SendAndReceive    │
              │                      │ (Register 2)      │
              │                      │───────────────────> Device
              │                      │                   │
   85ms       │                      │ <─────────────────│ Response
              │                      │                   │
              │                      │                   │
   90ms       │                      │                   │ Publish
              │                      │                   │──────────> Topic
              │                      │                   │
              │                      │ ... (more regs)   │
              │                      │                   │
              │                      │                   │
(Repeat for each register in read_registers)
              │                      │                   │
              │                      │                   │
~1000ms       │ All registers read   │                   │
              │─────────────────────>│                   │
              │                      │                   │
              │ Complete             │                   │
```

### Write Operation Timing (with Verification)

```
Time ─────────────────────────────────────────────────────────────────────►

Component:    MQTT              Writer              SerialPort        Device
              │                 │                   │                 │
              │ Message received│                   │                 │
              │ on write topic  │                   │                 │
              │────────────────>│                   │                 │
              │                 │ HandleMessage()    │                 │
              │                 │───────────────────>│                 │
              │                 │ SignalWritePriority│                 │
              │                 │ (cancel read)      │                 │
              │                 │                   │                 │
              │                 │ SendAndReceive()   │                 │
              │                 │───────────────────>│                 │
              │                 │                   │ Write command   │
              │                 │                   │────────────────>│
              │                 │                   │                 │
              │                 │                   │ <───────────────│ Response
              │                 │                   │                 │
              │                 │ FlushBuffer()      │                 │
              │                 │                   │                 │
              │                 │ Read verify reg 1  │                 │
              │                 │───────────────────>│                 │
              │                 │                   │────────────────>│
              │                 │                   │                 │
              │                 │                   │<────────────────│
              │                 │                   │                 │
              │                 │ Publish verified   │                 │
              │                 │───────────────────>│                 │
              │                 │                   │────────────────>│ MQTT Broker
              │                 │                   │                 │
              │                 │ Read verify reg 2  │                 │
              │                 │───────────────────>│                 │
              │                 │                   │────────────────>│
              │                 │                   │                 │
              │                 │                   │<────────────────│
              │                 │                   │                 │
              │                 │ Publish verified   │                 │
              │                 │───────────────────>│                 │
              │                 │                   │────────────────>│ MQTT Broker
              │                 │                   │                 │
              │                 │ Complete           │                 │
```

### Retry and Recovery Timing

```
Time ─────────────────────────────────────────────────────────────────────►

Component:    SerialPort
              │
   0ms        │ SendAndReceive()
              │──────────────────────> Write command
              │                      (FAIL - timeout)
              │
   10ms       │ tryReopen() - reopen port
              │
   15ms       │ Retry write
              │──────────────────────> Write command
              │                      (SUCCESS)
              │
   40ms       │ <───────────────────── Device response
              │
   45ms       │ Read response
              │ <───────────────────── (FAIL - timeout)
              │
   50ms       │ tryReopen() - reopen port
              │
   55ms       │ Retry read
              │ <───────────────────── Device response
              │
   60ms       │ Parse response
              │
   65ms       │ Return success
```

### Write Priority Preemption

```
Time ─────────────────────────────────────────────────────────────────────►

Component:    Main Loop            Writer              SerialPort        Device
              │                    │                   │                 │
              │ Periodic read      │                   │                 │
              │ started            │                   │                 │
              │───────────────────>│                   │                 │
              │                    │ SendAndReceive()  │                 │
              │                    │───────────────────>│                 │
              │                    │                   │ Write reg 1     │
              │                    │                   │────────────────>│
              │                    │                   │                 │
   +30ms      │                    │                   │ <───────────────│ Response
              │                    │                   │                 │
              │                    │                   │ Read reg 1      │
              │                    │                   │ <───────────────│ (completes)
              │                    │                   │                 │
              │                    │                   │                 │
   +50ms      │                    │ MQTT message      │                 │
              │                    │ received          │                 │
              │                    │──────────────────>│                 │
              │                    │                   │                 │
              │                    │ SignalWritePriority│                │
              │                    │ (sets timestamp)   │               │
              │                    │                   │                 │
              │                    │ Cancel()          │                 │
              │                    │──────────────────>│                 │
              │                    │                   │                 │
              │ Check write        │                   │                 │
              │ priority (active?) │                   │                 │
              │───────────────────>│                   │                 │
              │                    │                   │                 │
              │ Write detected!    │                   │                 │
              │ Cancel read        │                   │                 │
              │ Skip this cycle    │                   │                 │
              │                    │                   │                 │
              │                    │ HandleMessage()   │                 │
              │                    │───────────────────>│                 │
              │                    │                   │ Write command   │
              │                    │                   │────────────────>│
              │                    │                   │                 │
              │                    │                   │ <───────────────│ Response
              │                    │                   │                 │
              │                    │ Verify registers  │                 │
              │                    │                   │                 │
              │                    │                   │                 │
   +200ms     │ Next periodic      │                   │                 │
              │ read starts        │                   │                 │
              │ (normal priority)  │                   │                 │
```

---

## Retry and Recovery Mechanisms

### Connection Retry (Serial & MQTT)

Both serial and MQTT connections use exponential backoff with jitter:

```
Initial Delay: 100ms (serial), 500ms (MQTT)
Max Delay: 10000ms (serial), 30000ms (MQTT)
Jitter: 25%

Formula: delay = min(initialDelay * 2^attempt, maxDelay) * (1 + random(-jitter, +jitter))
```

### MQTT Resilience Features

#### KeepAlive Disconnect Detection
- **Interval**: 30 seconds
- **Detection Time**: 30-60 seconds after network failure
- **Behavior**: If no PINGRESP received within 30s + ping timeout, connection is considered lost

#### Auto-Reconnect Behavior
1. Connection lost detected (via KeepAlive or OnConnectionLost)
2. ReconnectingHandler logs: "MQTT attempting to reconnect..."
3. Auto-reconnect retries at a fixed 5s interval (capped at 60s)
4. On successful connect: OnConnectHandler triggers subscription recovery
5. Subscription recovery: resubscribeAll() re-subscribes to all topics
6. Normal operation resumes

#### Publish Retry
- **Retry Count**: 3 (configurable via `publish_retry_count`)
- **Backoff**: Exponential (1s, 2s, 4s)
- **QoS**: Uses configured QoS level (default: 1) for at-least-once delivery

#### Subscription Recovery
- Subscriptions tracked in internal map
- On reconnect: resubscribeAll() iterates and re-subscribes
- Each subscription uses the configured QoS level
- Logs recovery progress for monitoring

### Serial Communication Retry

The `SendAndReceive` method implements a hybrid retry approach:

1. **Write Retry**: Attempt to write up to `writeMaxRetries` times with `writeWithRetryDelay` between attempts
2. **Read Retry**: Attempt to read up to `readMaxRetries` times with `readWithRetryDelay` between attempts
3. **Port Reopen**: If either write or read fails, try to reopen the serial port and retry
4. **Max Attempts**: Total attempts limited by `maxRetries` parameter

### Port Reopen Strategy

- **Automatic Reopen**: On communication failure, the port is automatically reopened
- **Force Reopen**: On retry attempts only (attempt > 0), the port is force-reopened to recover a clean state; the first attempt reuses the open connection
- **Max Reopens**: Limited to `maxReopens` attempts before giving up

---

## Areas for Improvement

### Performance

1. **Parallel Register Reading**: Currently registers are read sequentially. Consider reading multiple registers in parallel where the device supports it.

2. **Batch MQTT Publishing**: Instead of publishing each register value individually, consider batching multiple values into a single MQTT message using JSON.

3. **Optimized Buffer Flushing**: `SendAndReceive` calls `FlushInputMultiple(3)` before writes, and `FlushBuffer()` calls `FlushInputMultiple(5)` after writes. Each flush iteration has a 10ms sleep between iterations. Consider more efficient buffer clearing strategies.

### Reliability

1. **Connection Health Monitoring**: Add periodic checks to verify both serial and MQTT connections are healthy.

2. **Watchdog Timer**: Implement a watchdog that restarts the application if no communication occurs for a configured period.

3. **Data Validation**: Add more robust validation for received data, including checksum verification if the protocol supports it.

### Architecture

1. **Event-Driven Model**: Consider transitioning from periodic polling to an event-driven model where the device pushes updates.

2. **State Machine**: Implement a proper state machine for connection states (connecting, connected, reconnecting, disconnected).

3. **Metrics Collection**: Add Prometheus metrics for monitoring serial communication success rates, MQTT message counts, and operation latencies.

---

## Configuration Summary

### Serial Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `port` | /dev/ttyUSB0 | Serial device path |
| `baudrate` | 115200 | Communication speed |
| `read_timeout` | 2ms (config default), 200ms (fallback if 0) | Read timeout (treated as milliseconds in code) |
| `deadline_timeout` | 150ms | Flush input deadline timeout |
| `device_response_delay` | 40ms | Wait after write |
| `write_with_retry_delay` | 10ms | Write retry delay |
| `read_with_retry_delay` | 100ms | Read retry delay |
| `read_max_retries` | 10 | Max read retries per SendAndReceive |
| `write_max_retries` | 10 | Max write retries per SendAndReceive |
| `max_retries` | 10 | Max SendAndReceive attempts |
| `max_reopens` | 10 | Max port reopen attempts |
| `connect_retry_initial_delay_ms` | 100ms | Initial backoff delay for connection |
| `connect_retry_max_delay_ms` | 10000ms | Max backoff delay for connection |
| `connect_retry_jitter_percent` | 25% | Jitter for backoff |

### MQTT Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `broker` | - | MQTT broker address |
| `port` | 1883 | MQTT broker port |
| `username` | - | MQTT username |
| `password` | - | MQTT password |
| `client_id` | aerosmart-gateway | Client identifier |
| `qos` | 1 | Quality of Service (0, 1, or 2) |
| `retain` | true | Retain messages |
| `publish_retry_count` | 3 | Number of retries for failed publishes |
| `connect_retry_initial_delay_ms` | 500 | Initial backoff delay (ms) |
| `connect_retry_max_delay_ms` | 30000 | Maximum backoff delay (ms) |
| `connect_retry_jitter_percent` | 25 | Jitter percentage for backoff |

### MQTT Resilience Configuration (Internal)

| Parameter | Value | Description |
|-----------|-------|-------------|
| `CleanSession` | false | Preserve subscriptions across reconnects |
| `KeepAlive` | 30s | Disconnect detection interval |
| `MaxReconnectInterval` | 60s | Maximum time between reconnection attempts |
| `AutoReconnect` | true | Enable automatic reconnection |
| `ConnectRetry` | true | Retry initial connection |
| `ConnectRetryInterval` | 5s | Interval between connection retries |
| `Subscription QoS` | 1 | At-least-once delivery |
| `Publish QoS` | 1 | At-least-once delivery |

### Application Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `device_id` | aerosmart | Device identifier for HA (used as identifiers) |
| `log_level` | info | Logging level |
| `read_interval` | 60s | Periodic read interval |
| `ha_discovery.enabled` | true | Enable HA discovery |
| `ha_discovery.prefix` | homeassistant | Discovery topic prefix |
| `ha_discovery.device_info.name` | Aerosmart Gateway | Device display name |
| `ha_discovery.device_info.manufacturer` | Drexel und Weiss | Device manufacturer |
| `ha_discovery.device_info.model` | aerosmartPI | Device model |
| `ha_discovery.device_info.sw_version` | (app version) | Software version |
