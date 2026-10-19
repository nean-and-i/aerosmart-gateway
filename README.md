# Aerosmart Gateway

A Go-based gateway application that communicates with Drexel&Weiss Aerosmart M ventilation/heatpump devices via USB serial and integrates with Home Assistant via MQTT.

> **⚠️ Disclaimer:** This software is provided "as is", without warranty of any kind. Use at your own risk. The authors accept no liability for any damages, data loss, or device malfunctions resulting from the use of this software. This project is not affiliated with or endorsed by Drexel und Weiss.

## Features

- **Serial Communication**: Reads and writes to Aerosmart M device via serial over [USB located on Mainboard - refer to INSTALLATION.md](docs/INSTALLATION.md). NOTE: It is **NOT** Modbus RTU, no modbus settings required.
- **MQTT Integration**: Publishes sensor data and subscribes to control commands
- **MQTT Resilience**:
  - **Clean Session**: The broker does not queue commands while the gateway is offline, so an hours-old command is never replayed at startup
  - **Auto-Recovery**: Subscriptions are restored explicitly once the connection returns
  - **Configurable QoS**: Supports QoS 0, 1, or 2 (default: 1 for at-least-once delivery)
  - **KeepAlive Monitoring**: Detects network failures within 30-60 seconds
  - **Connection State Visibility**: Logs reconnection progress and state changes
  - **Publish Retry**: Automatic retry with exponential backoff for failed publishes
- **Home Assistant Discovery**: Auto-discovers sensors, switches and number entities in Home Assistant
- **Derived Calculations**: Calculates derived values (e.g., zuluftabluftprozent, co2luefterstufe4)
- **Configurable Logging**: Supports debug, info, warn, and error log levels
- **Graceful Shutdown**: Handles SIGINT and SIGTERM signals for clean shutdown
- **Reconnection**: Automatic reconnection for both serial and MQTT connections
- **Write Priority**: Control commands preempt ongoing read operations for low-latency response (<1 second detection, 1-3 second write completion). Write priority auto-expires after 10 seconds to prevent blocking on serial errors.
- **Message Deduplication**: Prevents processing duplicate MQTT messages within 1 second window
- **Timing Metrics**: Logs message processing latency (receive → process → complete) for monitoring

## Architecture

Each layer depends on an interface rather than a concrete type, which is what
lets the whole stack be exercised against a scripted device in tests.

```
                        ┌──────────────┐
                        │  MQTT broker │
                        └──────┬───────┘
                               │ commands / state / discovery
                        ┌──────┴────────┐
                        │ internal/mqtt │  Client    bounded ops, LWT
                        │               │  Discovery HA payloads
                        └──────┬────────┘
                               │ Publisher
   ┌───────────────────────────┴────────────────────────────┐
   │                  internal/registers                    │
   │                                                        │
   │   Poller ──── RunCycle ────▶ Reader ────▶ Publisher    │
   │      │                         │                       │
   │   ReadGate ◀─── Preempt ──── Writer                    │
   │                                │                       │
   │                    DerivedCalculator                   │
   └───────────────────────────┬────────────────────────────┘
                               │ Transactor
                        ┌──────┴─────────┐
                        │ internal/serial│ SerialPort  transactions, health
                        │                │ frameReader persistent buffer
                        └──────┬─────────┘
                               │ Port
                        ┌──────┴───────┐
                        │Aerosmart unit│
                        └──────────────┘
```

`cmd` wires these together and owns signal handling, the poll loop and the
command worker.

### Components

| Package | Responsibility |
|---------|----------------|
| `internal/serial` | Frame reassembly, one command/response transaction at a time, port health, supervised reopen |
| `internal/registers` | Read/publish cycles (`Poller`, `Reader`), command handling (`Writer`), write priority (`ReadGate`), derived values |
| `internal/mqtt` | Broker connection with bounded operations, availability, Home Assistant discovery payloads |
| `internal/config` | Loading, defaults and startup validation of both YAML files |
| `internal/logger` | Levelled logging to console and/or file |

See [Application Flow](docs/APPLICATION_FLOW.md) for how the pieces interact and
why the serial layer is shaped the way it is.

## Quick Start

### Prerequisites

- Go 1.27 or later
- A serial device connected to your Aerosmart M ventilation system
- MQTT broker (e.g., Mosquitto)

### Installation

```bash
# Clone the repository
git clone https://github.com/nean/aerosmart-gateway.git
cd aerosmart-gateway

# Build the application
go build -o aerosmart-gateway ./cmd
```

### Configuration

1. Copy the example configuration file:

```bash
cp config/config.yaml.example config.yaml
```

2. Edit `config.yaml` with your settings:

```yaml
serial:
  port: "/dev/ttyUSB0"
  baudrate: 115200
  read_timeout: 200
  response_timeout: 300
  retries: 3

mqtt:
  broker: "192.168.1.20"
  port: 1883
  username: "mqtt"
  password: "your_password"
  client_id: "aerosmart-gateway"
  # Home Assistant marks entities unavailable when this says "offline"
  availability_topic: "aerosmart/status"

device_id: "aerosmart"
log_level: "info"
read_interval: 120

ha_discovery:
  enabled: true
  prefix: "homeassistant"
  # Seconds without an update before an entity is shown unavailable
  # rather than displaying a stale retained value
  expire_after: 300
  device_info:
    name: "Aerosmart Gateway"
    manufacturer: "Drexel und Weiss"
    model: "aerosmartPI"
    # sw_version: defaults to the application version if not set
```

Validate a configuration without starting the gateway:

```bash
./aerosmart-gateway -validate -config config.yaml -registers registers.yaml
```

### Running the Application

```bash
./aerosmart-gateway -config config.yaml -registers registers.yaml
```
Example of full register readout:
![aerosmart-gateway console ](docs/aerosmart-readout-02.png)

## Documentation

For detailed information about the application flow and timing diagrams, see:

- [Application Flow Analysis](docs/APPLICATION_FLOW.md) - Detailed analysis of the application flow
- [Timing Diagrams](docs/TIMING_DIAGRAMS.md) - Visual timing diagrams of component interactions
- [Installation Guide](docs/INSTALLATION.md) - Detailed setup instructions

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `-config` | Path to config file | `config.yaml` |
| `-registers` | Path to registers file | `registers.yaml` |
| `-validate` | Validate both files and exit, without opening the port or the broker | `false` |
| `-version` | Show version information | `false` |

## Configuration Options

### Serial Configuration

All timeouts are in milliseconds.

| Option | Description | Default |
|--------|-------------|---------|
| `serial.port` | Serial device path | `/dev/ttyUSB0` |
| `serial.baudrate` | Baud rate | `115200` |
| `serial.bytesize` | Data bits (5-8) | `8` |
| `serial.parity` | Parity: `N`, `O` or `E` | `N` |
| `serial.stopbits` | Stop bits (1 or 2) | `1` |
| `serial.read_timeout` | How long one read may block. A read is also cut short at the response deadline | `200` |
| `serial.response_timeout` | Budget for one command/response exchange. Stale or unmatched frames are discarded within this window | `300` |
| `serial.bus_quiet_gap` | How long the bus must be silent before a command is transmitted, so it does not collide with the control panel's traffic | `20` |
| `serial.bus_quiet_max_wait` | Upper bound on the wait for a quiet bus | `100` |
| `serial.retries` | Attempts per register operation. This is the only retry level in the stack, and it also pays for confirming a changed reading, so there are always at least 2 | `3` |
| `serial.retry_backoff` | Delay between attempts, jittered by up to half again | `100` |
| `serial.reopen_backoff` | Initial delay between port reopen attempts | `500` |
| `serial.reopen_max_backoff` | Maximum reopen delay | `30000` |
| `serial.health_check_interval` | How often port health is polled | `1000` |
| `serial.write_priority_timeout` | Upper bound on how long a write may hold reads off | `10000` |
| `serial.write_settle_delay` | Wait after writing before reading the register back | `50` |
| `serial.write_resettle_delay` | Wait before a second read-back, for values that trail the command; `0` disables it | `5000` |
| `serial.xonxoff` | Software flow control. **Not supported by the driver**; `true` is reported as a warning and has no effect | `false` |
| `serial.dsrdtr` | Hardware flow control. **Not supported by the driver**; warned about, no effect | `false` |

Worst-case time for one register is
`max(retries, 2) x (max(bus_quiet_gap, bus_quiet_max_wait) + response_timeout + 1.5 x retry_backoff)`, so with
the defaults 3 x 550ms = 1.65s. Startup rejects a configuration whose worst-case full cycle
(that figure times the number of read registers) does not fit inside `read_interval`; with
the shipped 29 registers that is 47.9s, inside both the shipped 120s interval and the 60s
default.

Options from earlier versions (`max_retries`, `read_max_retries`, `write_max_retries`,
`device_response_delay`, `read_with_retry_delay`, `write_with_retry_delay`, `deadline_timeout`,
`max_reopens`, `timeout`, `write_timeout`, and the `serial.connect_retry_*` settings) are no
longer used. They are reported as warnings at startup rather than being silently ignored, so
a configuration from an earlier release still starts — it just tells you what to clean up.

### MQTT Configuration

| Option | Description | Default |
|--------|-------------|---------|
| `mqtt.broker` | MQTT broker address | _(required)_ |
| `mqtt.port` | MQTT broker port | `1883` |
| `mqtt.username` | MQTT username | - |
| `mqtt.password` | MQTT password | - |
| `mqtt.client_id` | Client identifier | `aerosmart-gateway` |
| `mqtt.qos` | Quality of Service (0, 1, or 2) | `1` |
| `mqtt.retain` | Retain messages | `true` |
| `mqtt.publish_retry_count` | Number of retries for failed publishes | `3` |
| `mqtt.availability_topic` | Online/offline topic, also registered as the MQTT last will | `<device_id>/status` |
| `mqtt.op_timeout` | Bounds every broker interaction (ms). Publishes run on the serial read path, so an unbounded wait would stall polling | `5000` |
| `mqtt.connect_retry_initial_delay_ms` | Initial delay for exponential backoff | `500ms` |
| `mqtt.connect_retry_max_delay_ms` | Maximum delay for exponential backoff | `30000ms` |
| `mqtt.connect_retry_jitter_percent` | Jitter percentage for backoff | `25%` |

> **MQTT Resilience Note:** The gateway uses the configured QoS level (default: 1, at-least-once delivery) for subscriptions and publishes. A **clean session** is used deliberately: a persistent session lets the broker queue commands while the gateway is offline and replay them at startup, which would apply an hours-old fan command to a running unit. Subscriptions are therefore restored explicitly in the connect handler rather than by the broker. KeepAlive is 30 seconds for fast disconnect detection.

### Application Configuration

| Option | Description | Default |
|--------|-------------|---------|
| `device_id` | Device identifier (used as HA identifiers) | `aerosmart` |
| `log_level` | Logging level | `info` |
| `read_interval` | Read interval (seconds) | `60` |
| `ha_discovery.enabled` | Enable HA discovery | `true` |
| `ha_discovery.prefix` | HA discovery topic prefix | `homeassistant` |
| `ha_discovery.expire_after` | Seconds without an update before an entity is shown unavailable; `0` omits it, so entities never expire | `2.5 x read_interval` |
| `ha_discovery.device_info.name` | Device display name | `Aerosmart Gateway` |
| `ha_discovery.device_info.manufacturer` | Device manufacturer | `Drexel und Weiss` |
| `ha_discovery.device_info.model` | Device model | `aerosmartPI` |
| `ha_discovery.device_info.sw_version` | Software version (defaults to app version) | - |

## MQTT Topics

### Read Registers (Published)

The gateway publishes sensor values to the following topics:

- `aerosmart/luefterstatus` - Fan status (0-5)
- `aerosmart/lueftermode` - Fan mode (0-5)
- `aerosmart/co2` - CO2 level (ppm)
- `aerosmart/innentemp` - Indoor temperature (°C)
- `aerosmart/aussentemp` - Outdoor temperature (°C)
- `aerosmart/raumsolltemp` - Room setpoint temperature (°C)
- `aerosmart/zuluftumin` - Supply air RPM
- `aerosmart/abluftumin` - Exhaust air RPM
- `aerosmart/zuluftprozent` - Supply air percentage (%)
- `aerosmart/abluftprozent` - Exhaust air percentage (%)
- `aerosmart/zuluftsollvolumenstrom` - Supply air setpoint (m³/h)
- `aerosmart/abluftsollvolumenstrom` - Exhaust air setpoint (m³/h)
- And more... (see registers.yaml)

### Write Registers (Subscribed)

The gateway subscribes to the following topics for device control:

- `dw/aerosmart/luefterstufe` - Set fan stage (0-5) - *Topic changes detected and processed within 1-3 seconds*
- `dw/aerosmart/boilerheizstab` - Set boiler heating element (0-1) - *Topic changes detected and processed within 1-3 seconds*

> **Note:** The gateway implements immediate message detection with timestamp-based write priority signaling (auto-expires after 10 seconds), ensuring control commands are processed with minimal latency.

### Derived Registers

The gateway calculates and publishes derived values:

- `aerosmart/zuluftabluftprozent` - Supply/Exhaust percentage
- `aerosmart/co2luefterstufe4` - CO2 fan stage 4 (calculated from stage 3)
- `aerosmart/beschattungtemp_adjusted` - Adjusted shading temperature

## Home Assistant Integration

When `ha_discovery.enabled` is set to `true`, the gateway automatically publishes discovery configurations to Home Assistant. This creates:

- **Sensors**: For all read registers (temperature, CO2, fan speed, etc.)
- **Switches**: For write registers spanning exactly `0`-`1` (boiler heating element)
- **Numbers**: For write registers spanning a wider range (fan stage, `0`-`5`). Publishing these as switches made Home Assistant send `ON`/`OFF`, which the device rejects

The devices will appear in Home Assistant with the name "Aerosmart Gateway".

Example of Home Assistant dashboard:
![Home Assistant Dashboard](docs/aerosmart-ha.png)


## Application Flow

### Initialization

1. Load configuration from config.yaml
2. Load register definitions from registers.yaml
3. Initialize logger
4. Connect to serial device (with retry)
5. Connect to MQTT broker (with retry)
6. Publish Home Assistant discovery configs
7. Subscribe to write register topics
8. Start periodic read loop

### Periodic Read Cycle

1. A cycle runs at startup, then once per `read_interval`, measured from the end of the
   previous cycle so an overrunning readout still leaves the device an idle gap
2. The cycle is skipped if a write is in flight or the port is being recovered, and retried
   shortly afterwards rather than after a full interval
3. Registers are read sequentially: wait for a quiet bus → send command → read matching
   frame → apply divisor → validate range → confirm → publish. A reading is trusted when it
   is within `confirm_step` of the last trusted value; otherwise it is read again and
   trusted only if the two agree to within `confirm_step`, with a third read to break a tie. A register whose
   readings never agree is not published that cycle, so its previous value stands
4. Each write register's state topic is republished from its `state_from` register, so a
   mode the unit changed by itself - party mode expiring, or the panel being used - reaches
   Home Assistant, and the entities are restored after a restart
5. Derived values are computed from that same snapshot and published
6. A cancelled cycle publishes nothing, whether it was cancelled during the reads, after
   the last one, or partway through publishing. A write preempting the publish phase — 30+
   broker round trips — would otherwise have its fresher values overwritten by the rest of
   the stale snapshot

### Write Operation

1. MQTT message received on a write register's `subscribe_topic` and queued. Commands are
   applied by a dedicated worker in arrival order, never on the MQTT client's own callback
   goroutine, where blocking would stall inbound traffic including our own acknowledgements
2. The value is parsed and range-checked first, so a malformed payload never disturbs the
   read cycle
3. A duplicate of the last command accepted for that topic, within one second, is dropped
4. The read cycle is preempted, and released again on every exit path
5. The command is sent, with the value as parsed (`"03"` goes out as `3`). The unit applies
   writes but does not reliably echo them, so nothing waits for an echo
6. After `write_settle_delay`, the register named by `state_from` is read back. Its value —
   what the unit actually holds — is published to the write register's state topic, so Home
   Assistant shows reality rather than the request. Any `verify_registers` are read and
   published to their own topics too
7. If the read-back disagrees with the command, or nothing could be read back, the command
   is sent **once more** and read back again: a collision on the shared bus can corrupt a
   command on its way to the unit, and writes set an absolute value, so a re-send is
   harmless. A value still differing after that is reported as an error, with the actual
   value published. With `state_from` configured, a state that could not be read back is
   not published at all; without it, the commanded value is the only state there is
8. After `write_resettle_delay` the same registers are read once more, whatever the
   outcome of the write, because
   some values trail the command — the actual fan stage still reported the old value 60 ms
   after a stage change. Repeated commands restart that wait, so one read happens after the
   changes stop rather than one per command

### Reliability

The serial link is treated as lossy and shared: the unit's control panel talks on the same
bus, does not coordinate with the gateway, and the protocol carries no checksum.

- **Framing**: received bytes are buffered persistently, so a frame split across reads is
  reassembled rather than discarded. Discarding a partial frame leaves its tail to arrive
  alone, which desyncs every later frame.
- **Listen before talking**: a command is only transmitted once the bus has been quiet for
  `bus_quiet_gap`, and is drained before its reply is awaited. Only input is ever discarded;
  discarding output as well used to lose a command that had been written but not yet sent.
- **Frame matching**: a transaction reads until it sees a frame of exactly
  `<node> <register> <integer>` whose prefix matches its own command, discarding stale,
  unsolicited or corrupted frames. A lost newline that splices two frames, or a corrupted
  byte inside one, rejects it. The command is never re-sent to resync.
- **Confirmation**: a corrupted reply can still parse as a plausible number, so a reading
  that moved more than `confirm_step` from the last trusted value is only trusted once a
  second read agrees. Out-of-range readings are treated as corrupted, never clamped.
- **Retries**: one level, shared between failures and confirmation, bounded by a wall-clock
  budget per register, with jittered backoff so retries do not stay in step with the panel.
- **Port recovery**: closing and reopening the port is reserved for genuine port errors
  (`EIO`, `ENODEV`, `ENXIO`) and performed by a single supervisor with exponential backoff.
  A reopen toggles DTR/RTS, which many devices treat as a reset, so it is never used as a
  remedy for a timeout.
- **Write priority**: an incoming command cancels the in-flight read cycle and holds reads
  off until it completes, with a timeout so a failure cannot wedge polling.
- **Availability**: the gateway publishes `online`/`offline` to `mqtt.availability_topic` and
  registers it as the MQTT last will. Combined with `expire_after`, Home Assistant shows
  entities unavailable instead of a stale retained value. Readings that fail validation are
  not published.
- **MQTT**: every broker interaction is bounded by `mqtt.op_timeout`; connection loss triggers
  automatic reconnection and re-subscription. A clean session is used so the broker does not
  queue commands while the gateway is offline and replay them at startup, and retained
  messages on command topics are ignored, since the broker would hand them over again on
  every reconnect.

## Running as a Service

### systemd (Linux)

The repository ships a ready-to-use, hardened unit at [`docs/aerosmart.service`](docs/aerosmart.service) and an installer ([`docs/install.sh`](docs/install.sh)) that deploys the binary, config, and service. See the [Installation Guide](docs/INSTALLATION.md) for the full walkthrough.

To install the service manually:

```bash
sudo cp docs/aerosmart.service /etc/systemd/system/aerosmart.service
sudo systemctl daemon-reload
sudo systemctl enable --now aerosmart
```

The unit expects the binary and config under `/opt/aerosmart/` (`aerosmart-gateway`, `config.yaml`, `registers.yaml`) and gates startup on the serial device via `ConditionPathExists=/dev/ttyUSB0`.


## Development

### Running Tests

```bash
go test ./...
```

### Building with Docker

```dockerfile
FROM golang:1.27-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o aerosmart-gateway ./cmd

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/aerosmart-gateway .
COPY config.yaml .
COPY registers.yaml .
CMD ["./aerosmart-gateway", "-config", "config.yaml", "-registers", "registers.yaml"]
```

### Code Linting

```bash
golangci-lint run
```

## Registers Configuration

The `registers.yaml` file defines:

- **Read Registers**: Values to read from the device
- **Write Registers**: Values that can be written to the device
- **Derived Registers**: Calculated values based on read registers

Fields common to every register:

- `name`: Unique identifier, and the name other entries refer to
- `topic`: MQTT topic the value is published on
- `ha`: Home Assistant discovery configuration (`name`, and for read/derived registers
  `device_class` and `unit`)

Read registers additionally take:

- `command`: Serial command to send, as `<node> <register>`
- `divisor`: Value divisor (for float conversion)
- `type`: Value type (`integer` or `float`)
- `min_value` / `max_value`: Valid range; a reading outside it is not published
- `confirm_step`: How far a reading may move from the last trusted value and still be
  trusted without a second read, and how far two readings may differ and still confirm
  each other, in processed units. Default `0`: any change needs a second, identical
  reading. Measured values that drift between reads (fan speeds, CO2, temperatures) need a
  step, or their readings rarely agree; the shipped `registers.yaml` sets one for each

Write registers additionally take:

- `command_template`: Serial command with a `{value}` placeholder
- `subscribe_topic`: MQTT topic the gateway accepts commands on. This is also the Home
  Assistant command topic, so the two cannot drift apart
- `min_value` / `max_value`: Accepted range. A span of exactly `0`-`1` becomes a switch,
  anything wider becomes a number entity
- `state_from`: The read register that carries this register's real value. The unit reports
  a setpoint on a different address than the one it is written to — `luefterstufe` is
  written on `5002` and read back on `5003` — so this is what makes the state topic show
  what the unit actually holds rather than what was asked of it. Optional; without it the
  commanded value is published
- `verify_registers`: Read registers to read back and publish after a write, used to
  confirm the write took effect

Derived registers additionally take:

- `formula`: Expression evaluated over the current snapshot
- `sources`: The read registers the formula needs

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Serial communication library: [go.bug.st/serial](https://github.com/bugst/go-serial)
- MQTT client library: [eclipse/paho.mqtt.golang](https://github.com/eclipse/paho.mqtt.golang)
