# Aerosmart Gateway - Installation Guide

This document provides detailed instructions for setting up and running the Aerosmart Gateway application.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation Methods](#installation-methods)
3. [Configuration](#configuration)
4. [Serial Device Setup](#serial-device-setup)
5. [MQTT Broker Setup](#mqtt-broker-setup)
6. [Running the Application](#running-the-application)
7. [Service Installation](#service-installation)
8. [Home Assistant Integration](#home-assistant-integration)
9. [Troubleshooting](#troubleshooting)
10. [Building from Source](#building-from-source)

---

## Prerequisites

### Hardware Requirements

- **Aerosmart M Ventilation Device**: The gateway communicates with Aerosmart M units via USB serial
- **Serial Cable**: USB cable to connect the Aerosmart device to your computer/server
- **Computer/Server**: A device to run the gateway (Raspberry Pi, Linux server, macOS, etc.)

### Software Requirements

- **Go 1.21 or later**: For building from source
- **MQTT Broker**: Any MQTT broker (Mosquitto, Home Assistant MQTT, etc.)
- **Serial Port Access**: The user running the application needs access to the serial port

### Operating System Support

- Linux (Ubuntu, Debian, Raspbian, etc.)
- macOS
- Raspberry Pi (Raspbian)

---

## Installation Methods

### Method 1: Pre-built Binary

Download the latest release for your platform from the releases page.

### Method 2: Build from Source

```bash
# Clone the repository
git clone https://github.com/nean/aerosmart-gateway.git
cd aerosmart-gateway

# Build the application
go build -o aerosmart-gateway ./cmd/main.go
```

### Method 3: Docker

```bash
# Build the Docker image
docker build -t aerosmart-gateway .

# Run the container
docker run -d \
  --name aerosmart-gateway \
  --device /dev/ttyUSB0:/dev/ttyUSB0 \
  -v $(pwd)/config.yaml:/app/config.yaml \
  -v $(pwd)/registers.yaml:/app/registers.yaml \
  aerosmart-gateway
```

---

## Configuration

### Configuration File Structure

Create a `config.yaml` file with the following structure:

```yaml
# Serial port configuration
serial:
  port: "/dev/ttyUSB0"           # Serial device path
  baudrate: 115200               # Communication speed
  read_timeout: 2                # Read timeout in seconds
  device_response_delay: 40      # Wait after write (ms)
  write_with_retry_delay: 10     # Write retry delay (ms)
  read_with_retry_delay: 100     # Read retry delay (ms)
  max_retries: 10                # Max operation retries
  max_reopens: 10                # Max port reopen attempts
  # Connection retry settings
  connect_retry_initial_delay_ms: 2
  connect_retry_max_delay_ms: 400
  connect_retry_jitter_percent: 25

# MQTT broker configuration
mqtt:
  broker: "192.168.1.20"         # MQTT broker address
  port: 1883                     # MQTT broker port
  username: "mqtt"               # MQTT username
  password: "your_password"      # MQTT password
  client_id: "aerosmart-gateway" # Client identifier
  qos: 0                         # Quality of Service (0, 1, or 2)
  retain: true                   # Retain messages
  # Connection retry settings
  connect_retry_initial_delay_ms: 500
  connect_retry_max_delay_ms: 30000
  connect_retry_jitter_percent: 25

# Device ID for Home Assistant
device_id: "aerosmart"

# Logging configuration
log_level: "debug"               # debug, info, warn, error

logging:
  log_file: "/var/log/aerosmart.log"  # Log file path (empty to disable)
  file_logging: false             # Enable file logging
  console_logging: true           # Enable console logging

# Read interval in seconds
read_interval: 60

# Home Assistant MQTT Discovery
ha_discovery:
  enabled: true
  prefix: "homeassistant"
```

### Registers Configuration

The `registers.yaml` file defines which registers to read and write. The default configuration includes:

**Read Registers** (sensors):
- Fan status and mode
- CO2 levels
- Temperature sensors (indoor, outdoor, room setpoint, boiler)
- Fan speeds (RPM and percentage)
- Status flags (heat pump, shading, etc.)

**Write Registers** (controls):
- Fan stage (0-5)
- Boiler heating element (0-1)

**Derived Registers** (calculated):
- Supply/Exhaust percentage
- CO2 fan stage 4
- Adjusted shading temperature

---

## Serial Device Setup

### Linux (Ubuntu/Debian/Raspbian)

1. **Identify the serial port**:

```bash
# List available serial ports
ls -l /dev/ttyUSB*
ls -l /dev/ttyACM*
```

2. **Check permissions**:

```bash
# Check current permissions
ls -l /dev/ttyUSB0

# Add user to dialout group (if needed)
sudo usermod -a -G dialout $USER
```

3. **Logout and login** for group changes to take effect, or use:

```bash
newgrp dialout
```

### macOS

1. **Identify the serial port**:

```bash
# List available serial ports
ls /dev/tty.usbserial*
ls /dev/tty.usbserial-*
```

2. **No special permissions needed** - macOS typically allows access to USB serial devices without additional configuration.

### Finding the Correct Port

If you have multiple serial devices, identify the correct one:

```bash
# With the device disconnected
ls /dev/ttyUSB*

# Connect the device
ls /dev/ttyUSB*

# The new entry is your Aerosmart device
```

---

## MQTT Broker Setup

### Installing Mosquitto (Linux)

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install mosquitto mosquitto-clients

# Start Mosquitto
sudo systemctl start mosquitto
sudo systemctl enable mosquitto
```

### Configuring Mosquitto

Edit `/etc/mosquitto/mosquitto.conf`:

```conf
# Allow anonymous access (for testing)
allow_anonymous true

# Set listener port
listener 1883

# Persistence configuration
persistence true
persistence_location /var/lib/mosquitto/
```

### Testing MQTT Connection

```bash
# Subscribe to a test topic
mosquitto_sub -t test

# Publish to a test topic (from another terminal)
mosquitto_pub -t test -m "Hello World"
```

### Using Home Assistant MQTT

If you're using Home Assistant with the built-in MQTT broker:

1. Go to Home Assistant Settings → Integrations
2. Find MQTT and configure it
3. Use the broker address (usually `localhost` if running on the same machine)
4. Use your Home Assistant MQTT credentials

---

## Running the Application

### Basic Run

```bash
./aerosmart-gateway -config config.yaml -registers registers.yaml
```

### With Custom Paths

```bash
./aerosmart-gateway \
  -config /path/to/config.yaml \
  -registers /path/to/registers.yaml
```

### Verify It's Working

Check the logs for successful connection:

```
INFO Starting Aerosmart Gateway
INFO Connecting to serial device /dev/ttyUSB0...
INFO Serial device connected
INFO Connecting to MQTT broker 192.168.1.20:1883...
INFO MQTT broker connected
INFO Publishing Home Assistant discovery configs...
INFO Home Assistant discovery configs published
INFO Starting periodic read loop (interval: 60 seconds)
INFO === Triggering full register readout ===
INFO === SERIAL: Starting register read cycle ===
INFO === SERIAL: Register read cycle completed in 1.234s ===
```

### Testing MQTT Messages

Subscribe to all Aerosmart topics:

```bash
mosquitto_sub -t "aerosmart/#" -v
```

Publish a write command:

```bash
mosquitto_pub -t "dw/aerosmart/luefterstufe" -m "3"
```

---

## Service Installation

### systemd (Linux)

1. **Create the service file**:

```bash
sudo nano /etc/systemd/system/aerosmart.service
```

2. **Add the following content**:

```ini
[Unit]
Description=Aerosmart Gateway - Connects Aerosmart ventilation to MQTT
After=network.target
Wants=network.target

[Service]
Type=simple
User=root
ExecStart=/opt/aerosmart/aerosmart-gateway --config /opt/aerosmart/config.yaml --registers /opt/aerosmart/registers.yaml
Restart=on-failure
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

3. **Install the application**:

```bash
# Create the installation directory
sudo mkdir -p /opt/aerosmart

# Copy the binary and configuration
sudo cp aerosmart-gateway /opt/aerosmart/
sudo cp config.yaml /opt/aerosmart/
sudo cp registers.yaml /opt/aerosmart/

# Set permissions
sudo chmod +x /opt/aerosmart/aerosmart-gateway
```

4. **Enable and start the service**:

```bash
sudo systemctl daemon-reload
sudo systemctl enable aerosmart
sudo systemctl start aerosmart
```

5. **Check status**:

```bash
sudo systemctl status aerosmart
journalctl -u aerosmart -f
```

### SysVinit (Older Linux)

1. **Copy the init script**:

```bash
sudo cp init-script.sh /etc/init.d/aerosmart
sudo chmod +x /etc/init.d/aerosmart
```

2. **Configure to start on boot**:

```bash
# For Debian/Raspbian
sudo update-rc.d aerosmart defaults

# For Red Hat/CentOS
sudo chkconfig --add aerosmart
```

3. **Start the service**:

```bash
sudo service aerosmart start
```

### Launchd (macOS)

1. **Create the plist file**:

```bash
nano ~/Library/LaunchAgents/com.aerosmart.gateway.plist
```

2. **Add the following content**:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.aerosmart.gateway</string>
    <key>ProgramArguments</key>
    <array>
        <string>/path/to/aerosmart-gateway</string>
        <string>-config</string>
        <string>/path/to/config.yaml</string>
        <string>-registers</string>
        <string>/path/to/registers.yaml</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/var/log/aerosmart.log</string>
    <key>StandardErrorPath</key>
    <string>/var/log/aerosmart.log</string>
</dict>
</plist>
```

3. **Load the service**:

```bash
launchctl load ~/Library/LaunchAgents/com.aerosmart.gateway.plist
```

4. **Check status**:

```bash
launchctl list | grep aerosmart
```

---

## Home Assistant Integration

### Automatic Discovery

When `ha_discovery.enabled: true` is set in the configuration, the gateway automatically publishes discovery configurations to Home Assistant.

### What Gets Created

**Sensors** (for each read register):
- Fan status and mode
- CO2 levels
- Temperatures (indoor, outdoor, room, boiler)
- Fan speeds and percentages
- Status flags

**Switches** (for each write register):
- Fan stage control
- Boiler heating element control

### Manual Configuration

If you prefer manual configuration, you can:

1. Disable auto-discovery: `ha_discovery.enabled: false`
2. Create entities manually in Home Assistant
3. Use the MQTT topics directly

### Verifying Discovery

Check the MQTT topics for discovery messages:

```bash
mosquitto_sub -t "homeassistant/#" -v
```

You should see config messages for each sensor and switch.

---

## Troubleshooting

### Serial Port Issues

**Problem**: "Failed to open serial port"

```bash
# Check if the device exists
ls -l /dev/ttyUSB0

# Check permissions
ls -l /dev/ttyUSB0
groups

# Add to dialout group
sudo usermod -a -G dialout $USER
```

**Problem**: "Read timeout"

- Check the serial cable connection
- Verify the correct baud rate (115200)
- Try a different USB port

### MQTT Connection Issues

**Problem**: "Failed to connect to MQTT broker"

- Verify the broker is running: `systemctl status mosquitto`
- Check the broker address and port
- Verify username and password
- Check firewall settings

**Problem**: "MQTT connection lost"

- The broker may have restarted
- Check network connectivity
- The application will automatically reconnect

### Data Issues

**Problem**: Invalid or missing values

- Check the device is powered on
- Verify the register configuration matches your device
- Increase log level to debug for more information

**Problem**: Values out of range

- This may indicate a sensor issue
- Check the device documentation
- The application validates and marks invalid values

### Performance Issues

**Problem**: Slow response to write commands

- Check the read_interval (lower value = more frequent reads)
- The write priority mechanism should give control commands priority

**Problem**: High CPU usage

- Check log level (debug generates more output)
- Verify no infinite loops in the application

### Logs

Enable debug logging for troubleshooting:

```yaml
log_level: "debug"
```

View logs:

```bash
# Systemd
journalctl -u aerosmart -f

# Direct run
./aerosmart-gateway -config config.yaml -registers registers.yaml
```

---

## Building from Source

### Development Setup

```bash
# Install Go 1.21 or later
# https://go.dev/doc/install

# Clone the repository
git clone https://github.com/nean/aerosmart-gateway.git
cd aerosmart-gateway

# Download dependencies
go mod download

# Run tests
go test ./...

# Build
go build -o aerosmart-gateway ./cmd/main.go
```

### Cross-Compilation for Raspberry Pi

```bash
# Build for ARMv6 (Raspberry Pi Zero)
GOOS=linux GOARCH=arm GOARM=6 go build -o aerosmart-gateway-armv6 ./cmd/main.go

# Build for ARMv7 (Raspberry Pi 2/3)
GOOS=linux GOARCH=arm GOARM=7 go build -o aerosmart-gateway-armv7 ./cmd/main.go

# Build for ARM64 (Raspberry Pi 4)
GOOS=linux GOARCH=arm64 go build -o aerosmart-gateway-arm64 ./cmd/main.go
```

### Docker Build

```bash
# Build the image
docker build -t aerosmart-gateway .

# Run the container
docker run -d \
  --name aerosmart-gateway \
  --device /dev/ttyUSB0:/dev/ttyUSB0 \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -v $(pwd)/registers.yaml:/app/registers.yaml:ro \
  aerosmart-gateway
```

---

## Security Considerations

1. **MQTT Credentials**: Store MQTT passwords securely, not in plain text configuration files
2. **Serial Access**: Ensure only trusted users have access to the serial port
3. **Network Security**: If the MQTT broker is accessible over the network, consider using TLS
4. **Log Files**: Be aware that log files may contain sensitive information

---

## Support

For issues and questions:

1. Check the [troubleshooting section](#troubleshooting)
2. Review the [application flow documentation](docs/APPLICATION_FLOW.md)
3. Review the [timing diagrams](docs/TIMING_DIAGRAMS.md)
4. Open an issue on GitHub
