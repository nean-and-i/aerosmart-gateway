package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAppConfig(t *testing.T) {
	// Create a temporary config file
	content := `
serial:
  port: "/dev/ttyUSB1"
  baudrate: 57600
  bytesize: 8
  parity: "N"
  stopbits: 1
  timeout: 5
  xonxoff: true
  dsrdtr: false
  write_timeout: 15

mqtt:
  broker: "localhost"
  port: 1884
  username: "testuser"
  password: "testpass"
  client_id: "test-client"
  qos: 1
  retain: false

device_id: "test-device"
log_level: "debug"
read_interval: 30

ha_discovery:
  enabled: true
  prefix: "homeassistant"
`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	// Test loading
	cfg, err := LoadAppConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify serial config
	if cfg.Serial.Port != "/dev/ttyUSB1" {
		t.Errorf("Expected port /dev/ttyUSB1, got %s", cfg.Serial.Port)
	}
	if cfg.Serial.BaudRate != 57600 {
		t.Errorf("Expected baudrate 57600, got %d", cfg.Serial.BaudRate)
	}
	if cfg.Serial.Timeout != 5 {
		t.Errorf("Expected timeout 5, got %d", cfg.Serial.Timeout)
	}
	if !cfg.Serial.XonXoff {
		t.Error("Expected xonxoff to be true")
	}

	// Verify MQTT config
	if cfg.MQTT.Broker != "localhost" {
		t.Errorf("Expected broker localhost, got %s", cfg.MQTT.Broker)
	}
	if cfg.MQTT.Port != 1884 {
		t.Errorf("Expected port 1884, got %d", cfg.MQTT.Port)
	}
	if cfg.MQTT.Username != "testuser" {
		t.Errorf("Expected username testuser, got %s", cfg.MQTT.Username)
	}
	if cfg.MQTT.Password != "testpass" {
		t.Errorf("Expected password testpass, got %s", cfg.MQTT.Password)
	}
	if cfg.MQTT.ClientID != "test-client" {
		t.Errorf("Expected client_id test-client, got %s", cfg.MQTT.ClientID)
	}
	if cfg.MQTT.QOS != 1 {
		t.Errorf("Expected qos 1, got %d", cfg.MQTT.QOS)
	}
	if cfg.MQTT.Retain {
		t.Error("Expected retain to be false")
	}

	// Verify other config
	if cfg.DeviceID != "test-device" {
		t.Errorf("Expected device_id test-device, got %s", cfg.DeviceID)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("Expected log_level debug, got %s", cfg.LogLevel)
	}
	if cfg.ReadInterval != 30 {
		t.Errorf("Expected read_interval 30, got %d", cfg.ReadInterval)
	}
	if !cfg.HADiscovery.Enabled {
		t.Error("Expected ha_discovery enabled")
	}
}

func TestLoadAppConfigDefaults(t *testing.T) {
	// Create a minimal config file
	content := `
device_id: "my-device"
`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	// Test loading with defaults
	cfg, err := LoadAppConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify defaults
	if cfg.Serial.Port != "/dev/ttyUSB0" {
		t.Errorf("Expected default port /dev/ttyUSB0, got %s", cfg.Serial.Port)
	}
	if cfg.Serial.BaudRate != 115200 {
		t.Errorf("Expected default baudrate 115200, got %d", cfg.Serial.BaudRate)
	}
	if cfg.DeviceID != "my-device" {
		t.Errorf("Expected device_id my-device, got %s", cfg.DeviceID)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("Expected default log_level info, got %s", cfg.LogLevel)
	}
	if cfg.ReadInterval != 60 {
		t.Errorf("Expected default read_interval 60, got %d", cfg.ReadInterval)
	}

	// Verify device info defaults
	if cfg.HADiscovery.DeviceInfo.Name != "Aerosmart Gateway" {
		t.Errorf("Expected default device info name 'Aerosmart Gateway', got %s", cfg.HADiscovery.DeviceInfo.Name)
	}
	if cfg.HADiscovery.DeviceInfo.Manufacturer != "Drexel und Weiss" {
		t.Errorf("Expected default device info manufacturer 'Drexel und Weiss', got %s", cfg.HADiscovery.DeviceInfo.Manufacturer)
	}
	if cfg.HADiscovery.DeviceInfo.Model != "aerosmartPI" {
		t.Errorf("Expected default device info model 'aerosmartPI', got %s", cfg.HADiscovery.DeviceInfo.Model)
	}
}

func TestLoadRegistersConfig(t *testing.T) {
	content := `
read_registers:
  - name: "test_register"
    command: "130 100"
    topic: "aerosmart/test"
    divisor: 1000
    type: "float"
    min_value: 0
    max_value: 100
    ha:
      name: "Test Register"
      device_class: "temperature"
      unit: "°C"

write_registers:
  - name: "fan_speed"
    command_template: "130 5002 {value}"
    subscribe_topic: "dw/aerosmart/fan_speed"
    topic: "aerosmart/fan_speed"
    min_value: 0
    max_value: 5
    ha:
      name: "Fan Speed"
      command_topic: "dw/aerosmart/fan_speed"

derived_registers:
  - name: "test_derived"
    topic: "aerosmart/test_derived"
    formula: "test * 2"
    sources: ["test_register"]
    ha:
      name: "Test Derived"
      unit: "test"
`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "registers.yaml")
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	// Test loading
	cfg, err := LoadRegistersConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load registers config: %v", err)
	}

	// Verify read registers
	if len(cfg.ReadRegisters) != 1 {
		t.Fatalf("Expected 1 read register, got %d", len(cfg.ReadRegisters))
	}
	if cfg.ReadRegisters[0].Name != "test_register" {
		t.Errorf("Expected name test_register, got %s", cfg.ReadRegisters[0].Name)
	}
	if cfg.ReadRegisters[0].Command != "130 100" {
		t.Errorf("Expected command 130 100, got %s", cfg.ReadRegisters[0].Command)
	}
	if cfg.ReadRegisters[0].Divisor != 1000 {
		t.Errorf("Expected divisor 1000, got %d", cfg.ReadRegisters[0].Divisor)
	}
	if cfg.ReadRegisters[0].Type != "float" {
		t.Errorf("Expected type float, got %s", cfg.ReadRegisters[0].Type)
	}
	if cfg.ReadRegisters[0].HA.Name != "Test Register" {
		t.Errorf("Expected HA name Test Register, got %s", cfg.ReadRegisters[0].HA.Name)
	}

	// Verify write registers
	if len(cfg.WriteRegisters) != 1 {
		t.Fatalf("Expected 1 write register, got %d", len(cfg.WriteRegisters))
	}
	if cfg.WriteRegisters[0].Name != "fan_speed" {
		t.Errorf("Expected name fan_speed, got %s", cfg.WriteRegisters[0].Name)
	}
	if cfg.WriteRegisters[0].CommandTemplate != "130 5002 {value}" {
		t.Errorf("Expected command_template 130 5002 {value}, got %s", cfg.WriteRegisters[0].CommandTemplate)
	}

	// Verify derived registers
	if len(cfg.DerivedRegisters) != 1 {
		t.Fatalf("Expected 1 derived register, got %d", len(cfg.DerivedRegisters))
	}
	if cfg.DerivedRegisters[0].Name != "test_derived" {
		t.Errorf("Expected name test_derived, got %s", cfg.DerivedRegisters[0].Name)
	}
}

func TestLoadAppConfigFileNotFound(t *testing.T) {
	_, err := LoadAppConfig("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("Expected error for non-existent config file")
	}
}

func TestLoadRegistersConfigFileNotFound(t *testing.T) {
	_, err := LoadRegistersConfig("/nonexistent/path/registers.yaml")
	if err == nil {
		t.Error("Expected error for non-existent registers file")
	}
}
