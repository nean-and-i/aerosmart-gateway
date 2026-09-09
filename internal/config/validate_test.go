package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return path
}

func TestNewSerialDefaults(t *testing.T) {
	cfg, err := LoadAppConfig(writeConfig(t, "serial:\n  port: \"/dev/ttyUSB0\"\n"))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}

	if cfg.Serial.Retries != 3 {
		t.Errorf("Serial.Retries = %d, want 3", cfg.Serial.Retries)
	}
	if cfg.Serial.ResponseTimeout != 300 {
		t.Errorf("Serial.ResponseTimeout = %d, want 300", cfg.Serial.ResponseTimeout)
	}
	if cfg.Serial.ReadTimeout != 200 {
		t.Errorf("Serial.ReadTimeout = %d, want 200", cfg.Serial.ReadTimeout)
	}
	if cfg.MQTT.AvailabilityTopic != "aerosmart/status" {
		t.Errorf("MQTT.AvailabilityTopic = %q, want aerosmart/status", cfg.MQTT.AvailabilityTopic)
	}
}

// expire_after must outlast a read interval, or entities flap to unavailable
// between healthy cycles.
func TestExpireAfterDerivedFromReadInterval(t *testing.T) {
	cfg, err := LoadAppConfig(writeConfig(t, "read_interval: 120\n"))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}
	if cfg.HADiscovery.ExpireAfter != 300 {
		t.Errorf("ExpireAfter = %d, want 300 (2.5x the 120s interval)", cfg.HADiscovery.ExpireAfter)
	}
}

func TestExpireAfterExplicitValueKept(t *testing.T) {
	cfg, err := LoadAppConfig(writeConfig(t, "read_interval: 60\nha_discovery:\n  expire_after: 999\n"))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}
	if cfg.HADiscovery.ExpireAfter != 999 {
		t.Errorf("ExpireAfter = %d, want 999", cfg.HADiscovery.ExpireAfter)
	}
}

// Silently ignoring flow control lets XON/XOFF bytes be parsed as frame data.
func TestValidateRejectsUnsupportedFlowControl(t *testing.T) {
	cfg, err := LoadAppConfig(writeConfig(t, "serial:\n  xonxoff: true\n"))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}
	err = cfg.Validate()
	if err == nil {
		t.Fatal("Validate() = nil, want an error about xonxoff")
	}
	if !strings.Contains(err.Error(), "xonxoff") {
		t.Errorf("Validate() error = %v, want it to mention xonxoff", err)
	}
}

func TestValidateRejectsBadParity(t *testing.T) {
	cfg, err := LoadAppConfig(writeConfig(t, "serial:\n  parity: \"Z\"\n"))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() = nil, want an error about parity")
	}
}

func TestValidateRejectsNonPositiveReadInterval(t *testing.T) {
	cfg, err := LoadAppConfig(writeConfig(t, "read_interval: -5\n"))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() = nil, want an error about read_interval")
	}
}

func TestValidateAcceptsShippedConfig(t *testing.T) {
	cfg, err := LoadAppConfig(writeConfig(t, `
serial:
  port: "/dev/ttyUSB0"
  baudrate: 115200
  read_timeout: 200
mqtt:
  broker: "192.168.1.20"
  port: 1883
read_interval: 120
`))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

// Options that no longer exist must be reported, not silently ignored, or a
// user keeps tuning a setting that has no effect.
func TestDeprecatedKeysAreReported(t *testing.T) {
	cfg, err := LoadAppConfig(writeConfig(t, `
serial:
  max_retries: 10
  read_max_retries: 10
  device_response_delay: 40
`))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}

	warnings := cfg.DeprecatedKeys()
	if len(warnings) == 0 {
		t.Fatal("DeprecatedKeys() = empty, want the removed serial options listed")
	}
	joined := strings.Join(warnings, " ")
	for _, key := range []string{"max_retries", "read_max_retries", "device_response_delay"} {
		if !strings.Contains(joined, key) {
			t.Errorf("DeprecatedKeys() = %v, want it to mention %s", warnings, key)
		}
	}
}

func TestUnknownRegisterFieldsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registers.yaml")
	if err := os.WriteFile(path, []byte("read_registers:\n  - name: x\n    command: \"130 1\"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	cfg, err := LoadRegistersConfig(path)
	if err != nil {
		t.Fatalf("LoadRegistersConfig() error = %v", err)
	}
	// A register with no topic can never be published; that is a config bug.
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() = nil, want an error about the missing topic")
	}
}

func TestRegistersValidateRejectsDuplicateNames(t *testing.T) {
	cfg := &RegistersConfig{ReadRegisters: []ReadRegisterConfig{
		{Name: "a", Command: "130 1", Topic: "t/a", MaxValue: 1},
		{Name: "a", Command: "130 2", Topic: "t/a2", MaxValue: 1},
	}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() = nil, want a duplicate-name error")
	}
	if !strings.Contains(err.Error(), "a") {
		t.Errorf("Validate() error = %v, want it to name the duplicate", err)
	}
}

// A write register that verifies a register which does not exist would log a
// warning on every command; catch it at startup instead.
func TestRegistersValidateRejectsUnknownVerifyRegister(t *testing.T) {
	cfg := &RegistersConfig{
		ReadRegisters: []ReadRegisterConfig{{Name: "a", Command: "130 1", Topic: "t/a", MaxValue: 1}},
		WriteRegisters: []WriteRegisterConfig{{
			Name: "w", CommandTemplate: "130 2 {value}", SubscribeTopic: "cmd/w", Topic: "t/w",
			MaxValue: 1, VerifyRegisters: []string{"nonexistent"},
		}},
	}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "nonexistent") {
		t.Fatalf("Validate() error = %v, want it to name the unknown verify register", err)
	}
}

func TestRegistersValidateRequiresValuePlaceholder(t *testing.T) {
	cfg := &RegistersConfig{WriteRegisters: []WriteRegisterConfig{{
		Name: "w", CommandTemplate: "130 2", SubscribeTopic: "cmd/w", Topic: "t/w", MaxValue: 1,
	}}}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "{value}") {
		t.Fatalf("Validate() error = %v, want it to mention the missing {value} placeholder", err)
	}
}

func TestRegistersValidateRejectsInvertedRange(t *testing.T) {
	cfg := &RegistersConfig{ReadRegisters: []ReadRegisterConfig{
		{Name: "a", Command: "130 1", Topic: "t/a", MinValue: 10, MaxValue: 5},
	}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() = nil, want an error about min_value > max_value")
	}
}

func TestValidateErrorsAreWrapped(t *testing.T) {
	cfg := &AppConfig{}
	cfg.applyDefaults()
	cfg.Serial.XonXoff = true
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("Validate() error = %v, want it to wrap ErrInvalidConfig", err)
	}
}

// A config that omits the logging section must still log somewhere, or the
// gateway runs completely silent and field diagnosis is impossible.
func TestConsoleLoggingDefaultsOnWhenLoggingSectionAbsent(t *testing.T) {
	cfg, err := LoadAppConfig(writeConfig(t, "read_interval: 60\n"))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}
	if !cfg.Logging.ConsoleLogging {
		t.Error("Logging.ConsoleLogging = false with no logging section, want true")
	}
}

func TestConsoleLoggingCanBeDisabledExplicitly(t *testing.T) {
	cfg, err := LoadAppConfig(writeConfig(t, `
logging:
  console_logging: false
  file_logging: true
  log_file: "/tmp/aerosmart-test.log"
`))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}
	if cfg.Logging.ConsoleLogging {
		t.Error("Logging.ConsoleLogging = true when explicitly disabled, want false")
	}
}

// Disabling both sinks leaves no way to see what the gateway is doing.
func TestValidateRejectsAllLoggingDisabled(t *testing.T) {
	cfg, err := LoadAppConfig(writeConfig(t, `
mqtt:
  broker: "x"
logging:
  console_logging: false
  file_logging: false
`))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() = nil, want an error about having no log sink")
	}
}
