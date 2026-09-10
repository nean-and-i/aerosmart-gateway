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

// Flow control the driver cannot honour is reported, but it must not stop the
// gateway: it was equally inert in earlier releases, so failing here would
// break an upgrade without making anything safer.
func TestUnsupportedFlowControlWarnsRatherThanFails(t *testing.T) {
	cfg, err := LoadAppConfig(writeConfig(t, "mqtt:\n  broker: \"x\"\nserial:\n  xonxoff: true\n  dsrdtr: true\n"))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want flow control to be a warning", err)
	}

	warnings := strings.Join(cfg.Warnings(), "\n")
	for _, key := range []string{"xonxoff", "dsrdtr"} {
		if !strings.Contains(warnings, key) {
			t.Errorf("Warnings() = %q, want it to mention %s", warnings, key)
		}
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
func TestRemovedKeysAreReported(t *testing.T) {
	cfg, err := LoadAppConfig(writeConfig(t, `
serial:
  max_retries: 10
  read_max_retries: 10
  device_response_delay: 40
`))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}

	warnings := cfg.Warnings()
	if len(warnings) == 0 {
		t.Fatal("Warnings() = empty, want the removed serial options listed")
	}
	joined := strings.Join(warnings, " ")
	for _, key := range []string{"max_retries", "read_max_retries", "device_response_delay"} {
		if !strings.Contains(joined, key) {
			t.Errorf("Warnings() = %v, want it to mention %s", warnings, key)
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
	cfg.Serial.Parity = "Z"
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

// The budget guard must account for the whole cycle: a per-register figure can
// never exceed a read interval and so never fires.
func TestValidateCycleBudgetUsesRegisterCount(t *testing.T) {
	cfg, err := LoadAppConfig(writeConfig(t, `
mqtt:
  broker: "x"
read_interval: 30
serial:
  retries: 3
  response_timeout: 300
  retry_backoff: 100
`))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}

	// 3 attempts x 400ms = 1.2s per register; 40 registers is 48s > 30s.
	if err := cfg.ValidateCycleBudget(40); err == nil {
		t.Fatal("ValidateCycleBudget(40) = nil, want an error: the worst-case cycle overruns the interval")
	}
	// 10 registers is 12s, comfortably inside 30s.
	if err := cfg.ValidateCycleBudget(10); err != nil {
		t.Errorf("ValidateCycleBudget(10) error = %v, want nil", err)
	}
}

func TestValidateCycleBudgetIgnoresZeroRegisters(t *testing.T) {
	cfg, err := LoadAppConfig(writeConfig(t, "mqtt:\n  broker: \"x\"\n"))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}
	if err := cfg.ValidateCycleBudget(0); err != nil {
		t.Errorf("ValidateCycleBudget(0) error = %v, want nil", err)
	}
}

// The serial layer raises response_timeout to read_timeout when it is smaller,
// so the budget must be computed from the effective value, not the configured
// one, or a large read_timeout slips past the check.
func TestValidateCycleBudgetUsesEffectiveResponseTimeout(t *testing.T) {
	cfg, err := LoadAppConfig(writeConfig(t, `
mqtt:
  broker: "x"
read_interval: 120
serial:
  retries: 3
  read_timeout: 3000
  response_timeout: 300
  retry_backoff: 100
`))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}

	// Effective response timeout is 3000ms, so 29 x 3 x 3100ms = 269s > 120s.
	if err := cfg.ValidateCycleBudget(29); err == nil {
		t.Fatal("ValidateCycleBudget(29) = nil; read_timeout 3000ms makes the real cycle 269s")
	}
}

// Negative millisecond settings make the retry and reopen loops spin.
func TestValidateRejectsNegativeDurations(t *testing.T) {
	for _, body := range []string{
		"serial:\n  reopen_backoff: -1\n",
		"serial:\n  retry_backoff: -5\n",
		"serial:\n  response_timeout: -100\n",
		"serial:\n  read_timeout: -100\n",
		"serial:\n  health_check_interval: -1\n",
		"serial:\n  write_priority_timeout: -1\n",
		"mqtt:\n  broker: \"x\"\n  op_timeout: -1\n",
	} {
		t.Run(body, func(t *testing.T) {
			prefix := ""
			if !strings.Contains(body, "broker") {
				prefix = "mqtt:\n  broker: \"x\"\n"
			}
			cfg, err := LoadAppConfig(writeConfig(t, prefix+body))
			if err != nil {
				t.Fatalf("LoadAppConfig() error = %v", err)
			}
			if err := cfg.Validate(); err == nil {
				t.Errorf("Validate() = nil for %q, want a rejection", body)
			}
		})
	}
}

// One Transact attempt can start a read just before its deadline and then
// block for a whole read_timeout, so the budget must charge for both.
func TestValidateCycleBudgetChargesReadTimeoutPerAttempt(t *testing.T) {
	cfg, err := LoadAppConfig(writeConfig(t, `
mqtt:
  broker: "x"
read_interval: 61
serial:
  retries: 3
  read_timeout: 500
  response_timeout: 600
  retry_backoff: 0
`))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}

	// Charging response_timeout alone gives 29 x 3 x 600ms = 52.2s, which fits
	// 61s. The real cost is 3 x 1100ms per register, about 96s.
	if err := cfg.ValidateCycleBudget(29); err == nil {
		t.Fatal("ValidateCycleBudget(29) = nil; the real cycle overruns the 61s interval")
	}
}

// A maximum below the initial value collapses the startup backoff to nothing
// and hammers the device node in a tight loop.
func TestValidateRejectsReopenMaxBelowInitial(t *testing.T) {
	cfg, err := LoadAppConfig(writeConfig(t, `
mqtt:
  broker: "x"
serial:
  reopen_backoff: 500
  reopen_max_backoff: 1
`))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() = nil, want a rejection of reopen_max_backoff < reopen_backoff")
	}
}

// The configuration shipped before the serial rework must still start the
// gateway. Every setting it carries that no longer applies is inert, so an
// upgrade should warn, not refuse to run.
func TestPreviousReleaseConfigStillStarts(t *testing.T) {
	// xonxoff was silently ignored by the old code too: the driver has never
	// implemented flow control, so the link has always run without it.
	appBody := `
serial:
  port: "/dev/ttyUSB0"
  baudrate: 115200
  xonxoff: true
  dsrdtr: false
  timeout: 0
  write_timeout: 1
  read_timeout: 10
  max_reopens: 3
  deadline_timeout: 150
  write_with_retry_delay: 10
  device_response_delay: 40
  read_with_retry_delay: 100
  read_max_retries: 10
  write_max_retries: 10
  max_retries: 10
  connect_retry_initial_delay_ms: 2
  connect_retry_max_delay_ms: 400
  connect_retry_jitter_percent: 25
mqtt:
  broker: "192.168.1.20"
  port: 1883
  qos: 1
  retain: true
read_interval: 120
`
	cfg, err := LoadAppConfig(writeConfig(t, appBody))
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() rejected the previous release's config:\n%v", err)
	}

	warnings := strings.Join(cfg.Warnings(), "\n")
	if !strings.Contains(warnings, "xonxoff") {
		t.Errorf("Warnings() = %q, want it to flag xonxoff as having no effect", warnings)
	}
	for _, key := range []string{"max_retries", "device_response_delay", "max_reopens"} {
		if !strings.Contains(warnings, key) {
			t.Errorf("Warnings() does not mention removed option %s", key)
		}
	}
}

// A write register sharing a read register's state topic makes two Home
// Assistant entities publish to one topic. That is worth fixing, but the
// gateway runs, so it must not block an upgrade.
func TestSharedStateTopicWarnsRatherThanFails(t *testing.T) {
	cfg := &RegistersConfig{
		ReadRegisters: []ReadRegisterConfig{
			{Name: "lueftermode", Command: "130 5003", Topic: "aerosmart/lueftermode", MaxValue: 5},
		},
		WriteRegisters: []WriteRegisterConfig{{
			Name: "luefterstufe", CommandTemplate: "130 5002 {value}",
			SubscribeTopic: "dw/aerosmart/luefterstufe",
			Topic:          "aerosmart/lueftermode", // collides
			MaxValue:       5,
		}},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want the shared topic to be a warning", err)
	}

	warnings := strings.Join(cfg.Warnings(), "\n")
	if !strings.Contains(warnings, "luefterstufe") || !strings.Contains(warnings, "lueftermode") {
		t.Errorf("Warnings() = %q, want it to name both registers", warnings)
	}
}

// Configurations that genuinely cannot work must still be refused.
func TestValidateStillRefusesUnusableConfig(t *testing.T) {
	cases := map[string]string{
		"bad parity":       "serial:\n  parity: \"Z\"\nmqtt:\n  broker: \"x\"\n",
		"no broker":        "read_interval: 60\n",
		"zero interval":    "mqtt:\n  broker: \"x\"\nread_interval: -1\n",
		"negative backoff": "mqtt:\n  broker: \"x\"\nserial:\n  retry_backoff: -1\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			cfg, err := LoadAppConfig(writeConfig(t, body))
			if err != nil {
				t.Fatalf("LoadAppConfig() error = %v", err)
			}
			if err := cfg.Validate(); err == nil {
				t.Error("Validate() = nil, want a rejection")
			}
		})
	}
}

func TestRegistersValidateStillRefusesUnusableConfig(t *testing.T) {
	cfg := &RegistersConfig{WriteRegisters: []WriteRegisterConfig{{
		Name: "w", CommandTemplate: "130 2", SubscribeTopic: "", Topic: "t/w", MaxValue: 1,
	}}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() = nil, want rejection of a missing {value} and subscribe_topic")
	}
}
