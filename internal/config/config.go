package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// ErrInvalidConfig wraps every configuration validation failure.
var ErrInvalidConfig = errors.New("invalid configuration")

// SerialConfig holds serial port configuration.
//
// Timeouts are milliseconds. Note that the kernel expresses a serial read
// timeout in deciseconds, so ReadTimeout is rounded up to a multiple of 100ms
// by the serial layer.
type SerialConfig struct {
	Port     string `yaml:"port"`
	BaudRate int    `yaml:"baudrate"`
	ByteSize int    `yaml:"bytesize"` // 5-8
	Parity   string `yaml:"parity"`   // N, O or E
	StopBits int    `yaml:"stopbits"` // 1 or 2

	// ReadTimeout is how long a single read may block, in milliseconds.
	// Rounded up to a multiple of 100 (kernel VTIME granularity).
	ReadTimeout int `yaml:"read_timeout"`

	// ResponseTimeout is the budget for one command/response exchange, in
	// milliseconds.
	ResponseTimeout int `yaml:"response_timeout"`

	// Retries is the total number of attempts per register operation. This is
	// the only retry level: nesting retries multiplies the worst case.
	Retries int `yaml:"retries"`

	// RetryBackoff is the delay between attempts, in milliseconds.
	RetryBackoff int `yaml:"retry_backoff"`

	// ReopenBackoff and ReopenMaxBackoff bound how fast the supervisor
	// reopens a failed port, in milliseconds.
	ReopenBackoff    int `yaml:"reopen_backoff"`
	ReopenMaxBackoff int `yaml:"reopen_max_backoff"`

	// HealthCheckInterval is how often port health is polled, in milliseconds.
	HealthCheckInterval int `yaml:"health_check_interval"`

	// WritePriorityTimeout bounds how long an in-flight write may hold reads
	// off, in milliseconds.
	WritePriorityTimeout int `yaml:"write_priority_timeout"`

	// WriteSettleDelay is how long to wait after writing a register before
	// reading it back, in milliseconds. The unit does not reliably echo a
	// write, so the read-back is what confirms it.
	WriteSettleDelay int `yaml:"write_settle_delay"`

	// Flow control. The tarm/serial driver supports neither, so a true value
	// is rejected by Validate rather than silently ignored.
	XonXoff bool `yaml:"xonxoff"`
	DsrDtr  bool `yaml:"dsrdtr"`

	// Deprecated: retained only so their presence can be reported. They were
	// part of a nested retry scheme that could stall a cycle for hours.
	DeprecatedTimeout             *int `yaml:"timeout"`
	DeprecatedWriteTimeout        *int `yaml:"write_timeout"`
	DeprecatedMaxReopens          *int `yaml:"max_reopens"`
	DeprecatedDeadlineTimeout     *int `yaml:"deadline_timeout"`
	DeprecatedWriteWithRetryDelay *int `yaml:"write_with_retry_delay"`
	DeprecatedDeviceResponseDelay *int `yaml:"device_response_delay"`
	DeprecatedReadWithRetryDelay  *int `yaml:"read_with_retry_delay"`
	DeprecatedReadMaxRetries      *int `yaml:"read_max_retries"`
	DeprecatedWriteMaxRetries     *int `yaml:"write_max_retries"`
	DeprecatedMaxRetries          *int `yaml:"max_retries"`
	DeprecatedConnectInitialDelay *int `yaml:"connect_retry_initial_delay_ms"`
	DeprecatedConnectMaxDelay     *int `yaml:"connect_retry_max_delay_ms"`
	DeprecatedConnectJitter       *int `yaml:"connect_retry_jitter_percent"`
}

// MQTTConfig holds MQTT broker configuration.
type MQTTConfig struct {
	Broker   string `yaml:"broker"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	ClientID string `yaml:"client_id"`
	QOS      int    `yaml:"qos"`
	Retain   bool   `yaml:"retain"`

	// AvailabilityTopic carries online/offline and is registered as the last
	// will, so Home Assistant shows entities unavailable instead of stale.
	AvailabilityTopic string `yaml:"availability_topic"`

	// OpTimeout bounds every broker interaction, in milliseconds. Publishes
	// happen on the serial read path, so an unbounded wait stalls polling.
	OpTimeout int `yaml:"op_timeout"`

	PublishRetryCount          int `yaml:"publish_retry_count"`
	ConnectRetryInitialDelayMs int `yaml:"connect_retry_initial_delay_ms"`
	ConnectRetryMaxDelayMs     int `yaml:"connect_retry_max_delay_ms"`
	ConnectRetryJitterPercent  int `yaml:"connect_retry_jitter_percent"`
}

// DeviceInfoConfig holds MQTT device info for HA discovery.
type DeviceInfoConfig struct {
	Name         string `yaml:"name"`
	Manufacturer string `yaml:"manufacturer"`
	Model        string `yaml:"model"`
	SWVersion    string `yaml:"sw_version"`
}

// HADiscoveryConfig holds Home Assistant discovery configuration.
type HADiscoveryConfig struct {
	Enabled    bool             `yaml:"enabled"`
	Prefix     string           `yaml:"prefix"`
	DeviceInfo DeviceInfoConfig `yaml:"device_info"`

	// ExpireAfter marks an entity unavailable after this many seconds without
	// an update. Defaults to 2.5x the read interval.
	ExpireAfter int `yaml:"expire_after"`
}

// LoggingConfig holds logging configuration.
//
// ConsoleLogging is resolved from ConsoleLoggingSet, which is a pointer so an
// omitted key can be told apart from an explicit false. Defaulting it to false
// would make a config without a logging section run silently.
type LoggingConfig struct {
	LogFile           string `yaml:"log_file"`
	FileLogging       bool   `yaml:"file_logging"`
	ConsoleLoggingSet *bool  `yaml:"console_logging"`

	// ConsoleLogging is the resolved value, filled in by applyDefaults.
	ConsoleLogging bool `yaml:"-"`
}

// AppConfig holds the main application configuration.
type AppConfig struct {
	Serial       SerialConfig      `yaml:"serial"`
	MQTT         MQTTConfig        `yaml:"mqtt"`
	DeviceID     string            `yaml:"device_id"`
	LogLevel     string            `yaml:"log_level"`
	Logging      LoggingConfig     `yaml:"logging"`
	ReadInterval int               `yaml:"read_interval"`
	HADiscovery  HADiscoveryConfig `yaml:"ha_discovery"`
}

// ReadRegisterConfig holds configuration for a read register.
type ReadRegisterConfig struct {
	Name     string `yaml:"name"`
	Command  string `yaml:"command"`
	Topic    string `yaml:"topic"`
	Divisor  int    `yaml:"divisor"`
	Type     string `yaml:"type"` // "integer" or "float"
	MinValue int    `yaml:"min_value"`
	MaxValue int    `yaml:"max_value"`
	HA       struct {
		Name        string `yaml:"name"`
		DeviceClass string `yaml:"device_class"`
		Unit        string `yaml:"unit"`
	} `yaml:"ha"`
}

// WriteRegisterConfig holds configuration for a write register.
type WriteRegisterConfig struct {
	Name            string   `yaml:"name"`
	CommandTemplate string   `yaml:"command_template"`
	SubscribeTopic  string   `yaml:"subscribe_topic"`
	Topic           string   `yaml:"topic"`
	MinValue        int      `yaml:"min_value"`
	MaxValue        int      `yaml:"max_value"`
	VerifyRegisters []string `yaml:"verify_registers"`

	// StateFrom names the read register that carries this register's real
	// value. The unit reports a setpoint on a different address than the one
	// it is written to (luefterstufe is written on 5002 and read back on
	// 5003), so reading it back is the only way to publish what the unit
	// actually did rather than what was asked of it.
	//
	// Optional: without it the commanded value is published instead.
	StateFrom string `yaml:"state_from"`
	HA        struct {
		Name string `yaml:"name"`
		// Deprecated: the command topic is always SubscribeTopic, so the two
		// cannot drift apart and leave commands unhandled.
		DeprecatedCommandTopic string `yaml:"command_topic"`
	} `yaml:"ha"`
}

// DerivedRegisterConfig holds configuration for a derived register.
type DerivedRegisterConfig struct {
	Name    string   `yaml:"name"`
	Topic   string   `yaml:"topic"`
	Formula string   `yaml:"formula"`
	Sources []string `yaml:"sources"`
	HA      struct {
		Name        string `yaml:"name"`
		Unit        string `yaml:"unit"`
		DeviceClass string `yaml:"device_class"`
	} `yaml:"ha"`
}

// RegistersConfig holds all register configurations.
type RegistersConfig struct {
	ReadRegisters    []ReadRegisterConfig    `yaml:"read_registers"`
	WriteRegisters   []WriteRegisterConfig   `yaml:"write_registers"`
	DerivedRegisters []DerivedRegisterConfig `yaml:"derived_registers"`
}

// LoadAppConfig loads the application configuration from a YAML file.
func LoadAppConfig(path string) (*AppConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg AppConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	cfg.applyDefaults()
	return &cfg, nil
}

func (c *AppConfig) applyDefaults() {
	if c.Serial.Port == "" {
		c.Serial.Port = "/dev/ttyUSB0"
	}
	if c.Serial.BaudRate == 0 {
		c.Serial.BaudRate = 115200
	}
	if c.Serial.ByteSize == 0 {
		c.Serial.ByteSize = 8
	}
	if c.Serial.Parity == "" {
		c.Serial.Parity = "N"
	}
	if c.Serial.StopBits == 0 {
		c.Serial.StopBits = 1
	}
	// 200ms is two VTIME ticks: enough for the unit to answer without the
	// caller waiting a full second on a silent link.
	if c.Serial.ReadTimeout == 0 {
		c.Serial.ReadTimeout = 200
	}
	if c.Serial.ResponseTimeout == 0 {
		c.Serial.ResponseTimeout = 300
	}
	if c.Serial.Retries == 0 {
		c.Serial.Retries = 3
	}
	if c.Serial.RetryBackoff == 0 {
		c.Serial.RetryBackoff = 100
	}
	if c.Serial.ReopenBackoff == 0 {
		c.Serial.ReopenBackoff = 500
	}
	if c.Serial.ReopenMaxBackoff == 0 {
		c.Serial.ReopenMaxBackoff = 30000
	}
	if c.Serial.HealthCheckInterval == 0 {
		c.Serial.HealthCheckInterval = 1000
	}
	if c.Serial.WritePriorityTimeout == 0 {
		c.Serial.WritePriorityTimeout = 10000
	}
	if c.Serial.WriteSettleDelay == 0 {
		c.Serial.WriteSettleDelay = 50
	}

	if c.DeviceID == "" {
		c.DeviceID = "aerosmart"
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if c.ReadInterval == 0 {
		c.ReadInterval = 60
	}

	if c.MQTT.Port == 0 {
		c.MQTT.Port = 1883
	}
	if c.MQTT.ClientID == "" {
		c.MQTT.ClientID = "aerosmart-gateway"
	}
	if c.MQTT.AvailabilityTopic == "" {
		c.MQTT.AvailabilityTopic = c.DeviceID + "/status"
	}
	if c.MQTT.OpTimeout == 0 {
		c.MQTT.OpTimeout = 5000
	}
	if c.MQTT.PublishRetryCount == 0 {
		c.MQTT.PublishRetryCount = 3
	}
	if c.MQTT.ConnectRetryInitialDelayMs == 0 {
		c.MQTT.ConnectRetryInitialDelayMs = 500
	}
	if c.MQTT.ConnectRetryMaxDelayMs == 0 {
		c.MQTT.ConnectRetryMaxDelayMs = 30000
	}
	if c.MQTT.ConnectRetryJitterPercent == 0 {
		c.MQTT.ConnectRetryJitterPercent = 25
	}

	if c.HADiscovery.Prefix == "" {
		c.HADiscovery.Prefix = "homeassistant"
	}
	if c.HADiscovery.DeviceInfo.Name == "" {
		c.HADiscovery.DeviceInfo.Name = "Aerosmart Gateway"
	}
	if c.HADiscovery.DeviceInfo.Manufacturer == "" {
		c.HADiscovery.DeviceInfo.Manufacturer = "Drexel und Weiss"
	}
	if c.HADiscovery.DeviceInfo.Model == "" {
		c.HADiscovery.DeviceInfo.Model = "aerosmartPI"
	}
	// Allow a couple of missed cycles before an entity is called stale, so a
	// single failed readout does not flap every sensor to unavailable.
	if c.HADiscovery.ExpireAfter == 0 && c.ReadInterval > 0 {
		c.HADiscovery.ExpireAfter = c.ReadInterval * 5 / 2
	}

	// An empty or "-" log file means console only.
	if c.Logging.LogFile == "" || c.Logging.LogFile == "-" {
		c.Logging.FileLogging = false
	}
	if c.Logging.ConsoleLoggingSet != nil {
		c.Logging.ConsoleLogging = *c.Logging.ConsoleLoggingSet
	} else {
		c.Logging.ConsoleLogging = true
	}
}

// Validate reports configuration that cannot work, so the process fails at
// startup instead of misbehaving silently.
func (c *AppConfig) Validate() error {
	var problems []string

	switch strings.ToUpper(c.Serial.Parity) {
	case "N", "O", "E":
	default:
		problems = append(problems, fmt.Sprintf("serial.parity: %q is not one of N, O, E", c.Serial.Parity))
	}
	if c.Serial.ByteSize < 5 || c.Serial.ByteSize > 8 {
		problems = append(problems, fmt.Sprintf("serial.bytesize: %d is not in 5..8", c.Serial.ByteSize))
	}
	if c.Serial.StopBits != 1 && c.Serial.StopBits != 2 {
		problems = append(problems, fmt.Sprintf("serial.stopbits: %d is not 1 or 2", c.Serial.StopBits))
	}
	if c.Serial.Retries < 1 {
		problems = append(problems, fmt.Sprintf("serial.retries: %d must be at least 1", c.Serial.Retries))
	}
	if c.ReadInterval <= 0 {
		problems = append(problems, fmt.Sprintf("read_interval: %d must be positive", c.ReadInterval))
	}
	if c.MQTT.Broker == "" {
		problems = append(problems, "mqtt.broker: must be set")
	}
	if c.MQTT.QOS < 0 || c.MQTT.QOS > 2 {
		problems = append(problems, fmt.Sprintf("mqtt.qos: %d is not 0, 1 or 2", c.MQTT.QOS))
	}
	// Negative durations make the retry and reopen loops spin without waiting.
	for _, d := range []struct {
		key   string
		value int
	}{
		{"serial.read_timeout", c.Serial.ReadTimeout},
		{"serial.response_timeout", c.Serial.ResponseTimeout},
		{"serial.retry_backoff", c.Serial.RetryBackoff},
		{"serial.reopen_backoff", c.Serial.ReopenBackoff},
		{"serial.reopen_max_backoff", c.Serial.ReopenMaxBackoff},
		{"serial.health_check_interval", c.Serial.HealthCheckInterval},
		{"serial.write_priority_timeout", c.Serial.WritePriorityTimeout},
		{"serial.write_settle_delay", c.Serial.WriteSettleDelay},
		{"mqtt.op_timeout", c.MQTT.OpTimeout},
		{"mqtt.connect_retry_initial_delay_ms", c.MQTT.ConnectRetryInitialDelayMs},
		{"mqtt.connect_retry_max_delay_ms", c.MQTT.ConnectRetryMaxDelayMs},
		{"ha_discovery.expire_after", c.HADiscovery.ExpireAfter},
	} {
		if d.value < 0 {
			problems = append(problems, fmt.Sprintf("%s: %d must not be negative", d.key, d.value))
		}
	}
	// The startup open loop doubles the backoff and caps it at the maximum, so
	// a maximum below the initial value collapses the delay to nothing and
	// hammers the device node.
	if c.Serial.ReopenMaxBackoff < c.Serial.ReopenBackoff {
		problems = append(problems, fmt.Sprintf(
			"serial.reopen_max_backoff: %d is below serial.reopen_backoff (%d)",
			c.Serial.ReopenMaxBackoff, c.Serial.ReopenBackoff))
	}
	if c.MQTT.ConnectRetryMaxDelayMs < c.MQTT.ConnectRetryInitialDelayMs {
		problems = append(problems, fmt.Sprintf(
			"mqtt.connect_retry_max_delay_ms: %d is below mqtt.connect_retry_initial_delay_ms (%d)",
			c.MQTT.ConnectRetryMaxDelayMs, c.MQTT.ConnectRetryInitialDelayMs))
	}
	if p := c.MQTT.ConnectRetryJitterPercent; p < 0 || p > 100 {
		problems = append(problems, fmt.Sprintf("mqtt.connect_retry_jitter_percent: %d is not in 0..100", p))
	}

	if !c.Logging.ConsoleLogging && !c.Logging.FileLogging {
		problems = append(problems, "logging: both console_logging and file_logging are disabled, "+
			"so nothing would be logged anywhere")
	}

	if len(problems) > 0 {
		return fmt.Errorf("%w:\n  - %s", ErrInvalidConfig, strings.Join(problems, "\n  - "))
	}
	return nil
}

// EffectiveReadTimeout is the per-read block time the serial layer will
// actually use: the kernel expresses it as termios VTIME in deciseconds, so it
// is rounded up to a multiple of 100ms with that as the floor.
func (c *AppConfig) EffectiveReadTimeout() int {
	readTimeout := c.Serial.ReadTimeout
	if readTimeout < serialReadGranularityMs {
		return serialReadGranularityMs
	}
	if r := readTimeout % serialReadGranularityMs; r != 0 {
		readTimeout += serialReadGranularityMs - r
	}
	return readTimeout
}

// EffectiveResponseTimeout is the response budget the serial layer will
// actually use. A response can never arrive faster than one read, so a smaller
// response_timeout is raised to the effective read timeout.
func (c *AppConfig) EffectiveResponseTimeout() int {
	if readTimeout := c.EffectiveReadTimeout(); c.Serial.ResponseTimeout < readTimeout {
		return readTimeout
	}
	return c.Serial.ResponseTimeout
}

// serialReadGranularityMs mirrors the kernel's termios VTIME resolution.
const serialReadGranularityMs = 100

// ValidateCycleBudget reports a retry budget that cannot finish a full readout
// within one read interval.
//
// The check needs the register count, which lives in the registers file, so it
// is separate from Validate. A cycle that overruns its interval never idles,
// leaving the device no gap between readouts.
func (c *AppConfig) ValidateCycleBudget(registerCount int) error {
	if registerCount <= 0 {
		return nil
	}

	// An attempt can pass its response deadline only after a read that already
	// started, and that read blocks for a whole read_timeout, so one attempt
	// costs up to response_timeout + read_timeout.
	perAttemptMs := c.EffectiveResponseTimeout() + c.EffectiveReadTimeout() + c.Serial.RetryBackoff
	perRegisterMs := c.Serial.Retries * perAttemptMs
	worstCycleMs := perRegisterMs * registerCount
	if worstCycleMs <= c.ReadInterval*1000 {
		return nil
	}

	return fmt.Errorf("%w: worst-case read cycle is %d registers x %d attempts x %dms = %.1fs, "+
		"which exceeds read_interval (%ds); lower serial.retries or serial.response_timeout, "+
		"or raise read_interval",
		ErrInvalidConfig, registerCount, c.Serial.Retries, perAttemptMs,
		float64(worstCycleMs)/1000, c.ReadInterval)
}

// Warnings returns configuration that works but should be fixed: settings the
// driver cannot honour, and options that no longer have any effect.
//
// These are deliberately not errors. Every one of them was already inert in
// earlier releases, so refusing to start would break an upgrade without making
// anything safer.
func (c *AppConfig) Warnings() []string {
	var notes []string

	// The driver has never implemented flow control, so a link configured this
	// way has always run without it.
	if c.Serial.XonXoff {
		notes = append(notes, "serial.xonxoff has no effect: the serial driver does not implement "+
			"IXON/IXOFF, and never has. Set it to false to silence this.")
	}
	if c.Serial.DsrDtr {
		notes = append(notes, "serial.dsrdtr has no effect: the serial driver does not implement "+
			"hardware flow control. Set it to false to silence this.")
	}

	add := func(set bool, key, replacement string) {
		if set {
			notes = append(notes, fmt.Sprintf("serial.%s is no longer used (%s)", key, replacement))
		}
	}

	s := &c.Serial
	add(s.DeprecatedTimeout != nil, "timeout", "never had an effect")
	add(s.DeprecatedWriteTimeout != nil, "write_timeout", "the driver has no write timeout")
	add(s.DeprecatedMaxReopens != nil, "max_reopens", "reopens are now supervised with backoff and not capped")
	add(s.DeprecatedDeadlineTimeout != nil, "deadline_timeout", "input is discarded with a flush ioctl instead of timed reads")
	add(s.DeprecatedWriteWithRetryDelay != nil, "write_with_retry_delay", "replaced by serial.retry_backoff")
	add(s.DeprecatedDeviceResponseDelay != nil, "device_response_delay", "replaced by serial.response_timeout")
	add(s.DeprecatedReadWithRetryDelay != nil, "read_with_retry_delay", "replaced by serial.retry_backoff")
	add(s.DeprecatedReadMaxRetries != nil, "read_max_retries", "replaced by serial.retries")
	add(s.DeprecatedWriteMaxRetries != nil, "write_max_retries", "replaced by serial.retries")
	add(s.DeprecatedMaxRetries != nil, "max_retries", "replaced by serial.retries")
	add(s.DeprecatedConnectInitialDelay != nil, "connect_retry_initial_delay_ms", "the serial supervisor uses reopen_backoff")
	add(s.DeprecatedConnectMaxDelay != nil, "connect_retry_max_delay_ms", "the serial supervisor uses reopen_max_backoff")
	add(s.DeprecatedConnectJitter != nil, "connect_retry_jitter_percent", "no longer applied to serial reconnects")

	return notes
}

// LoadRegistersConfig loads the registers configuration from a YAML file.
func LoadRegistersConfig(path string) (*RegistersConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read registers file: %w", err)
	}

	var cfg RegistersConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse registers file: %w", err)
	}

	return &cfg, nil
}

// Validate reports register definitions that cannot work.
func (c *RegistersConfig) Validate() error {
	var problems []string

	readNames := make(map[string]bool, len(c.ReadRegisters))
	for _, reg := range c.ReadRegisters {
		switch {
		case reg.Name == "":
			problems = append(problems, "read_registers: an entry has no name")
			continue
		case readNames[reg.Name]:
			problems = append(problems, fmt.Sprintf("read_registers: duplicate name %q", reg.Name))
			continue
		}
		readNames[reg.Name] = true

		if reg.Command == "" {
			problems = append(problems, fmt.Sprintf("read_registers[%s]: command is required", reg.Name))
		}
		if reg.Topic == "" {
			problems = append(problems, fmt.Sprintf("read_registers[%s]: topic is required, or the value can never be published", reg.Name))
		}
		if reg.MinValue > reg.MaxValue {
			problems = append(problems, fmt.Sprintf("read_registers[%s]: min_value %d exceeds max_value %d", reg.Name, reg.MinValue, reg.MaxValue))
		}
		if reg.MinValue == 0 && reg.MaxValue == 0 {
			problems = append(problems, fmt.Sprintf("read_registers[%s]: min_value and max_value are both 0, so every reading is rejected", reg.Name))
		}
	}

	writeNames := make(map[string]bool, len(c.WriteRegisters))
	subscribeTopics := make(map[string]string, len(c.WriteRegisters))
	for _, reg := range c.WriteRegisters {
		if reg.Name == "" {
			problems = append(problems, "write_registers: an entry has no name")
			continue
		}
		if writeNames[reg.Name] {
			problems = append(problems, fmt.Sprintf("write_registers: duplicate name %q", reg.Name))
			continue
		}
		writeNames[reg.Name] = true

		if !strings.Contains(reg.CommandTemplate, "{value}") {
			problems = append(problems, fmt.Sprintf("write_registers[%s]: command_template %q has no {value} placeholder", reg.Name, reg.CommandTemplate))
		}
		if reg.SubscribeTopic == "" {
			problems = append(problems, fmt.Sprintf("write_registers[%s]: subscribe_topic is required", reg.Name))
		} else if other, clash := subscribeTopics[reg.SubscribeTopic]; clash {
			problems = append(problems, fmt.Sprintf("write_registers[%s]: subscribe_topic %q is already used by %q", reg.Name, reg.SubscribeTopic, other))
		} else {
			subscribeTopics[reg.SubscribeTopic] = reg.Name
		}
		if reg.MinValue > reg.MaxValue {
			problems = append(problems, fmt.Sprintf("write_registers[%s]: min_value %d exceeds max_value %d", reg.Name, reg.MinValue, reg.MaxValue))
		}
		for _, verify := range reg.VerifyRegisters {
			if !readNames[verify] {
				problems = append(problems, fmt.Sprintf("write_registers[%s]: verify_registers references unknown read register %q", reg.Name, verify))
			}
		}
		if reg.StateFrom != "" && !readNames[reg.StateFrom] {
			problems = append(problems, fmt.Sprintf("write_registers[%s]: state_from references unknown read register %q", reg.Name, reg.StateFrom))
		}
		if reg.HA.DeprecatedCommandTopic != "" && reg.HA.DeprecatedCommandTopic != reg.SubscribeTopic {
			problems = append(problems, fmt.Sprintf(
				"write_registers[%s]: ha.command_topic %q differs from subscribe_topic %q; "+
					"ha.command_topic is no longer used and commands would be lost",
				reg.Name, reg.HA.DeprecatedCommandTopic, reg.SubscribeTopic))
		}
	}

	derivedNames := make(map[string]bool, len(c.DerivedRegisters))
	for _, reg := range c.DerivedRegisters {
		if derivedNames[reg.Name] {
			problems = append(problems, fmt.Sprintf("derived_registers: duplicate name %q", reg.Name))
			continue
		}
		derivedNames[reg.Name] = true

		if reg.Topic == "" {
			problems = append(problems, fmt.Sprintf("derived_registers[%s]: topic is required", reg.Name))
		}
		for _, source := range reg.Sources {
			if !readNames[source] {
				problems = append(problems, fmt.Sprintf("derived_registers[%s]: source %q is not a read register", reg.Name, source))
			}
		}
	}

	if len(problems) > 0 {
		return fmt.Errorf("%w:\n  - %s", ErrInvalidConfig, strings.Join(problems, "\n  - "))
	}
	return nil
}

// Warnings returns register definitions that work but should be fixed.
func (c *RegistersConfig) Warnings() []string {
	var notes []string

	// A write register publishing state to a read register's topic makes two
	// Home Assistant entities publish to one topic; both will look wrong.
	readTopics := make(map[string]string, len(c.ReadRegisters))
	for _, reg := range c.ReadRegisters {
		readTopics[reg.Topic] = reg.Name
	}
	for _, reg := range c.WriteRegisters {
		if owner, clash := readTopics[reg.Topic]; clash {
			notes = append(notes, fmt.Sprintf(
				"write_registers[%s]: topic %q is also the state topic of read register %q; "+
					"give the write register its own state topic so the two Home Assistant "+
					"entities do not collide",
				reg.Name, reg.Topic, owner))
		}
	}

	return notes
}

// GetConfigPaths returns the default config file paths.
func GetConfigPaths() (appConfigPath, registersConfigPath string) {
	cwd, _ := os.Getwd()

	appConfigPath = filepath.Join(cwd, "config", "config.yaml")
	if _, err := os.Stat(appConfigPath); os.IsNotExist(err) {
		appConfigPath = filepath.Join(cwd, "config.yaml")
	}
	registersConfigPath = filepath.Join(cwd, "registers.yaml")

	return appConfigPath, registersConfigPath
}
