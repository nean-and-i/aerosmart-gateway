package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// SerialConfig holds serial port configuration
type SerialConfig struct {
	Port                       string `yaml:"port"`
	BaudRate                   int    `yaml:"baudrate"`
	ByteSize                   int    `yaml:"bytesize"`                       // 8
	Parity                     string `yaml:"parity"`                         // N
	StopBits                   int    `yaml:"stopbits"`                       // 1
	Timeout                    int    `yaml:"timeout"`                        // 0 (non-blocking)
	XonXoff                    bool   `yaml:"xonxoff"`                        // true
	DsrDtr                     bool   `yaml:"dsrdtr"`                         // false
	WriteTimeout               int    `yaml:"write_timeout"`                  // 10
	ReadTimeout                int    `yaml:"read_timeout"`                   // 1 (milliseconds, 0 = 200ms default)
	MaxReopens                 int    `yaml:"max_reopens"`                    // 10
	DeadlineTimeout            int    `yaml:"deadline_timeout"`               // 150 (milliseconds)
	WriteWithRetryDelay        int    `yaml:"write_with_retry_delay"`         // 10 (milliseconds)
	DeviceResponseDelay        int    `yaml:"device_response_delay"`          // 40 (milliseconds)
	ReadWithRetryDelay         int    `yaml:"read_with_retry_delay"`          // 100 (milliseconds)
	ReadMaxRetries             int    `yaml:"read_max_retries"`               // 10
	WriteMaxRetries            int    `yaml:"write_max_retries"`              // 10
	MaxRetries                 int    `yaml:"max_retries"`                    // 10 (max retries for register read/write operations)
	ConnectRetryInitialDelayMs int    `yaml:"connect_retry_initial_delay_ms"` // 2 (ms)
	ConnectRetryMaxDelayMs     int    `yaml:"connect_retry_max_delay_ms"`     // 400 (ms)
	ConnectRetryJitterPercent  int    `yaml:"connect_retry_jitter_percent"`   // 25 (%)
}

// MQTTConfig holds MQTT broker configuration
type MQTTConfig struct {
	Broker                     string `yaml:"broker"`
	Port                       int    `yaml:"port"`
	Username                   string `yaml:"username"`
	Password                   string `yaml:"password"`
	ClientID                   string `yaml:"client_id"`
	QOS                        int    `yaml:"qos"`
	Retain                     bool   `yaml:"retain"`
	ConnectRetryInitialDelayMs int    `yaml:"connect_retry_initial_delay_ms"` // 500 (ms)
	ConnectRetryMaxDelayMs     int    `yaml:"connect_retry_max_delay_ms"`     // 30000 (ms)
	ConnectRetryJitterPercent  int    `yaml:"connect_retry_jitter_percent"`   // 25 (%)
}

// HADiscoveryConfig holds Home Assistant discovery configuration
type HADiscoveryConfig struct {
	Enabled bool   `yaml:"enabled"`
	Prefix  string `yaml:"prefix"` // "homeassistant"
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	LogFile        string `yaml:"log_file"`        // path to log file (empty to disable file logging)
	FileLogging    bool   `yaml:"file_logging"`    // enable/disable file logging
	ConsoleLogging bool   `yaml:"console_logging"` // enable/disable console logging
}

// AppConfig holds the main application configuration
type AppConfig struct {
	Serial       SerialConfig      `yaml:"serial"`
	MQTT         MQTTConfig        `yaml:"mqtt"`
	DeviceID     string            `yaml:"device_id"`
	LogLevel     string            `yaml:"log_level"`
	Logging      LoggingConfig     `yaml:"logging"`
	ReadInterval int               `yaml:"read_interval"`
	HADiscovery  HADiscoveryConfig `yaml:"ha_discovery"`
}

// ReadRegisterConfig holds configuration for a read register
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

// WriteRegisterConfig holds configuration for a write register
type WriteRegisterConfig struct {
	Name            string   `yaml:"name"`
	CommandTemplate string   `yaml:"command_template"`
	SubscribeTopic  string   `yaml:"subscribe_topic"`
	Topic           string   `yaml:"topic"`
	MinValue        int      `yaml:"min_value"`
	MaxValue        int      `yaml:"max_value"`
	VerifyRegisters []string `yaml:"verify_registers"`
	HA              struct {
		Name         string `yaml:"name"`
		CommandTopic string `yaml:"command_topic"`
	} `yaml:"ha"`
}

// DerivedRegisterConfig holds configuration for a derived register
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

// RegistersConfig holds all register configurations
type RegistersConfig struct {
	ReadRegisters    []ReadRegisterConfig    `yaml:"read_registers"`
	WriteRegisters   []WriteRegisterConfig   `yaml:"write_registers"`
	DerivedRegisters []DerivedRegisterConfig `yaml:"derived_registers"`
}

// LoadAppConfig loads the application configuration from a YAML file
func LoadAppConfig(path string) (*AppConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg AppConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set defaults
	if cfg.Serial.Port == "" {
		cfg.Serial.Port = "/dev/ttyUSB0"
	}
	if cfg.Serial.BaudRate == 0 {
		cfg.Serial.BaudRate = 115200
	}
	if cfg.Serial.ByteSize == 0 {
		cfg.Serial.ByteSize = 8
	}
	if cfg.Serial.Parity == "" {
		cfg.Serial.Parity = "N"
	}
	if cfg.Serial.StopBits == 0 {
		cfg.Serial.StopBits = 1
	}
	if cfg.Serial.Timeout == 0 {
		cfg.Serial.Timeout = 0
	}
	if cfg.Serial.WriteTimeout == 0 {
		cfg.Serial.WriteTimeout = 10
	}
	if cfg.Serial.ReadTimeout == 0 {
		cfg.Serial.ReadTimeout = 2
	}
	if cfg.Serial.MaxReopens == 0 {
		cfg.Serial.MaxReopens = 10
	}
	if cfg.Serial.DeadlineTimeout == 0 {
		cfg.Serial.DeadlineTimeout = 150
	}
	if cfg.Serial.WriteWithRetryDelay == 0 {
		cfg.Serial.WriteWithRetryDelay = 10
	}
	if cfg.Serial.DeviceResponseDelay == 0 {
		cfg.Serial.DeviceResponseDelay = 40
	}
	if cfg.Serial.ReadWithRetryDelay == 0 {
		cfg.Serial.ReadWithRetryDelay = 100
	}
	if cfg.Serial.ReadMaxRetries == 0 {
		cfg.Serial.ReadMaxRetries = 10
	}
	if cfg.Serial.WriteMaxRetries == 0 {
		cfg.Serial.WriteMaxRetries = 10
	}
	if cfg.Serial.MaxRetries == 0 {
		cfg.Serial.MaxRetries = 10
	}
	if cfg.Serial.ConnectRetryInitialDelayMs == 0 {
		cfg.Serial.ConnectRetryInitialDelayMs = 100
	}
	if cfg.Serial.ConnectRetryMaxDelayMs == 0 {
		cfg.Serial.ConnectRetryMaxDelayMs = 10000
	}
	if cfg.Serial.ConnectRetryJitterPercent == 0 {
		cfg.Serial.ConnectRetryJitterPercent = 25
	}
	// XonXoff defaults to false (no flow control) - no explicit default needed
	if cfg.DeviceID == "" {
		cfg.DeviceID = "aerosmart-gateway"
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.ReadInterval == 0 {
		cfg.ReadInterval = 60
	}
	if cfg.HADiscovery.Prefix == "" {
		cfg.HADiscovery.Prefix = "homeassistant"
	}

	// Set logging defaults
	// Only apply defaults if the user hasn't specified a log file path
	// If log_file is explicitly set to empty or "-", disable file logging
	if cfg.Logging.LogFile == "" {
		cfg.Logging.LogFile = "/var/log/aerosmart.log"
	}
	// If log_file is set to "-" or empty string, disable file logging
	if cfg.Logging.LogFile == "-" || cfg.Logging.LogFile == "" {
		cfg.Logging.FileLogging = false
	}
	// If file_logging is explicitly set to false, respect that choice
	// (don't override user's explicit setting)

	// Set MQTT defaults
	if cfg.MQTT.ConnectRetryInitialDelayMs == 0 {
		cfg.MQTT.ConnectRetryInitialDelayMs = 500
	}
	if cfg.MQTT.ConnectRetryMaxDelayMs == 0 {
		cfg.MQTT.ConnectRetryMaxDelayMs = 30000
	}
	if cfg.MQTT.ConnectRetryJitterPercent == 0 {
		cfg.MQTT.ConnectRetryJitterPercent = 25
	}

	return &cfg, nil
}

// LoadRegistersConfig loads the registers configuration from a YAML file
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

// GetConfigPaths returns the default config file paths
func GetConfigPaths() (appConfigPath, registersConfigPath string) {
	// Check for config directory in current working directory
	cwd, _ := os.Getwd()
	configDir := filepath.Join(cwd, "config")

	// Check if config files exist in config directory
	appConfigPath = filepath.Join(configDir, "config.yaml")
	registersConfigPath = filepath.Join(cwd, "registers.yaml")

	// If not found, try current directory
	if _, err := os.Stat(appConfigPath); os.IsNotExist(err) {
		appConfigPath = filepath.Join(cwd, "config.yaml")
	}
	if _, err := os.Stat(registersConfigPath); os.IsNotExist(err) {
		registersConfigPath = filepath.Join(cwd, "registers.yaml")
	}

	return appConfigPath, registersConfigPath
}
