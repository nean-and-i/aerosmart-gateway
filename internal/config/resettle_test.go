package config

import (
	"os"
	"path/filepath"
	"testing"
)

// loadFromYAML writes body to a temp file and loads it, as the binary would.
func loadFromYAML(t *testing.T, body string) *AppConfig {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	cfg, err := LoadAppConfig(path)
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}
	return cfg
}

// The setting is documented as disabling the second read-back, and there is no
// other way to opt out: applyDefaults rewriting an explicit 0 makes it
// unreachable.
func TestWriteResettleDelayZeroDisablesTheSecondRead(t *testing.T) {
	cfg := loadFromYAML(t, "mqtt:\n  broker: \"test\"\nserial:\n  write_resettle_delay: 0\n")

	if cfg.Serial.WriteResettleDelay != 0 {
		t.Errorf("write_resettle_delay = %d, want 0; an explicit 0 must disable the second read",
			cfg.Serial.WriteResettleDelay)
	}
}

// An omitted key must still get the default, or every existing configuration
// silently loses the second read-back.
func TestWriteResettleDelayDefaultsWhenOmitted(t *testing.T) {
	cfg := loadFromYAML(t, "mqtt:\n  broker: \"test\"\n")

	if cfg.Serial.WriteResettleDelay != 5000 {
		t.Errorf("write_resettle_delay = %d, want the 5000 default when omitted",
			cfg.Serial.WriteResettleDelay)
	}
}

// expire_after is emitted with omitempty, so 0 is how an entity is told never
// to expire. Rewriting it to 2.5x read_interval makes that unreachable.
func TestExpireAfterZeroDisablesExpiry(t *testing.T) {
	cfg := loadFromYAML(t, "mqtt:\n  broker: \"test\"\nread_interval: 120\nha_discovery:\n  expire_after: 0\n")

	if cfg.HADiscovery.ExpireAfter != 0 {
		t.Errorf("expire_after = %d, want 0; an explicit 0 must disable entity expiry",
			cfg.HADiscovery.ExpireAfter)
	}
}

// Omitting it keeps the 2.5x read_interval default.
func TestExpireAfterDefaultsToTwoAndAHalfIntervals(t *testing.T) {
	cfg := loadFromYAML(t, "mqtt:\n  broker: \"test\"\nread_interval: 120\n")

	if cfg.HADiscovery.ExpireAfter != 300 {
		t.Errorf("expire_after = %d, want 300 (2.5 x 120) when omitted", cfg.HADiscovery.ExpireAfter)
	}
}

// A negative value is still rejected, whichever way it was supplied.
func TestNegativeResettleDelayRejected(t *testing.T) {
	cfg := loadFromYAML(t, "mqtt:\n  broker: \"test\"\nserial:\n  write_resettle_delay: -1\n")

	if err := cfg.Validate(); err == nil {
		t.Error("Validate() error = nil, want a negative write_resettle_delay rejected")
	}
}
