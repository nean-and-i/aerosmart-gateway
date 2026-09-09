package config

import (
	"os"
	"testing"
)

// TestShippedConfigsValidate checks the configuration files in the repository
// root against the validation rules, so the shipped defaults stay usable.
func TestShippedConfigsValidate(t *testing.T) {
	for _, path := range []string{"../../config.yaml", "../../registers.yaml"} {
		if _, err := os.Stat(path); err != nil {
			t.Skipf("%s not present: %v", path, err)
		}
	}

	rc, err := LoadRegistersConfig("../../registers.yaml")
	if err != nil {
		t.Fatalf("LoadRegistersConfig() error = %v", err)
	}
	if err := rc.Validate(); err != nil {
		t.Errorf("registers.yaml is invalid:\n%v", err)
	}

	ac, err := LoadAppConfig("../../config.yaml")
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}
	if err := ac.Validate(); err != nil {
		t.Errorf("config.yaml is invalid:\n%v", err)
	}
	for _, note := range ac.DeprecatedKeys() {
		t.Logf("deprecated: %s", note)
	}
}
