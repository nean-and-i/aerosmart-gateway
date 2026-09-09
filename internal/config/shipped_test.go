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
	// The shipped retry budget must fit inside the shipped read interval.
	if err := ac.ValidateCycleBudget(len(rc.ReadRegisters)); err != nil {
		t.Errorf("shipped retry budget does not fit the read interval:\n%v", err)
	}
	t.Logf("%d read registers, worst-case cycle fits within the %ds interval",
		len(rc.ReadRegisters), ac.ReadInterval)
	for _, note := range ac.DeprecatedKeys() {
		t.Logf("deprecated: %s", note)
	}
}
