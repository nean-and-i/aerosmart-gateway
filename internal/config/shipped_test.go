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
	for _, note := range ac.Warnings() {
		t.Logf("deprecated: %s", note)
	}
}

// TestExampleConfigIsCurrent checks config/config.yaml.example, which the README
// tells users to copy over config.yaml. It went stale once, still carrying keys
// from the pre-rework retry scheme, so a fresh copy started with a screen of
// deprecation warnings. Validating it here keeps it in step with the schema.
func TestExampleConfigIsCurrent(t *testing.T) {
	const path = "../../config/config.yaml.example"
	if _, err := os.Stat(path); err != nil {
		t.Skipf("%s not present: %v", path, err)
	}

	ac, err := LoadAppConfig(path)
	if err != nil {
		t.Fatalf("LoadAppConfig() error = %v", err)
	}
	if err := ac.Validate(); err != nil {
		t.Errorf("config.yaml.example is invalid:\n%v", err)
	}
	// Copying the example must not produce a configuration that immediately
	// complains: every warning here is a key the example should not ship.
	for _, note := range ac.Warnings() {
		t.Errorf("config.yaml.example still carries a warned-about setting: %s", note)
	}

	if rc, err := LoadRegistersConfig("../../registers.yaml"); err == nil {
		if err := ac.ValidateCycleBudget(len(rc.ReadRegisters)); err != nil {
			t.Errorf("example retry budget does not fit its read interval:\n%v", err)
		}
	}
}
