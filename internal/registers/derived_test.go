package registers

import (
	"testing"

	"github.com/nean/aerosmart-gateway/internal/config"
	"github.com/nean/aerosmart-gateway/internal/logger"
	"github.com/nean/aerosmart-gateway/internal/mqtt"
)

func TestDerivedCalculator_Calculate(t *testing.T) {
	// Create a mock logger
	log := logger.New("error")

	// Create derived register configs
	derivedRegisters := []config.DerivedRegisterConfig{
		{
			Name:    "zuluftabluftprozent",
			Topic:   "aerosmart/zuluftabluftprozent",
			Formula: "round((zuluftumin / abluftumin) * 100, 1)",
			Sources: []string{"zuluftumin", "abluftumin"},
		},
		{
			Name:    "co2luefterstufe4",
			Topic:   "aerosmart/co2luefterstufe4",
			Formula: "round(co2luefterstufe3 * 1.2)",
			Sources: []string{"co2luefterstufe3"},
		},
		{
			Name:    "beschattungtemp_adjusted",
			Topic:   "aerosmart/beschattungtemp_adjusted",
			Formula: "beschattungtemp - 0.5 if beschattungaussentemp < aussentemp else beschattungtemp + 1",
			Sources: []string{"beschattungtemp", "beschattungaussentemp", "aussentemp"},
		},
	}

	// Create a mock MQTT client (nil is fine for testing calculations)
	mqttClient := mqtt.NewClient(nil, "test")

	// Create the derived calculator
	calc := NewDerivedCalculator(mqttClient, log, derivedRegisters)

	// Test case 1: zuluftabluftprozent
	sourceValues := map[string]*RegisterValue{
		"zuluftumin": {Name: "zuluftumin", Value: "2500", Valid: true},
		"abluftumin": {Name: "abluftumin", Value: "2000", Valid: true},
	}

	results, err := calc.Calculate(sourceValues)
	if err != nil {
		t.Fatalf("Calculate failed: %v", err)
	}

	// Check zuluftabluftprozent
	if val, ok := results["zuluftabluftprozent"]; !ok {
		t.Error("Expected zuluftabluftprozent in results")
	} else if val.Value != "125.0" {
		t.Errorf("Expected zuluftabluftprozent = 125.0, got %s", val.Value)
	}

	// Test case 2: co2luefterstufe4
	sourceValues = map[string]*RegisterValue{
		"co2luefterstufe3": {Name: "co2luefterstufe3", Value: "1000", Valid: true},
	}

	results, err = calc.Calculate(sourceValues)
	if err != nil {
		t.Fatalf("Calculate failed: %v", err)
	}

	if val, ok := results["co2luefterstufe4"]; !ok {
		t.Error("Expected co2luefterstufe4 in results")
	} else if val.Value != "1200.0" {
		t.Errorf("Expected co2luefterstufe4 = 1200.0, got %s", val.Value)
	}

	// Test case 3: beschattungtemp_adjusted (aussentemp > beschattungaussentemp)
	sourceValues = map[string]*RegisterValue{
		"beschattungtemp":       {Name: "beschattungtemp", Value: "25.0", Valid: true},
		"beschattungaussentemp": {Name: "beschattungaussentemp", Value: "10.0", Valid: true},
		"aussentemp":            {Name: "aussentemp", Value: "15.0", Valid: true},
	}

	results, err = calc.Calculate(sourceValues)
	if err != nil {
		t.Fatalf("Calculate failed: %v", err)
	}

	if val, ok := results["beschattungtemp_adjusted"]; !ok {
		t.Error("Expected beschattungtemp_adjusted in results")
	} else if val.Value != "24.5" {
		t.Errorf("Expected beschattungtemp_adjusted = 24.5, got %s", val.Value)
	}

	// Test case 4: beschattungtemp_adjusted (aussentemp <= beschattungaussentemp)
	sourceValues = map[string]*RegisterValue{
		"beschattungtemp":       {Name: "beschattungtemp", Value: "25.0", Valid: true},
		"beschattungaussentemp": {Name: "beschattungaussentemp", Value: "20.0", Valid: true},
		"aussentemp":            {Name: "aussentemp", Value: "15.0", Valid: true},
	}

	results, err = calc.Calculate(sourceValues)
	if err != nil {
		t.Fatalf("Calculate failed: %v", err)
	}

	if val, ok := results["beschattungtemp_adjusted"]; !ok {
		t.Error("Expected beschattungtemp_adjusted in results")
	} else if val.Value != "26.0" {
		t.Errorf("Expected beschattungtemp_adjusted = 26.0, got %s", val.Value)
	}
}

func TestDerivedCalculator_MissingSource(t *testing.T) {
	log := logger.New("error")

	derivedRegisters := []config.DerivedRegisterConfig{
		{
			Name:    "test",
			Topic:   "aerosmart/test",
			Formula: "test",
			Sources: []string{"missing_source"},
		},
	}

	mqttClient := mqtt.NewClient(nil, "test")
	calc := NewDerivedCalculator(mqttClient, log, derivedRegisters)

	sourceValues := map[string]*RegisterValue{}

	results, err := calc.Calculate(sourceValues)
	if err != nil {
		t.Fatalf("Calculate should not fail: %v", err)
	}

	// Should have invalid result for missing source
	if val, ok := results["test"]; !ok {
		t.Error("Expected test in results")
	} else if val.Valid {
		t.Error("Expected test to be invalid due to missing source")
	}
}

func TestDerivedCalculator_DivisionByZero(t *testing.T) {
	log := logger.New("error")

	derivedRegisters := []config.DerivedRegisterConfig{
		{
			Name:    "zuluftabluftprozent",
			Topic:   "aerosmart/zuluftabluftprozent",
			Formula: "round((zuluftumin / abluftumin) * 100, 1)",
			Sources: []string{"zuluftumin", "abluftumin"},
		},
	}

	mqttClient := mqtt.NewClient(nil, "test")
	calc := NewDerivedCalculator(mqttClient, log, derivedRegisters)

	// Test with abluftumin = 0
	sourceValues := map[string]*RegisterValue{
		"zuluftumin": {Name: "zuluftumin", Value: "2500", Valid: true},
		"abluftumin": {Name: "abluftumin", Value: "0", Valid: true},
	}

	results, err := calc.Calculate(sourceValues)
	if err != nil {
		t.Fatalf("Calculate should handle division by zero: %v", err)
	}

	// Should return 100 as fallback
	if val, ok := results["zuluftabluftprozent"]; !ok {
		t.Error("Expected zuluftabluftprozent in results")
	} else if val.Value != "100.0" {
		t.Errorf("Expected zuluftabluftprozent = 100.0, got %s", val.Value)
	}
}
