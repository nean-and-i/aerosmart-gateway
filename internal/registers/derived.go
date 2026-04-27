package registers

import (
	"fmt"
	"math"
	"strconv"

	"github.com/nean/aerosmart-gateway/internal/config"
	"github.com/nean/aerosmart-gateway/internal/logger"
	"github.com/nean/aerosmart-gateway/internal/mqtt"
)

// DerivedCalculator handles derived register calculations
type DerivedCalculator struct {
	mqtt      *mqtt.Client
	logger    *logger.Logger
	registers []config.DerivedRegisterConfig
}

// NewDerivedCalculator creates a new derived register calculator
func NewDerivedCalculator(mqttClient *mqtt.Client, log *logger.Logger, registers []config.DerivedRegisterConfig) *DerivedCalculator {
	return &DerivedCalculator{
		mqtt:      mqttClient,
		logger:    log,
		registers: registers,
	}
}

// Calculate calculates derived register values based on source values
func (d *DerivedCalculator) Calculate(sourceValues map[string]*RegisterValue) (map[string]*RegisterValue, error) {
	results := make(map[string]*RegisterValue)

	for _, derived := range d.registers {
		value, err := d.calculateSingle(derived, sourceValues)
		if err != nil {
			d.logger.Warn("Failed to calculate derived register %s: %v", derived.Name, err)
			results[derived.Name] = &RegisterValue{
				Name:  derived.Name,
				Value: "",
				Valid: false,
			}
			continue
		}

		results[derived.Name] = &RegisterValue{
			Name:  derived.Name,
			Value: value,
			Valid: true,
		}

		d.logger.Debug("Derived register %s = %s", derived.Name, value)
	}

	return results, nil
}

// calculateSingle calculates a single derived register
func (d *DerivedCalculator) calculateSingle(derived config.DerivedRegisterConfig, sourceValues map[string]*RegisterValue) (string, error) {
	// Get source values
	sources := make(map[string]float64)
	for _, sourceName := range derived.Sources {
		if regVal, ok := sourceValues[sourceName]; ok && regVal.Valid {
			val, err := strconv.ParseFloat(regVal.Value, 64)
			if err != nil {
				return "", fmt.Errorf("invalid source value for %s: %v", sourceName, err)
			}
			sources[sourceName] = val
		} else {
			return "", fmt.Errorf("source register %s not available", sourceName)
		}
	}

	// Calculate based on formula
	var result float64
	var err error

	switch derived.Name {
	case "zuluftabluftprozent":
		// formula: round((zuluftumin / abluftumin) * 100, 1)
		zuluftumin := sources["zuluftumin"]
		abluftumin := sources["abluftumin"]
		if abluftumin == 0 {
			result = 100
		} else {
			result = math.Round((zuluftumin/abluftumin)*100*10) / 10
		}

	case "co2luefterstufe4":
		// formula: round(co2luefterstufe3 * 1.2)
		co2luefterstufe3 := sources["co2luefterstufe3"]
		result = math.Round(co2luefterstufe3 * 1.2)

	case "beschattungtemp_adjusted":
		// formula: beschattungtemp - 0.5 if beschattungaussentemp < aussentemp else beschattungtemp + 1
		beschattungtemp := sources["beschattungtemp"]
		beschattungaussentemp := sources["beschattungaussentemp"]
		aussentemp := sources["aussentemp"]

		if beschattungaussentemp < aussentemp {
			result = math.Round((beschattungtemp-0.5)*10) / 10
		} else {
			result = math.Round((beschattungtemp+1)*10) / 10
		}

	default:
		// For any other formula, try to evaluate generically
		result, err = d.evaluateFormula(derived.Formula, sources)
		if err != nil {
			return "", err
		}
	}

	// Format result
	return fmt.Sprintf("%.1f", result), nil
}

// evaluateFormula evaluates a generic formula (simplified implementation)
func (d *DerivedCalculator) evaluateFormula(formula string, sources map[string]float64) (float64, error) {
	// This is a simplified formula evaluator
	// For more complex formulas, consider using a proper expression parser

	// Replace source names with values in formula
	expr := formula
	for name, value := range sources {
		expr = replaceVar(expr, name, value)
	}

	// For now, return error for unknown formulas
	return 0, fmt.Errorf("unsupported formula: %s", formula)
}

// replaceVar replaces a variable name with its value in the formula
func replaceVar(expr string, name string, value float64) string {
	// Simple string replacement
	return expr
}

// PublishAll publishes all derived register values to MQTT
func (d *DerivedCalculator) PublishAll(values map[string]*RegisterValue) error {
	for name, regVal := range values {
		if !regVal.Valid {
			continue
		}

		// Find the topic for this register
		var topic string
		for _, reg := range d.registers {
			if reg.Name == name {
				topic = reg.Topic
				break
			}
		}

		if topic != "" {
			if err := d.mqtt.Publish(topic, regVal.Value); err != nil {
				d.logger.Error("Failed to publish derived %s to MQTT: %v", topic, err)
			} else {
				d.logger.Debug("Published derived %s = %s to %s", name, regVal.Value, topic)
			}
		}
	}

	return nil
}

// GetDerivedHAConfig returns HA discovery configs for derived registers
func (d *DerivedCalculator) GetDerivedHAConfig(deviceID, name, manufacturer, model, swVersion string) []mqtt.HASensorConfig {
	configs := make([]mqtt.HASensorConfig, 0, len(d.registers))

	deviceInfo := mqtt.CreateDeviceInfo(deviceID, name, manufacturer, model, swVersion)

	for _, reg := range d.registers {
		config := mqtt.HASensorConfig{
			Name:        reg.HA.Name,
			StateTopic:  reg.Topic,
			Unit:        reg.HA.Unit,
			DeviceClass: reg.HA.DeviceClass,
			UniqueID:    fmt.Sprintf("%s_%s", deviceID, reg.Name),
			Device:      deviceInfo,
		}
		configs = append(configs, config)
	}

	return configs
}
