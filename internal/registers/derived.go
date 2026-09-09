package registers

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/nean/aerosmart-gateway/internal/config"
	"github.com/nean/aerosmart-gateway/internal/logger"
	"github.com/nean/aerosmart-gateway/internal/mqtt"
)

// DerivedCalculator handles derived register calculations
type DerivedCalculator struct {
	pub       Publisher
	logger    *logger.Logger
	registers []config.DerivedRegisterConfig
	topics    map[string]string
}

// NewDerivedCalculator creates a new derived register calculator
func NewDerivedCalculator(pub Publisher, log *logger.Logger, registers []config.DerivedRegisterConfig) *DerivedCalculator {
	topics := make(map[string]string, len(registers))
	for _, reg := range registers {
		topics[reg.Name] = reg.Topic
	}
	return &DerivedCalculator{
		pub:       pub,
		logger:    log,
		registers: registers,
		topics:    topics,
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

	fn, ok := derivedFormulas[derived.Name]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrNoFormula, derived.Name)
	}
	result := fn(sources)

	// Format result
	return fmt.Sprintf("%.1f", result), nil
}

// ErrNoFormula means a configured derived register has no implementation.
var ErrNoFormula = errors.New("derived register has no formula implementation")

// derivedFormulas holds the arithmetic for each derived register.
//
// The formula string in registers.yaml documents the intent; the code here is
// what runs. Adding a derived register to the config therefore requires adding
// it here too, which Validate enforces at startup.
var derivedFormulas = map[string]func(src map[string]float64) float64{
	// round((zuluftumin / abluftumin) * 100, 1)
	"zuluftabluftprozent": func(src map[string]float64) float64 {
		abluft := src["abluftumin"]
		if abluft == 0 {
			// Both fans idle: report balanced rather than dividing by zero.
			return 100
		}
		return math.Round((src["zuluftumin"]/abluft)*100*10) / 10
	},

	// round(co2luefterstufe3 * 1.2)
	"co2luefterstufe4": func(src map[string]float64) float64 {
		return math.Round(src["co2luefterstufe3"] * 1.2)
	},

	// beschattungtemp - 0.5 if beschattungaussentemp < aussentemp else beschattungtemp + 1
	"beschattungtemp_adjusted": func(src map[string]float64) float64 {
		if src["beschattungaussentemp"] < src["aussentemp"] {
			return math.Round((src["beschattungtemp"]-0.5)*10) / 10
		}
		return math.Round((src["beschattungtemp"]+1)*10) / 10
	},
}

// Validate reports configured derived registers that have no implementation,
// so the mistake surfaces at startup instead of as a warning every cycle.
func (d *DerivedCalculator) Validate() error {
	var missing []string
	for _, reg := range d.registers {
		if _, ok := derivedFormulas[reg.Name]; !ok {
			missing = append(missing, reg.Name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: %s", ErrNoFormula, strings.Join(missing, ", "))
	}
	return nil
}

// PublishAll publishes valid derived register values.
func (d *DerivedCalculator) PublishAll(values map[string]*RegisterValue) error {
	var firstErr error
	for name, regVal := range values {
		if regVal == nil || !regVal.Valid {
			continue
		}
		topic, ok := d.topics[name]
		if !ok || topic == "" {
			continue
		}
		if err := d.pub.Publish(topic, regVal.Value); err != nil {
			d.logger.Error("Failed to publish derived %s to %s: %v", name, topic, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		d.logger.Debug("Published derived %s = %s to %s", name, regVal.Value, topic)
	}
	return firstErr
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
