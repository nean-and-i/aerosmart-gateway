package registers

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/nean/aerosmart-gateway/internal/config"
	"github.com/nean/aerosmart-gateway/internal/logger"
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

// derivedInputs lists the register values each formula reads. A formula takes
// its inputs from a map, so an input the config omits from `sources` would
// silently read zero and publish a wrong value as valid.
var derivedInputs = map[string][]string{
	"zuluftabluftprozent":      {"zuluftumin", "abluftumin"},
	"co2luefterstufe4":         {"co2luefterstufe3"},
	"beschattungtemp_adjusted": {"beschattungtemp", "beschattungaussentemp", "aussentemp"},
}

// Validate reports derived registers that have no implementation, or whose
// configured sources do not cover everything their formula reads, so the
// mistake surfaces at startup instead of as a wrong value published forever.
func (d *DerivedCalculator) Validate() error {
	var problems []string

	for _, reg := range d.registers {
		if _, ok := derivedFormulas[reg.Name]; !ok {
			problems = append(problems, fmt.Sprintf("%s has no formula implementation", reg.Name))
			continue
		}

		listed := make(map[string]bool, len(reg.Sources))
		for _, source := range reg.Sources {
			listed[source] = true
		}
		for _, input := range derivedInputs[reg.Name] {
			if !listed[input] {
				problems = append(problems, fmt.Sprintf(
					"%s reads %q but does not list it in sources; it would silently read 0",
					reg.Name, input))
			}
		}
	}

	if len(problems) > 0 {
		return fmt.Errorf("%w: %s", ErrNoFormula, strings.Join(problems, "; "))
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
