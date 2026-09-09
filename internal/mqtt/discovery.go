package mqtt

import (
	"fmt"

	"github.com/nean/aerosmart-gateway/internal/config"
)

// HASensorConfig is a Home Assistant sensor discovery payload.
type HASensorConfig struct {
	Name              string                 `json:"name"`
	StateTopic        string                 `json:"state_topic"`
	Unit              string                 `json:"unit_of_measurement,omitempty"`
	DeviceClass       string                 `json:"device_class,omitempty"`
	StateClass        string                 `json:"state_class,omitempty"`
	UniqueID          string                 `json:"unique_id"`
	AvailabilityTopic string                 `json:"availability_topic,omitempty"`
	ExpireAfter       int                    `json:"expire_after,omitempty"`
	Device            map[string]interface{} `json:"device"`
}

// HASwitchConfig is a Home Assistant switch discovery payload.
//
// PayloadOn/Off and StateOn/Off are always set explicitly: Home Assistant
// defaults to "ON"/"OFF", which this device neither accepts nor reports.
type HASwitchConfig struct {
	Name              string                 `json:"name"`
	CommandTopic      string                 `json:"command_topic"`
	StateTopic        string                 `json:"state_topic,omitempty"`
	PayloadOn         string                 `json:"payload_on"`
	PayloadOff        string                 `json:"payload_off"`
	StateOn           string                 `json:"state_on"`
	StateOff          string                 `json:"state_off"`
	UniqueID          string                 `json:"unique_id"`
	AvailabilityTopic string                 `json:"availability_topic,omitempty"`
	Device            map[string]interface{} `json:"device"`
}

// HANumberConfig is a Home Assistant number discovery payload, used for
// registers that take a range of values rather than on/off.
type HANumberConfig struct {
	Name              string                 `json:"name"`
	CommandTopic      string                 `json:"command_topic"`
	StateTopic        string                 `json:"state_topic,omitempty"`
	Min               int                    `json:"min"`
	Max               int                    `json:"max"`
	Step              int                    `json:"step"`
	Mode              string                 `json:"mode"`
	UniqueID          string                 `json:"unique_id"`
	AvailabilityTopic string                 `json:"availability_topic,omitempty"`
	Device            map[string]interface{} `json:"device"`
}

// Discovery builds Home Assistant MQTT discovery payloads.
type Discovery struct {
	DeviceID string
	Prefix   string
	Device   map[string]interface{}

	// AvailabilityTopic is where the gateway publishes online/offline, so
	// entities go unavailable instead of showing a stale retained value.
	AvailabilityTopic string

	// ExpireAfter marks an individual entity unavailable if it has not been
	// updated for this many seconds. Zero omits the field.
	ExpireAfter int
}

func (d Discovery) uniqueID(name string) string {
	return fmt.Sprintf("%s_%s", d.DeviceID, name)
}

func (d Discovery) topic(component, uniqueID string) string {
	return fmt.Sprintf("%s/%s/%s/%s/config", d.Prefix, component, d.DeviceID, uniqueID)
}

// SensorConfig returns the discovery topic and payload for a read register.
func (d Discovery) SensorConfig(reg config.ReadRegisterConfig) (string, HASensorConfig) {
	uid := d.uniqueID(reg.Name)
	return d.topic("sensor", uid), HASensorConfig{
		Name:              reg.HA.Name,
		StateTopic:        reg.Topic,
		Unit:              reg.HA.Unit,
		DeviceClass:       reg.HA.DeviceClass,
		UniqueID:          uid,
		AvailabilityTopic: d.AvailabilityTopic,
		ExpireAfter:       d.ExpireAfter,
		Device:            d.Device,
	}
}

// DerivedSensorConfig returns the discovery topic and payload for a derived
// register.
func (d Discovery) DerivedSensorConfig(reg config.DerivedRegisterConfig) (string, HASensorConfig) {
	uid := d.uniqueID(reg.Name)
	return d.topic("sensor", uid), HASensorConfig{
		Name:              reg.HA.Name,
		StateTopic:        reg.Topic,
		Unit:              reg.HA.Unit,
		DeviceClass:       reg.HA.DeviceClass,
		UniqueID:          uid,
		AvailabilityTopic: d.AvailabilityTopic,
		ExpireAfter:       d.ExpireAfter,
		Device:            d.Device,
	}
}

// CommandConfig returns the component name, discovery topic and payload for a
// write register.
//
// A register spanning more than on/off becomes a number entity. Publishing it
// as a switch made Home Assistant send "ON", which the device rejects.
//
// The command topic is always the writer's subscribe topic, so commands cannot
// be sent somewhere nothing is listening.
func (d Discovery) CommandConfig(reg config.WriteRegisterConfig) (string, string, interface{}) {
	uid := d.uniqueID(reg.Name)

	if reg.MinValue == 0 && reg.MaxValue == 1 {
		return "switch", d.topic("switch", uid), HASwitchConfig{
			Name:              reg.HA.Name,
			CommandTopic:      reg.SubscribeTopic,
			StateTopic:        reg.Topic,
			PayloadOn:         "1",
			PayloadOff:        "0",
			StateOn:           "1",
			StateOff:          "0",
			UniqueID:          uid,
			AvailabilityTopic: d.AvailabilityTopic,
			Device:            d.Device,
		}
	}

	return "number", d.topic("number", uid), HANumberConfig{
		Name:              reg.HA.Name,
		CommandTopic:      reg.SubscribeTopic,
		StateTopic:        reg.Topic,
		Min:               reg.MinValue,
		Max:               reg.MaxValue,
		Step:              1,
		Mode:              "box",
		UniqueID:          uid,
		AvailabilityTopic: d.AvailabilityTopic,
		Device:            d.Device,
	}
}

// CreateDeviceInfo builds the shared device block for discovery payloads.
func CreateDeviceInfo(deviceID, name, manufacturer, model, swVersion string) map[string]interface{} {
	info := map[string]interface{}{
		"identifiers":  []string{deviceID},
		"name":         name,
		"manufacturer": manufacturer,
		"model":        model,
	}
	if swVersion != "" {
		info["sw_version"] = swVersion
	}
	return info
}
