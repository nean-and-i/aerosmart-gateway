package mqtt

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/nean/aerosmart-gateway/internal/config"
)

func testDiscovery() Discovery {
	return Discovery{
		DeviceID:          "aerosmart",
		Prefix:            "homeassistant",
		Device:            CreateDeviceInfo("aerosmart", "Aerosmart Gateway", "Drexel und Weiss", "aerosmartPI", "1.0.0"),
		AvailabilityTopic: "aerosmart/status",
		ExpireAfter:       300,
	}
}

func TestSensorDiscoveryCarriesAvailabilityAndExpiry(t *testing.T) {
	topic, payload := testDiscovery().SensorConfig(config.ReadRegisterConfig{
		Name:  "innentemp",
		Topic: "aerosmart/innentemp",
	})
	payload.Name = "Aerosmart innentemp"

	if want := "homeassistant/sensor/aerosmart/aerosmart_innentemp/config"; topic != want {
		t.Errorf("topic = %q, want %q", topic, want)
	}
	if payload.UniqueID != "aerosmart_innentemp" {
		t.Errorf("unique_id = %q, want aerosmart_innentemp", payload.UniqueID)
	}
	if payload.AvailabilityTopic != "aerosmart/status" {
		t.Errorf("availability_topic = %q, want aerosmart/status", payload.AvailabilityTopic)
	}
	if payload.ExpireAfter != 300 {
		t.Errorf("expire_after = %d, want 300 (a stale sensor must go unavailable)", payload.ExpireAfter)
	}
}

// A 0..5 fan stage is not a switch: Home Assistant would send "ON"/"OFF",
// which the device rejects and which used to stall the read loop.
func TestRangedWriteRegisterBecomesNumberEntity(t *testing.T) {
	component, topic, payload := testDiscovery().CommandConfig(config.WriteRegisterConfig{
		Name:           "luefterstufe",
		Topic:          "aerosmart/luefterstufe_state",
		MinValue:       0,
		MaxValue:       5,
		SubscribeTopic: "dw/aerosmart/luefterstufe",
	})

	if component != "number" {
		t.Fatalf("component = %q, want number for a 0..5 register", component)
	}
	if !strings.HasPrefix(topic, "homeassistant/number/") {
		t.Errorf("topic = %q, want a number discovery topic", topic)
	}

	num, ok := payload.(HANumberConfig)
	if !ok {
		t.Fatalf("payload type = %T, want HANumberConfig", payload)
	}
	if num.Min != 0 || num.Max != 5 {
		t.Errorf("min/max = %v/%v, want 0/5", num.Min, num.Max)
	}
	if num.CommandTopic != "dw/aerosmart/luefterstufe" {
		t.Errorf("command_topic = %q, want the writer's subscribe topic", num.CommandTopic)
	}
	if num.StateTopic != "aerosmart/luefterstufe_state" {
		t.Errorf("state_topic = %q, want the register's own state topic", num.StateTopic)
	}
	if num.AvailabilityTopic != "aerosmart/status" {
		t.Errorf("availability_topic = %q, want aerosmart/status", num.AvailabilityTopic)
	}
}

// A genuine 0/1 register may be a switch, but only with explicit numeric
// payloads: the HA defaults are "ON"/"OFF".
func TestBinaryWriteRegisterBecomesSwitchWithNumericPayloads(t *testing.T) {
	component, _, payload := testDiscovery().CommandConfig(config.WriteRegisterConfig{
		Name:           "boilerheizstab",
		Topic:          "aerosmart/boilerheizstab_state",
		MinValue:       0,
		MaxValue:       1,
		SubscribeTopic: "dw/aerosmart/boilerheizstab",
	})

	if component != "switch" {
		t.Fatalf("component = %q, want switch for a 0..1 register", component)
	}
	sw, ok := payload.(HASwitchConfig)
	if !ok {
		t.Fatalf("payload type = %T, want HASwitchConfig", payload)
	}
	if sw.PayloadOn != "1" || sw.PayloadOff != "0" {
		t.Errorf("payload_on/off = %q/%q, want 1/0", sw.PayloadOn, sw.PayloadOff)
	}
	if sw.StateOn != "1" || sw.StateOff != "0" {
		t.Errorf("state_on/off = %q/%q, want 1/0", sw.StateOn, sw.StateOff)
	}
}

// The command topic HA is told to use must be the one the writer subscribes to.
func TestCommandTopicMatchesSubscribeTopic(t *testing.T) {
	reg := config.WriteRegisterConfig{
		Name:           "luefterstufe",
		SubscribeTopic: "dw/aerosmart/luefterstufe",
		MinValue:       0,
		MaxValue:       5,
	}
	_, _, payload := testDiscovery().CommandConfig(reg)
	num := payload.(HANumberConfig)
	if num.CommandTopic != reg.SubscribeTopic {
		t.Errorf("command_topic = %q, subscribe topic = %q; they must match or commands are lost",
			num.CommandTopic, reg.SubscribeTopic)
	}
}

func TestSensorPayloadOmitsEmptyOptionalFields(t *testing.T) {
	d := testDiscovery()
	d.ExpireAfter = 0
	_, payload := d.SensorConfig(config.ReadRegisterConfig{Name: "x", Topic: "aerosmart/x"})

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	for _, absent := range []string{"expire_after", "device_class", "unit_of_measurement"} {
		if strings.Contains(string(data), absent) {
			t.Errorf("payload contains %q for an unset field: %s", absent, data)
		}
	}
}

func TestDerivedSensorDiscovery(t *testing.T) {
	topic, payload := testDiscovery().DerivedSensorConfig(config.DerivedRegisterConfig{
		Name:  "zuluftabluftprozent",
		Topic: "aerosmart/zuluftabluftprozent",
	})
	if want := "homeassistant/sensor/aerosmart/aerosmart_zuluftabluftprozent/config"; topic != want {
		t.Errorf("topic = %q, want %q", topic, want)
	}
	if payload.AvailabilityTopic != "aerosmart/status" {
		t.Errorf("availability_topic = %q, want aerosmart/status", payload.AvailabilityTopic)
	}
}

func TestParseWriteRegisterCommand(t *testing.T) {
	if got := ParseWriteRegisterCommand("130 5002 {value}", "3"); got != "130 5002 3" {
		t.Errorf("ParseWriteRegisterCommand() = %q, want %q", got, "130 5002 3")
	}
}

// A write register spanning exactly 0-1 becomes a switch; everything wider
// becomes a number. Whichever it isn't must be cleared, so an install that
// upgrades across that boundary - or across the switch-to-number change this
// register went through - does not keep a stale retained entity that still
// accepts commands the device rejects.
func TestStaleCommandTopicsNamesTheOtherComponent(t *testing.T) {
	numberReg := config.WriteRegisterConfig{Name: "luefterstufe", MinValue: 0, MaxValue: 5}
	stale := testDiscovery().StaleCommandTopics(numberReg)

	if len(stale) != 1 || stale[0] != "homeassistant/switch/aerosmart/aerosmart_luefterstufe/config" {
		t.Errorf("stale topics = %v, want exactly the switch topic for a number entity", stale)
	}

	switchReg := config.WriteRegisterConfig{Name: "boilerheizstab", MinValue: 0, MaxValue: 1}
	stale = testDiscovery().StaleCommandTopics(switchReg)

	if len(stale) != 1 || stale[0] != "homeassistant/number/aerosmart/aerosmart_boilerheizstab/config" {
		t.Errorf("stale topics = %v, want exactly the number topic for a switch entity", stale)
	}
}
