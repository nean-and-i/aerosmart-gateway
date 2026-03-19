package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
)

// Client represents an MQTT client for communicating with Home Assistant
type Client struct {
	client    mqtt.Client
	config    *MQTTConfig
	connected bool
	mu        sync.RWMutex
	deviceID  string
}

// MQTTConfig holds MQTT connection configuration
type MQTTConfig struct {
	Broker   string
	Port     int
	Username string
	Password string
	ClientID string
	QOS      int
	Retain   bool
}

// MessageHandler is a function type for handling incoming MQTT messages
type MessageHandler func(topic string, message string)

// NewClient creates a new MQTT client
func NewClient(config *MQTTConfig, deviceID string) *Client {
	return &Client{
		config:    config,
		deviceID:  deviceID,
		connected: false,
	}
}

// Connect connects to the MQTT broker
func (c *Client) Connect() error {
	broker := fmt.Sprintf("tcp://%s:%d", c.config.Broker, c.config.Port)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(c.config.ClientID)
	opts.SetUsername(c.config.Username)
	opts.SetPassword(c.config.Password)
	opts.SetCleanSession(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)
	opts.SetAutoReconnect(true)

	// Set connection handlers
	opts.OnConnectionLost = func(client mqtt.Client, err error) {
		c.mu.Lock()
		c.connected = false
		c.mu.Unlock()
		fmt.Printf("MQTT connection lost: %v\n", err)
	}

	opts.OnConnect = func(client mqtt.Client) {
		c.mu.Lock()
		c.connected = true
		c.mu.Unlock()
		fmt.Printf("MQTT connected to %s\n", broker)
	}

	c.client = mqtt.NewClient(opts)

	if token := c.client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	c.mu.Lock()
	c.connected = true
	c.mu.Unlock()

	return nil
}

// Disconnect disconnects from the MQTT broker
func (c *Client) Disconnect() {
	if c.client != nil {
		c.client.Disconnect(250)
	}
	c.mu.Lock()
	c.connected = false
	c.mu.Unlock()
}

// IsConnected returns true if the MQTT client is connected
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected && c.client != nil
}

// Publish publishes a message to a topic
func (c *Client) Publish(topic string, value string) error {
	if !c.IsConnected() {
		return fmt.Errorf("MQTT client not connected")
	}

	token := c.client.Publish(topic, byte(c.config.QOS), c.config.Retain, value)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to publish to %s: %w", topic, token.Error())
	}

	return nil
}

// Subscribe subscribes to a topic with a message handler
func (c *Client) Subscribe(topic string, handler MessageHandler) error {
	if !c.IsConnected() {
		return fmt.Errorf("MQTT client not connected")
	}

	callback := func(client mqtt.Client, msg mqtt.Message) {
		handler(msg.Topic(), string(msg.Payload()))
	}

	token := c.client.Subscribe(topic, byte(c.config.QOS), callback)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to subscribe to %s: %w", topic, token.Error())
	}

	return nil
}

// SubscribeMultiple subscribes to multiple topics
func (c *Client) SubscribeMultiple(topics []string, handler MessageHandler) error {
	for _, topic := range topics {
		if err := c.Subscribe(topic, handler); err != nil {
			return err
		}
	}
	return nil
}

// HASensorConfig holds Home Assistant sensor discovery configuration
type HASensorConfig struct {
	Name        string                 `json:"name"`
	StateTopic  string                 `json:"state_topic"`
	Unit        string                 `json:"unit_of_measurement,omitempty"`
	DeviceClass string                 `json:"device_class,omitempty"`
	UniqueID    string                 `json:"unique_id"`
	Device      map[string]interface{} `json:"device"`
}

// HASwitchConfig holds Home Assistant switch discovery configuration
type HASwitchConfig struct {
	Name         string                 `json:"name"`
	CommandTopic string                 `json:"command_topic"`
	StateTopic   string                 `json:"state_topic,omitempty"`
	UniqueID     string                 `json:"unique_id"`
	Device       map[string]interface{} `json:"device"`
}

// PublishSensorDiscovery publishes sensor discovery config to HA
func (c *Client) PublishSensorDiscovery(sensor *HASensorConfig) error {
	topic := fmt.Sprintf("homeassistant/sensor/%s/%s/config", c.deviceID, sensor.UniqueID)

	data, err := json.Marshal(sensor)
	if err != nil {
		return fmt.Errorf("failed to marshal sensor config: %w", err)
	}

	return c.Publish(topic, string(data))
}

// PublishSwitchDiscovery publishes switch discovery config to HA
func (c *Client) PublishSwitchDiscovery(switchCfg *HASwitchConfig) error {
	topic := fmt.Sprintf("homeassistant/switch/%s/%s/config", c.deviceID, switchCfg.UniqueID)

	data, err := json.Marshal(switchCfg)
	if err != nil {
		return fmt.Errorf("failed to marshal switch config: %w", err)
	}

	return c.Publish(topic, string(data))
}

// CreateDeviceInfo creates device info for HA discovery
func CreateDeviceInfo(deviceID, name, manufacturer, model string) map[string]interface{} {
	return map[string]interface{}{
		"identifiers":  []string{deviceID},
		"name":         name,
		"manufacturer": manufacturer,
		"model":        model,
	}
}

// PublishAllSensorDiscovery publishes discovery configs for all sensors
func (c *Client) PublishAllSensorDiscovery(sensors []HASensorConfig) error {
	for _, sensor := range sensors {
		if err := c.PublishSensorDiscovery(&sensor); err != nil {
			return err
		}
		time.Sleep(2 * time.Millisecond) // Small delay between publishes
	}
	return nil
}

// PublishAllSwitchDiscovery publishes discovery configs for all switches
func (c *Client) PublishAllSwitchDiscovery(switches []HASwitchConfig) error {
	for _, sw := range switches {
		if err := c.PublishSwitchDiscovery(&sw); err != nil {
			return err
		}
		time.Sleep(2 * time.Millisecond) // Small delay between publishes
	}
	return nil
}

// GetTopicsFromConfig extracts subscribe topics from write register config
func GetTopicsFromConfig(writeRegisters []WriteRegisterConfig) []string {
	topics := make([]string, 0, len(writeRegisters))
	for _, reg := range writeRegisters {
		if reg.SubscribeTopic != "" {
			topics = append(topics, reg.SubscribeTopic)
		}
	}
	return topics
}

// WriteRegisterConfig holds configuration for a write register
type WriteRegisterConfig struct {
	Name            string
	SubscribeTopic  string
	CommandTemplate string
	Topic           string
	MinValue        int
	MaxValue        int
}

// ParseWriteRegisterCommand parses a command template with a value
func ParseWriteRegisterCommand(template string, value string) string {
	return strings.Replace(template, "{value}", value, 1)
}
