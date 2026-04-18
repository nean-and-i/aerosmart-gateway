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

	// Subscription tracking for resilience
	subscribedTopics map[string]MessageHandler // topic -> handler
	subMu            sync.RWMutex
}

// ConnectionHandler is a callback for connection state changes
type ConnectionHandler func(connected bool)

// MQTTConfig holds MQTT connection configuration
type MQTTConfig struct {
	Broker            string
	Port              int
	Username          string
	Password          string
	ClientID          string
	QOS               int
	Retain            bool
	PublishRetryCount int
}

// MessageHandler is a function type for handling incoming MQTT messages
type MessageHandler func(topic string, message string)

// NewClient creates a new MQTT client
func NewClient(config *MQTTConfig, deviceID string) *Client {
	return &Client{
		config:           config,
		deviceID:         deviceID,
		connected:        false,
		subscribedTopics: make(map[string]MessageHandler),
	}
}

// Connect connects to the MQTT broker with resilience features
func (c *Client) Connect() error {
	broker := fmt.Sprintf("tcp://%s:%d", c.config.Broker, c.config.Port)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(c.config.ClientID)
	opts.SetUsername(c.config.Username)
	opts.SetPassword(c.config.Password)

	// Enable persistent session for subscription recovery
	// This preserves subscriptions across reconnects
	opts.SetCleanSession(false)

	// Enable auto-reconnect with configurable interval
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)
	opts.SetMaxReconnectInterval(60 * time.Second)

	// Set KeepAlive to detect network failures within 30 seconds
	// This ensures faster disconnect detection compared to default
	opts.SetKeepAlive(30 * time.Second)

	// Set reconnection handler for visibility into reconnection progress
	opts.SetReconnectingHandler(func(client mqtt.Client, opts *mqtt.ClientOptions) {
		fmt.Println("MQTT attempting to reconnect...")
	})

	// Set connection notification handler for state change visibility
	opts.SetConnectionNotificationHandler(func(client mqtt.Client, notification mqtt.ConnectionNotification) {
		fmt.Printf("MQTT connection state: %v\n", notification.Type())
	})

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

		// Auto-recover subscriptions after reconnect
		// This ensures we re-subscribe to all topics even with persistent session
		if err := c.resubscribeAll(); err != nil {
			fmt.Printf("MQTT failed to recover subscriptions: %v\n", err)
		}
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

// resubscribeAll re-subscribes to all previously subscribed topics
func (c *Client) resubscribeAll() error {
	c.subMu.RLock()
	topicCount := len(c.subscribedTopics)
	c.subMu.RUnlock()

	if topicCount == 0 {
		return nil
	}

	fmt.Printf("MQTT recovering %d subscriptions...\n", topicCount)

	c.subMu.RLock()
	topics := make(map[string]MessageHandler)
	for topic, handler := range c.subscribedTopics {
		topics[topic] = handler
	}
	c.subMu.RUnlock()

	for topic, handler := range topics {
		if err := c.subscribeTopic(topic, handler); err != nil {
			return fmt.Errorf("failed to subscribe to %s: %w", topic, err)
		}
	}

	fmt.Printf("MQTT all subscriptions recovered\n")
	return nil
}

// subscribeTopic subscribes to a single topic with QoS 1
func (c *Client) subscribeTopic(topic string, handler MessageHandler) error {
	callback := func(client mqtt.Client, msg mqtt.Message) {
		handler(msg.Topic(), string(msg.Payload()))
	}

	// Use QoS 1 for at-least-once delivery guarantee
	token := c.client.Subscribe(topic, 1, callback)
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}

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

// GetSubscriptionCount returns the number of active subscriptions
// Useful for health monitoring and debugging
func (c *Client) GetSubscriptionCount() int {
	c.subMu.RLock()
	defer c.subMu.RUnlock()
	return len(c.subscribedTopics)
}

// GetSubscribedTopics returns a copy of currently subscribed topics
// Useful for debugging and health checks
func (c *Client) GetSubscribedTopics() []string {
	c.subMu.RLock()
	defer c.subMu.RUnlock()
	topics := make([]string, 0, len(c.subscribedTopics))
	for topic := range c.subscribedTopics {
		topics = append(topics, topic)
	}
	return topics
}

// Publish publishes a message to a topic with QoS 1 and retry
func (c *Client) Publish(topic string, value string) error {
	if !c.IsConnected() {
		return fmt.Errorf("MQTT client not connected")
	}

	// Get retry count from config, default to 3
	retryCount := c.config.PublishRetryCount
	if retryCount <= 0 {
		retryCount = 3
	}

	var lastErr error
	for attempt := 0; attempt < retryCount; attempt++ {
		// Use QoS 1 for at-least-once delivery guarantee
		token := c.client.Publish(topic, 1, c.config.Retain, value)
		if token.Wait() && token.Error() != nil {
			lastErr = fmt.Errorf("failed to publish to %s: %w", topic, token.Error())
			// Wait before retry with exponential backoff
			if attempt < retryCount-1 {
				delay := time.Duration(1<<attempt) * time.Second
				time.Sleep(delay)
				continue
			}
			return lastErr
		}
		return nil
	}

	return lastErr
}

// Subscribe subscribes to a topic with a message handler
// Uses QoS 1 for at-least-once delivery and tracks for auto-recovery
func (c *Client) Subscribe(topic string, handler MessageHandler) error {
	if !c.IsConnected() {
		return fmt.Errorf("MQTT client not connected")
	}

	callback := func(client mqtt.Client, msg mqtt.Message) {
		handler(msg.Topic(), string(msg.Payload()))
	}

	// Use QoS 1 for at-least-once delivery guarantee
	token := c.client.Subscribe(topic, 1, callback)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to subscribe to %s: %w", topic, token.Error())
	}

	// Track subscription for auto-recovery on reconnect
	c.subMu.Lock()
	c.subscribedTopics[topic] = handler
	c.subMu.Unlock()

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
