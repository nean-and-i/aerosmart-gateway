package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// Logger is the subset of logging this package needs.
type Logger interface {
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
}

// MQTTConfig holds MQTT connection configuration.
type MQTTConfig struct {
	Broker            string
	Port              int
	Username          string
	Password          string
	ClientID          string
	QOS               int
	Retain            bool
	PublishRetryCount int

	// AvailabilityTopic carries online/offline for Home Assistant. It is also
	// registered as the last will, so an unclean disconnect marks the gateway
	// offline instead of leaving stale retained values looking live.
	AvailabilityTopic string

	// OpTimeout bounds every broker interaction. Without it a broker that
	// holds the TCP connection open but stops acknowledging wedges the caller
	// forever, and publishes happen on the serial read path.
	OpTimeout time.Duration
}

const (
	payloadOnline  = "online"
	payloadOffline = "offline"
)

// MessageHandler handles an incoming MQTT message.
type MessageHandler func(topic string, message string)

// Client is an MQTT client for talking to Home Assistant.
type Client struct {
	// mu guards client, which is replaced on every Connect attempt and read
	// from paho's callback goroutines and the read loop.
	mu     sync.RWMutex
	client paho.Client

	config *MQTTConfig
	logger Logger

	subMu            sync.RWMutex
	subscribedTopics map[string]MessageHandler

	// shuttingDown makes publishes fail fast instead of sleeping through their
	// retry backoff, which could otherwise outlast the shutdown grace period.
	shuttingDown atomic.Bool
}

// NewClient creates a new MQTT client.
func NewClient(config *MQTTConfig, log Logger) *Client {
	if config != nil && config.OpTimeout <= 0 {
		config.OpTimeout = 5 * time.Second
	}
	return &Client{
		config:           config,
		logger:           log,
		subscribedTopics: make(map[string]MessageHandler),
	}
}

// Connect connects to the broker.
//
// Automatic connect-retry is deliberately left off: with it enabled paho's
// Connect token never completes while the broker is down, which makes the
// caller's own retry, backoff and cancellation unreachable. Reconnection after
// an established session is still handled by paho.
func (c *Client) Connect() error {
	broker := fmt.Sprintf("tcp://%s:%d", c.config.Broker, c.config.Port)

	opts := paho.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(c.config.ClientID)
	opts.SetUsername(c.config.Username)
	opts.SetPassword(c.config.Password)

	// A clean session avoids the broker queueing commands while the gateway is
	// offline and replaying them on reconnect: an hours-old fan command must
	// not be applied at startup. Subscriptions are restored by OnConnect.
	opts.SetCleanSession(true)

	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(60 * time.Second)
	opts.SetKeepAlive(30 * time.Second)
	opts.SetConnectTimeout(c.config.OpTimeout)

	if c.config.AvailabilityTopic != "" {
		opts.SetWill(c.config.AvailabilityTopic, payloadOffline, byte(c.config.QOS), true)
	}

	opts.SetConnectionLostHandler(func(_ paho.Client, err error) {
		c.logger.Warn("MQTT connection lost: %v", err)
	})

	opts.SetReconnectingHandler(func(_ paho.Client, _ *paho.ClientOptions) {
		c.logger.Info("MQTT reconnecting to %s...", broker)
	})

	opts.SetOnConnectHandler(func(_ paho.Client) {
		c.logger.Info("MQTT connected to %s", broker)
		// This runs on paho's connection goroutine, which must not block, so
		// recovery happens separately.
		go c.onConnected()
	})

	// Tear down any client from a previous attempt: left running it would keep
	// trying to connect under the same ClientID and its OnConnect could still
	// fire against this Client.
	c.closeExisting()

	client := paho.NewClient(opts)
	c.mu.Lock()
	c.client = client
	c.mu.Unlock()

	token := client.Connect()
	if !token.WaitTimeout(c.config.OpTimeout) {
		return fmt.Errorf("connect to %s timed out after %v", broker, c.config.OpTimeout)
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("connect to %s: %w", broker, err)
	}
	return nil
}

// closeExisting disconnects a client left over from an earlier attempt.
func (c *Client) closeExisting() {
	c.mu.Lock()
	previous := c.client
	c.client = nil
	c.mu.Unlock()

	if previous != nil {
		previous.Disconnect(0)
	}
}

// handle returns the current paho client, or nil.
func (c *Client) handle() paho.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.client
}

// onConnected restores subscriptions and announces availability.
func (c *Client) onConnected() {
	if err := c.publishAvailability(payloadOnline); err != nil {
		c.logger.Error("MQTT failed to announce availability: %v", err)
	}
	c.resubscribeAll()
}

// publishAvailability marks the gateway online or offline.
func (c *Client) publishAvailability(state string) error {
	if c.config.AvailabilityTopic == "" {
		return nil
	}
	return c.publishRetained(c.config.AvailabilityTopic, state, true)
}

// resubscribeAll re-subscribes to every tracked topic, continuing past
// individual failures so one bad topic cannot leave the rest unsubscribed.
func (c *Client) resubscribeAll() {
	c.subMu.RLock()
	topics := make(map[string]MessageHandler, len(c.subscribedTopics))
	for topic, handler := range c.subscribedTopics {
		topics[topic] = handler
	}
	c.subMu.RUnlock()

	if len(topics) == 0 {
		return
	}

	c.logger.Info("MQTT restoring %d subscription(s)...", len(topics))
	failed := 0
	for topic, handler := range topics {
		if err := c.subscribe(topic, handler); err != nil {
			c.logger.Error("MQTT failed to restore subscription to %s: %v", topic, err)
			failed++
		}
	}
	if failed > 0 {
		c.logger.Error("MQTT restored %d/%d subscription(s); commands on the rest will be missed",
			len(topics)-failed, len(topics))
		return
	}
	c.logger.Info("MQTT all subscriptions restored")
}

// Disconnect marks the gateway offline and closes the connection.
//
// Publishes stop retrying from here on, so a broker that has gone away cannot
// stretch shutdown past its grace period.
func (c *Client) Disconnect() {
	client := c.handle()
	if client == nil {
		return
	}
	if client.IsConnected() {
		if err := c.publishAvailability(payloadOffline); err != nil {
			c.logger.Warn("MQTT failed to publish offline state: %v", err)
		}
	}
	c.shuttingDown.Store(true)
	client.Disconnect(250)

	c.mu.Lock()
	c.client = nil
	c.mu.Unlock()
}

// IsConnected reports whether the broker connection is up. It asks paho rather
// than tracking a flag, so it cannot drift from reality.
func (c *Client) IsConnected() bool {
	client := c.handle()
	return client != nil && client.IsConnected()
}

// Publish publishes a value, retrying with backoff.
func (c *Client) Publish(topic string, value string) error {
	return c.publishRetained(topic, value, c.config.Retain)
}

func (c *Client) publishRetained(topic string, value string, retain bool) error {
	attempts := c.config.PublishRetryCount
	if attempts <= 0 {
		attempts = 3
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			if c.shuttingDown.Load() {
				return lastErr
			}
			time.Sleep(time.Duration(attempt-1) * 250 * time.Millisecond)
		}
		client := c.handle()
		if client == nil || !client.IsConnected() {
			lastErr = fmt.Errorf("publish to %s: not connected", topic)
			continue
		}

		token := client.Publish(topic, byte(c.config.QOS), retain, value)
		if !token.WaitTimeout(c.config.OpTimeout) {
			// Retained state publishes are last-write-wins, so a retry after
			// an unacknowledged publish is safe.
			lastErr = fmt.Errorf("publish to %s: timed out after %v", topic, c.config.OpTimeout)
			continue
		}
		if err := token.Error(); err != nil {
			lastErr = fmt.Errorf("publish to %s: %w", topic, err)
			continue
		}
		return nil
	}
	return lastErr
}

// Subscribe subscribes to a topic and tracks it for recovery on reconnect.
func (c *Client) Subscribe(topic string, handler MessageHandler) error {
	if c.handle() == nil {
		return fmt.Errorf("subscribe to %s: MQTT client not initialised", topic)
	}

	c.subMu.Lock()
	c.subscribedTopics[topic] = handler
	c.subMu.Unlock()

	return c.subscribe(topic, handler)
}

func (c *Client) subscribe(topic string, handler MessageHandler) error {
	client := c.handle()
	if client == nil {
		return fmt.Errorf("subscribe to %s: no MQTT client", topic)
	}

	callback := func(_ paho.Client, msg paho.Message) {
		handler(msg.Topic(), string(msg.Payload()))
	}

	token := client.Subscribe(topic, byte(c.config.QOS), callback)
	if !token.WaitTimeout(c.config.OpTimeout) {
		return fmt.Errorf("subscribe to %s timed out after %v", topic, c.config.OpTimeout)
	}
	return token.Error()
}

// PublishDiscovery publishes one discovery payload.
func (c *Client) PublishDiscovery(topic string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal discovery payload for %s: %w", topic, err)
	}
	// Discovery is always retained so Home Assistant rediscovers the entities
	// after a restart without the gateway republishing.
	return c.publishRetained(topic, string(data), true)
}

// ParseWriteRegisterCommand fills a command template with a value.
func ParseWriteRegisterCommand(template string, value string) string {
	return strings.Replace(template, "{value}", value, 1)
}
