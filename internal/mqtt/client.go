package mqtt

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/nean/aerosmart-gateway/internal/config"
)

// Logger is the subset of logging this package needs.
type Logger interface {
	Debug(format string, args ...any)
	Info(format string, args ...any)
	Warn(format string, args ...any)
	Error(format string, args ...any)
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

	// shutdownPublishTimeout bounds the offline announcement. Shutdown runs
	// under a grace period after which the process is force-exited, so the
	// retrying publish path (attempts x OpTimeout plus backoff) is far too
	// slow to use here.
	shutdownPublishTimeout = 2 * time.Second
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
	// Tear down any client from a previous attempt: left running it would keep
	// trying to connect under the same ClientID and its OnConnect could still
	// fire against this Client.
	c.closeExisting()

	client := paho.NewClient(c.clientOptions())
	c.mu.Lock()
	c.client = client
	c.mu.Unlock()

	token := client.Connect()
	if !token.WaitTimeout(c.config.OpTimeout) {
		return fmt.Errorf("connect to %s timed out after %v", c.brokerURL(), c.config.OpTimeout)
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("connect to %s: %w", c.brokerURL(), err)
	}
	return nil
}

// brokerURL is the broker address in paho's form.
func (c *Client) brokerURL() string {
	return fmt.Sprintf("tcp://%s:%d", c.config.Broker, c.config.Port)
}

// clientOptions builds the paho options. Kept separate so the choices below
// can be asserted in tests: they are what make the connection survive a
// flaky network without stalling the serial read loop.
func (c *Client) clientOptions() *paho.ClientOptions {
	broker := c.brokerURL()

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

	// Without this, paho leaves the socket write with no deadline and blocks
	// the caller for a hardcoded 30s handing the packet to its unbuffered
	// outgoing channel. That happens inside Publish, before any token is
	// waited on, so the token timeouts below would bound nothing: a broker
	// that keeps the socket open but stops reading would stall the serial
	// read loop and overrun the shutdown grace period.
	opts.SetWriteTimeout(c.config.OpTimeout)

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

	return opts
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
	// A reconnect landing as shutdown begins must not re-announce "online":
	// a clean disconnect suppresses the last will, so a retained "online"
	// would outlive the process and leave every entity looking available.
	if c.shuttingDown.Load() {
		return
	}
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

// publishOnce makes a single bounded publish attempt, for use where the
// retrying path would take too long.
func (c *Client) publishOnce(topic, value string, timeout time.Duration) error {
	client := c.handle()
	if client == nil || !client.IsConnected() {
		return fmt.Errorf("publish to %s: not connected", topic)
	}

	token := client.Publish(topic, byte(c.config.QOS), true, value)
	if !token.WaitTimeout(timeout) {
		return fmt.Errorf("publish to %s: timed out after %v", topic, timeout)
	}
	return token.Error()
}

// resubscribeAll re-subscribes to every tracked topic, continuing past
// individual failures so one bad topic cannot leave the rest unsubscribed.
func (c *Client) resubscribeAll() {
	c.subMu.RLock()
	topics := make(map[string]MessageHandler, len(c.subscribedTopics))
	maps.Copy(topics, c.subscribedTopics)
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

	// Set this first so any publish already in flight elsewhere stops
	// retrying rather than sleeping through its backoff.
	c.shuttingDown.Store(true)

	if client.IsConnected() && c.config.AvailabilityTopic != "" {
		timeout := shutdownPublishTimeout
		if c.config.OpTimeout > 0 && c.config.OpTimeout < timeout {
			timeout = c.config.OpTimeout
		}
		if err := c.publishOnce(c.config.AvailabilityTopic, payloadOffline, timeout); err != nil {
			c.logger.Warn("MQTT failed to publish offline state: %v", err)
		}
	}

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
		client := c.handle()
		if client == nil || !client.IsConnected() {
			// A disconnected broker will not reconnect any faster for this
			// call having waited: reconnection is paho's own goroutine, and
			// this runs on the serial read path. Retrying immediately just
			// wastes the round trip; sleeping through every remaining
			// attempt's backoff first would waste 750ms per topic, on every
			// one of the dozens published each cycle.
			return fmt.Errorf("publish to %s: not connected", topic)
		}

		if attempt > 1 {
			if c.shuttingDown.Load() {
				return lastErr
			}
			time.Sleep(time.Duration(attempt-1) * 250 * time.Millisecond)
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
		// The broker hands a retained message to every new subscription, and
		// every reconnect re-subscribes. A command published with the retain
		// flag would be applied again after each connection blip - an
		// hours-old party mode coming back. Only live commands are applied.
		if msg.Retained() {
			c.logger.Warn("MQTT: ignoring retained command %s=%s; publish commands without the retain flag",
				msg.Topic(), msg.Payload())
			return
		}
		handler(msg.Topic(), string(msg.Payload()))
	}

	token := client.Subscribe(topic, byte(c.config.QOS), callback)
	if !token.WaitTimeout(c.config.OpTimeout) {
		return fmt.Errorf("subscribe to %s timed out after %v", topic, c.config.OpTimeout)
	}
	return token.Error()
}

// PublishDiscovery publishes one discovery payload.
func (c *Client) PublishDiscovery(topic string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal discovery payload for %s: %w", topic, err)
	}
	// Discovery is always retained so Home Assistant rediscovers the entities
	// after a restart without the gateway republishing.
	return c.publishRetained(topic, string(data), true)
}

// ClearDiscovery removes a discovery entry Home Assistant has retained.
//
// An empty retained payload is the documented way to tell Home Assistant to
// forget an entity, as opposed to simply no longer updating it: a topic this
// gateway stops publishing to otherwise keeps whatever was last retained
// there forever, including across a broker restart.
func (c *Client) ClearDiscovery(topic string) error {
	return c.publishRetained(topic, "", true)
}

// PublishCommandConfig publishes a write register's discovery config and
// clears any component it is not currently published as.
//
// A register can change which entity type it is published as - luefterstufe
// moved from a switch to a number when component selection became
// range-aware - and Home Assistant keeps whatever was last retained at the
// old component's topic until something clears it. Left alone, an upgraded
// install ends up with both entities, and the stale one still accepts
// commands the device rejects.
func (c *Client) PublishCommandConfig(d Discovery, reg config.WriteRegisterConfig) (string, error) {
	component, topic, payload := d.CommandConfig(reg)
	if err := c.PublishDiscovery(topic, payload); err != nil {
		return component, err
	}
	for _, stale := range d.StaleCommandTopics(reg) {
		if err := c.ClearDiscovery(stale); err != nil {
			return component, fmt.Errorf("clear stale discovery %s: %w", stale, err)
		}
	}
	return component, nil
}

// ParseWriteRegisterCommand fills a command template with a value.
func ParseWriteRegisterCommand(template string, value string) string {
	return strings.Replace(template, "{value}", value, 1)
}
