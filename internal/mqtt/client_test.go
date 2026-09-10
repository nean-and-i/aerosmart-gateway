package mqtt

import (
	"sync"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/nean/aerosmart-gateway/internal/config"
)

// fakeToken satisfies paho.Token with a scripted outcome.
type fakeToken struct {
	completes bool // whether WaitTimeout returns true
	err       error
	blockFor  time.Duration
}

func (t *fakeToken) Wait() bool { return t.completes }

func (t *fakeToken) WaitTimeout(d time.Duration) bool {
	// Model a token that never completes: the caller burns the full timeout.
	if !t.completes {
		wait := d
		if t.blockFor > 0 && t.blockFor < d {
			wait = t.blockFor
		}
		time.Sleep(wait)
		return false
	}
	return true
}

func (t *fakeToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (t *fakeToken) Error() error { return t.err }

// fakePaho satisfies paho.Client, recording calls.
type fakePaho struct {
	mu sync.Mutex

	connected bool
	publishes []publishCall
	subs      []string
	disconns  int

	// token decides the outcome of each Publish.
	token func(topic string) paho.Token
}

type publishCall struct {
	topic   string
	payload string
	retain  bool
}

func (f *fakePaho) IsConnected() bool      { f.mu.Lock(); defer f.mu.Unlock(); return f.connected }
func (f *fakePaho) IsConnectionOpen() bool { return f.IsConnected() }
func (f *fakePaho) Connect() paho.Token    { return &fakeToken{completes: true} }

func (f *fakePaho) Disconnect(uint) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.disconns++
	f.connected = false
}

func (f *fakePaho) Publish(topic string, _ byte, retained bool, payload interface{}) paho.Token {
	f.mu.Lock()
	f.publishes = append(f.publishes, publishCall{topic: topic, payload: payload.(string), retain: retained})
	tokenFn := f.token
	f.mu.Unlock()

	if tokenFn != nil {
		return tokenFn(topic)
	}
	return &fakeToken{completes: true}
}

func (f *fakePaho) Subscribe(topic string, _ byte, _ paho.MessageHandler) paho.Token {
	f.mu.Lock()
	f.subs = append(f.subs, topic)
	f.mu.Unlock()
	return &fakeToken{completes: true}
}

func (f *fakePaho) SubscribeMultiple(map[string]byte, paho.MessageHandler) paho.Token {
	return &fakeToken{completes: true}
}
func (f *fakePaho) Unsubscribe(...string) paho.Token        { return &fakeToken{completes: true} }
func (f *fakePaho) AddRoute(string, paho.MessageHandler)    {}
func (f *fakePaho) OptionsReader() paho.ClientOptionsReader { return paho.ClientOptionsReader{} }

func (f *fakePaho) publishCount(topic string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, p := range f.publishes {
		if p.topic == topic {
			n++
		}
	}
	return n
}

type nopLogger struct{}

func (nopLogger) Debug(string, ...interface{}) {}
func (nopLogger) Info(string, ...interface{})  {}
func (nopLogger) Warn(string, ...interface{})  {}
func (nopLogger) Error(string, ...interface{}) {}

func testClient(t *testing.T, fake *fakePaho) *Client {
	t.Helper()
	c := NewClient(&MQTTConfig{
		Broker:            "127.0.0.1",
		Port:              1883,
		QOS:               1,
		Retain:            true,
		PublishRetryCount: 3,
		AvailabilityTopic: "aerosmart/status",
		OpTimeout:         200 * time.Millisecond,
	}, nopLogger{})
	c.client = fake
	return c
}

func TestPublishSucceeds(t *testing.T) {
	fake := &fakePaho{connected: true}
	c := testClient(t, fake)

	if err := c.Publish("aerosmart/co2", "812"); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if fake.publishCount("aerosmart/co2") != 1 {
		t.Errorf("publishes = %d, want 1", fake.publishCount("aerosmart/co2"))
	}
	if !fake.publishes[0].retain {
		t.Error("publish was not retained, want retained per config")
	}
}

func TestPublishReportsTimeoutAndRetries(t *testing.T) {
	fake := &fakePaho{connected: true, token: func(string) paho.Token {
		return &fakeToken{completes: false}
	}}
	c := testClient(t, fake)

	err := c.Publish("aerosmart/co2", "812")
	if err == nil {
		t.Fatal("Publish() error = nil, want a timeout error")
	}
	if fake.publishCount("aerosmart/co2") != 3 {
		t.Errorf("attempts = %d, want 3", fake.publishCount("aerosmart/co2"))
	}
}

func TestPublishFailsFastWhileDisconnected(t *testing.T) {
	fake := &fakePaho{connected: false}
	c := testClient(t, fake)

	start := time.Now()
	if err := c.Publish("aerosmart/co2", "812"); err == nil {
		t.Fatal("Publish() error = nil, want a not-connected error")
	}
	// No broker interaction, so only the retry sleeps elapse (250+500ms).
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Publish() took %v while disconnected, want well under 2s", elapsed)
	}
	if fake.publishCount("aerosmart/co2") != 0 {
		t.Error("published while disconnected")
	}
}

// Shutdown must be bounded: the offline announcement gets one attempt with a
// short deadline, so a broker that stops acknowledging cannot stretch shutdown
// past the grace period and let the watchdog kill the process before the
// serial port is closed.
func TestDisconnectOfflinePublishIsBounded(t *testing.T) {
	fake := &fakePaho{connected: true, token: func(string) paho.Token {
		return &fakeToken{completes: false} // never acknowledges
	}}
	c := testClient(t, fake)

	start := time.Now()
	c.Disconnect()
	elapsed := time.Since(start)

	if n := fake.publishCount("aerosmart/status"); n != 1 {
		t.Errorf("offline publish attempts = %d, want exactly 1", n)
	}
	if elapsed > time.Second {
		t.Errorf("Disconnect() took %v, want under 1s so shutdown stays inside its grace period", elapsed)
	}
	if fake.disconns == 0 {
		t.Error("Disconnect() never closed the paho client")
	}
}

func TestDisconnectPublishesOfflineState(t *testing.T) {
	fake := &fakePaho{connected: true}
	c := testClient(t, fake)

	c.Disconnect()

	if len(fake.publishes) == 0 {
		t.Fatal("Disconnect() published nothing, want an offline announcement")
	}
	last := fake.publishes[len(fake.publishes)-1]
	if last.topic != "aerosmart/status" || last.payload != payloadOffline {
		t.Errorf("published %q=%q, want aerosmart/status=%s", last.topic, last.payload, payloadOffline)
	}
	if !last.retain {
		t.Error("offline state was not retained; Home Assistant would not see it after a restart")
	}
}

func TestDisconnectWithoutClientIsSafe(t *testing.T) {
	c := NewClient(&MQTTConfig{AvailabilityTopic: "aerosmart/status"}, nopLogger{})
	c.Disconnect() // must not panic
	if c.IsConnected() {
		t.Error("IsConnected() = true with no client")
	}
}

func TestSubscribeTracksTopicForRecovery(t *testing.T) {
	fake := &fakePaho{connected: true}
	c := testClient(t, fake)

	if err := c.Subscribe("dw/aerosmart/luefterstufe", func(string, string) {}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	c.subMu.RLock()
	_, tracked := c.subscribedTopics["dw/aerosmart/luefterstufe"]
	c.subMu.RUnlock()
	if !tracked {
		t.Error("topic was not tracked; it would not be restored after a reconnect")
	}

	// A reconnect must re-subscribe every tracked topic.
	fake.mu.Lock()
	fake.subs = nil
	fake.mu.Unlock()
	c.resubscribeAll()

	fake.mu.Lock()
	got := append([]string(nil), fake.subs...)
	fake.mu.Unlock()
	if len(got) != 1 || got[0] != "dw/aerosmart/luefterstufe" {
		t.Errorf("resubscribed topics = %v, want the tracked topic", got)
	}
}

// One failing topic must not stop the others from being restored.
func TestResubscribeContinuesPastFailures(t *testing.T) {
	fake := &fakePaho{connected: true}
	c := testClient(t, fake)
	for _, topic := range []string{"a", "b", "c"} {
		c.subscribedTopics[topic] = func(string, string) {}
	}

	c.resubscribeAll()

	if n := len(fake.subs); n != 3 {
		t.Errorf("resubscribed %d topics, want 3", n)
	}
}

func TestPublishDiscoveryMarshalsAndRetains(t *testing.T) {
	fake := &fakePaho{connected: true}
	c := testClient(t, fake)

	reg := config.ReadRegisterConfig{Name: "co2", Topic: "aerosmart/co2"}
	_, payload := Discovery{DeviceID: "aerosmart", Prefix: "homeassistant"}.SensorConfig(reg)
	if err := c.PublishDiscovery("homeassistant/sensor/aerosmart/aerosmart_co2/config", payload); err != nil {
		t.Fatalf("PublishDiscovery() error = %v", err)
	}

	if len(fake.publishes) != 1 {
		t.Fatalf("publishes = %d, want 1", len(fake.publishes))
	}
	if !fake.publishes[0].retain {
		t.Error("discovery was not retained; Home Assistant would lose entities on restart")
	}
}

// The paho options carry the whole reliability story, so pin them.
func TestClientOptions(t *testing.T) {
	c := NewClient(&MQTTConfig{
		Broker:            "192.168.1.20",
		Port:              1883,
		ClientID:          "aerosmart-gateway",
		QOS:               1,
		AvailabilityTopic: "aerosmart/status",
		OpTimeout:         5 * time.Second,
	}, nopLogger{})

	o := c.clientOptions()

	// Without a write timeout, paho leaves the socket write with no deadline
	// and blocks the caller for a hardcoded 30s handing the packet to its
	// unbuffered outgoing channel. OpTimeout would then bound nothing: the
	// blocking happens inside Publish, before the token is ever waited on.
	if got := o.WriteTimeout; got != 5*time.Second {
		t.Errorf("WriteTimeout() = %v, want the configured op_timeout; publishes would not be bounded", got)
	}
	if got := o.ConnectTimeout; got != 5*time.Second {
		t.Errorf("ConnectTimeout() = %v, want 5s", got)
	}
	// A persistent session would let the broker queue commands while the
	// gateway is offline and replay them at startup.
	if !o.CleanSession {
		t.Error("CleanSession() = false, want true")
	}
	if !o.AutoReconnect {
		t.Error("AutoReconnect() = false, want true")
	}
	if !o.WillEnabled {
		t.Fatal("WillEnabled = false, want a last will so an unclean exit marks the gateway offline")
	}
	if o.WillTopic != "aerosmart/status" || string(o.WillPayload) != payloadOffline || !o.WillRetained {
		t.Errorf("will = %q/%q retained=%v, want aerosmart/status/%s retained",
			o.WillTopic, o.WillPayload, o.WillRetained, payloadOffline)
	}
}

// An announcement racing a shutdown must not leave the availability topic
// retained as "online" after the process exits: a clean disconnect suppresses
// the last will, so nothing would correct it.
func TestOnConnectedSkipsAnnouncementDuringShutdown(t *testing.T) {
	fake := &fakePaho{connected: true}
	c := testClient(t, fake)
	c.shuttingDown.Store(true)

	c.onConnected()

	if n := fake.publishCount("aerosmart/status"); n != 0 {
		t.Errorf("published availability %d times during shutdown, want 0", n)
	}
}
