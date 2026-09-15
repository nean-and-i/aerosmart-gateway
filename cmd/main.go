// Command aerosmart-gateway bridges a Drexel und Weiss Aerosmart ventilation
// unit's serial interface to MQTT / Home Assistant.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nean/aerosmart-gateway/internal/config"
	"github.com/nean/aerosmart-gateway/internal/logger"
	"github.com/nean/aerosmart-gateway/internal/mqtt"
	"github.com/nean/aerosmart-gateway/internal/registers"
	"github.com/nean/aerosmart-gateway/internal/serial"
)

var (
	configPath    string
	registersPath string
	showVersion   bool
	validateOnly  bool

	// version is replaced at build time via -X main.version. The fallback is
	// deliberately not a plausible release number, so an unstamped build is
	// obvious in the startup log.
	version = "dev (unstamped build)"
)

const (
	// shutdownGrace bounds the orderly shutdown before the process is forced
	// down.
	shutdownGrace = 10 * time.Second

	// skipRetryDelay is how soon to retry after a cycle that read nothing,
	// rather than waiting a whole read interval.
	skipRetryDelay = 5 * time.Second

	// commandQueueDepth bounds how many commands may wait to be applied.
	// Applying one takes serial I/O, so a deeper queue would only accumulate
	// stale commands.
	commandQueueDepth = 16
)

func init() {
	flag.StringVar(&configPath, "config", "config.yaml", "Path to config file")
	flag.StringVar(&registersPath, "registers", "registers.yaml", "Path to registers file")
	flag.BoolVar(&showVersion, "version", false, "Show version information")
	flag.BoolVar(&validateOnly, "validate", false, "Validate the configuration and exit")
}

func main() {
	flag.Parse()

	if showVersion {
		fmt.Printf("aerosmart-gateway version %s\n", version)
		return
	}

	if err := run(); err != nil {
		if errors.Is(err, context.Canceled) {
			// Stopped by SIGINT/SIGTERM before startup finished; not a failure.
			return
		}
		fmt.Fprintf(os.Stderr, "aerosmart-gateway: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	appConfig, registersConfig, err := loadConfig()
	if err != nil {
		return err
	}

	log := logger.NewWithConfig(logger.Config{
		Level:          appConfig.LogLevel,
		LogFile:        appConfig.Logging.LogFile,
		FileLogging:    appConfig.Logging.FileLogging,
		ConsoleLogging: appConfig.Logging.ConsoleLogging,
	})
	defer func() { _ = log.Close() }()

	log.Info("Starting Aerosmart Gateway %s", version)
	for _, note := range appConfig.Warnings() {
		log.Warn("Config: %s", note)
	}
	for _, note := range registersConfig.Warnings() {
		log.Warn("Registers: %s", note)
	}

	derivedCalc := registers.NewDerivedCalculator(nil, log, registersConfig.DerivedRegisters)
	if err := derivedCalc.Validate(); err != nil {
		return fmt.Errorf("registers config: %w", err)
	}

	if validateOnly {
		log.Info("Configuration is valid")
		return nil
	}

	// Ask the logger rather than the config: an unwritable path falls back to
	// console logging, and the startup line should say what is really happening.
	if log.FileLoggingActive() {
		log.Info("File logging enabled: %s", appConfig.Logging.LogFile)
	} else {
		log.Info("File logging disabled")
	}

	// Shutdown is driven by ctx; the signal handler also arms a watchdog so a
	// wedged teardown cannot hang the process forever.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 2)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	shutdownDone := make(chan struct{})

	go func() {
		select {
		case sig := <-sigChan:
			log.SignalReceived(sig)
			cancel()
		case <-ctx.Done():
			return
		}

		// A second signal, or an overrunning shutdown, exits immediately.
		select {
		case <-shutdownDone:
		case sig := <-sigChan:
			log.Warn("Second signal (%v) received, exiting now", sig)
			os.Exit(1)
		case <-time.After(shutdownGrace):
			log.Warn("Shutdown exceeded %v, forcing exit", shutdownGrace)
			os.Exit(1)
		}
	}()

	serialPort := serial.New(serial.Config{
		Port:                appConfig.Serial.Port,
		BaudRate:            appConfig.Serial.BaudRate,
		DataBits:            appConfig.Serial.ByteSize,
		Parity:              appConfig.Serial.Parity,
		StopBits:            appConfig.Serial.StopBits,
		ReadTimeout:         msDuration(appConfig.Serial.ReadTimeout),
		ResponseTimeout:     msDuration(appConfig.Serial.ResponseTimeout),
		ReopenBackoff:       msDuration(appConfig.Serial.ReopenBackoff),
		MaxReopenBackoff:    msDuration(appConfig.Serial.ReopenMaxBackoff),
		HealthCheckInterval: msDuration(appConfig.Serial.HealthCheckInterval),
		XonXoff:             appConfig.Serial.XonXoff,
		DsrDtr:              appConfig.Serial.DsrDtr,
	}, log)

	// Keep trying to open the port: on an embedded gateway the USB adapter may
	// appear after boot, and exiting here would only crash-loop under systemd.
	log.Info("Connecting to serial device %s...", appConfig.Serial.Port)
	if err := openSerialWithRetry(ctx, serialPort, log,
		msDuration(appConfig.Serial.ReopenBackoff),
		msDuration(appConfig.Serial.ReopenMaxBackoff)); err != nil {
		return fmt.Errorf("serial: %w", err)
	}
	log.Info("Serial device connected")

	// The supervisor is the only component that reopens the port.
	var wg sync.WaitGroup
	wg.Go(func() {
		serialPort.Supervise(ctx)
	})

	mqttClient := mqtt.NewClient(&mqtt.MQTTConfig{
		Broker:            appConfig.MQTT.Broker,
		Port:              appConfig.MQTT.Port,
		Username:          appConfig.MQTT.Username,
		Password:          appConfig.MQTT.Password,
		ClientID:          appConfig.MQTT.ClientID,
		QOS:               appConfig.MQTT.QOS,
		Retain:            appConfig.MQTT.Retain,
		PublishRetryCount: appConfig.MQTT.PublishRetryCount,
		AvailabilityTopic: appConfig.MQTT.AvailabilityTopic,
		OpTimeout:         msDuration(appConfig.MQTT.OpTimeout),
	}, log)

	log.Info("Connecting to MQTT broker %s:%d...", appConfig.MQTT.Broker, appConfig.MQTT.Port)
	if err := connectMQTTWithRetry(ctx, mqttClient, log, 10,
		appConfig.MQTT.ConnectRetryInitialDelayMs,
		appConfig.MQTT.ConnectRetryMaxDelayMs,
		appConfig.MQTT.ConnectRetryJitterPercent); err != nil {
		// Stop the supervisor before closing, or it reopens the port as the
		// process exits and toggles DTR/RTS at the device.
		cancel()
		wg.Wait()
		_ = serialPort.Close()
		return fmt.Errorf("mqtt: %w", err)
	}
	log.Info("MQTT broker connected")

	derivedCalc = registers.NewDerivedCalculator(mqttClient, log, registersConfig.DerivedRegisters)

	gate := registers.NewReadGate(msDuration(appConfig.Serial.WritePriorityTimeout))
	retries := appConfig.Serial.Retries
	backoff := msDuration(appConfig.Serial.RetryBackoff)

	reader := registers.NewReader(serialPort, mqttClient, log, registersConfig.ReadRegisters, retries, backoff)
	writer := registers.NewWriter(serialPort, mqttClient, log, registersConfig.WriteRegisters, gate, retries, backoff,
		msDuration(appConfig.Serial.WriteSettleDelay))
	writer.SetReader(reader)

	// Values that trail a command are read again shortly afterwards.
	runResettle := writer.SetResettleDelay(msDuration(appConfig.Serial.WriteResettleDelay))
	wg.Go(func() {
		runResettle(ctx)
	})
	poller := registers.NewPoller(reader, derivedCalc, gate, log, registersConfig.WriteRegisters)

	// Commands are applied by a dedicated worker, not on paho's router
	// goroutine. The worker is joined at shutdown, so the port is never closed
	// under a write that is mid-exchange.
	commands := newCommandQueue(commandQueueDepth, log)
	wg.Go(func() {
		commands.run(ctx, writer.HandleMessage)
	})

	// Subscribed before discovery is published: with CleanSession(true) the
	// broker holds no state for us, so a command arriving in the window
	// between an entity being discovered and us actually listening for it is
	// simply not delivered, silently, while Home Assistant already shows the
	// entity as available.
	if topics := writer.GetSubscribeTopics(); len(topics) > 0 {
		log.Info("Subscribing to command topics: %v", topics)
		for _, topic := range topics {
			if err := mqttClient.Subscribe(topic, commands.enqueue); err != nil {
				log.Error("Failed to subscribe to %s: %v", topic, err)
			}
		}
	}

	if appConfig.HADiscovery.Enabled {
		log.Info("Publishing Home Assistant discovery configs...")
		publishHADiscovery(mqttClient, registersConfig, appConfig, log)
	}

	log.Info("Starting periodic read loop (interval: %ds)", appConfig.ReadInterval)
	wg.Go(func() {
		pollLoop(ctx, poller, time.Duration(appConfig.ReadInterval)*time.Second, skipRetryDelay, log)
	})

	<-ctx.Done()

	// Disconnect first so no further commands are delivered, then wait for the
	// loops, then close the port. Close takes the transaction lock, so a write
	// already on the wire completes before the device is released.
	log.Info("Shutdown initiated, waiting for in-flight work...")
	mqttClient.Disconnect()
	wg.Wait()

	if err := serialPort.Close(); err != nil {
		log.Error("Error closing serial port: %v", err)
	}

	log.Info("Aerosmart Gateway stopped")
	close(shutdownDone)
	return nil
}

// loadConfig loads and validates both configuration files.
func loadConfig() (*config.AppConfig, *config.RegistersConfig, error) {
	appConfig, err := config.LoadAppConfig(configPath)
	if err != nil {
		return nil, nil, fmt.Errorf("loading %s: %w", configPath, err)
	}
	if appConfig.HADiscovery.DeviceInfo.SWVersion == "" {
		appConfig.HADiscovery.DeviceInfo.SWVersion = version
	}
	if err := appConfig.Validate(); err != nil {
		return nil, nil, err
	}

	registersConfig, err := config.LoadRegistersConfig(registersPath)
	if err != nil {
		return nil, nil, fmt.Errorf("loading %s: %w", registersPath, err)
	}
	if err := registersConfig.Validate(); err != nil {
		return nil, nil, err
	}
	if err := appConfig.ValidateCycleBudget(len(registersConfig.ReadRegisters)); err != nil {
		return nil, nil, err
	}

	return appConfig, registersConfig, nil
}

// cycler runs one read/publish cycle.
type cycler interface {
	RunCycle(ctx context.Context) error
}

// pollLoop runs a cycle immediately, then once per interval.
//
// The wait is measured from the end of each cycle rather than from a fixed
// ticker, so a readout that overruns the interval still leaves the device an
// idle gap instead of being polled back-to-back.
//
// skipDelay is used instead of interval when a cycle read nothing (a write was
// in flight, or the port is being recovered), so a momentary blip does not
// cost a whole interval of data.
func pollLoop(ctx context.Context, c cycler, interval, skipDelay time.Duration, log *logger.Logger) {
	if interval <= 0 {
		interval = time.Minute
	}
	if skipDelay <= 0 || skipDelay > interval {
		skipDelay = interval
	}

	for {
		err := c.RunCycle(ctx)

		// Only the parent context ending means shutdown. A cycle preempted by
		// an incoming write also reports context.Canceled, and must not end
		// the loop.
		if ctx.Err() != nil {
			log.Info("Read loop stopped")
			return
		}
		wait := interval
		switch {
		case err == nil:
		case errors.Is(err, context.Canceled):
			log.Debug("Read cycle preempted by a write")
			wait = skipDelay
		case errors.Is(err, registers.ErrCycleSkipped):
			// Nothing was read, so waiting a whole interval would lose data
			// over what may be a momentary blip.
			wait = skipDelay
		default:
			log.Error("Read cycle failed: %v", err)
		}

		select {
		case <-ctx.Done():
			log.Info("Read loop stopped")
			return
		case <-time.After(wait):
		}
	}
}

// openSerialWithRetry opens the port, retrying with backoff until it succeeds
// or the context is cancelled.
func openSerialWithRetry(ctx context.Context, sp *serial.SerialPort, log *logger.Logger, backoff, maxBackoff time.Duration) error {
	attempt := 0
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("cancelled while opening port: %w", err)
		}

		err := sp.Open()
		if err == nil {
			return nil
		}

		attempt++
		log.Warn("Failed to open serial port (attempt %d), retrying in %v: %v", attempt, backoff, err)

		select {
		case <-ctx.Done():
			return fmt.Errorf("cancelled while opening port: %w", ctx.Err())
		case <-time.After(backoff):
		}

		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// connectMQTTWithRetry connects to the broker with exponential backoff and
// jitter. This retry loop is reachable because the client leaves paho's own
// connect-retry off; with it enabled, Connect never returns while the broker
// is down.
func connectMQTTWithRetry(ctx context.Context, client *mqtt.Client, log *logger.Logger, maxRetries int, initialDelayMs int, maxDelayMs int, jitterPercent int) error {
	var err error
	for attempt := range maxRetries {
		select {
		case <-ctx.Done():
			return fmt.Errorf("MQTT connection cancelled: %w", ctx.Err())
		default:
		}

		err = client.Connect()
		if err == nil {
			return nil
		}

		log.Warn("Failed to connect to MQTT (attempt %d/%d): %v", attempt+1, maxRetries, err)

		if attempt < maxRetries-1 {
			delay := calculateBackoffWithJitter(attempt, initialDelayMs, maxDelayMs, jitterPercent)
			log.Debug("MQTT connection retry delay: %v", delay)

			select {
			case <-ctx.Done():
				return fmt.Errorf("MQTT connection cancelled during retry: %w", ctx.Err())
			case <-time.After(delay):
			}
		}
	}
	return fmt.Errorf("failed to connect after %d attempts: %w", maxRetries, err)
}

// calculateBackoffWithJitter returns min(initial * 2^attempt, max) scaled by a
// random factor within +/- jitterPercent.
func calculateBackoffWithJitter(attempt int, initialDelayMs int, maxDelayMs int, jitterPercent int) time.Duration {
	delayMs := float64(initialDelayMs)
	for range attempt {
		delayMs *= 2
	}

	if float64(maxDelayMs) < delayMs {
		delayMs = float64(maxDelayMs)
	}

	// rand.Float64() yields [0,1); (2*x-1) maps that to [-1,1).
	if jitterPercent > 0 {
		delayMs *= 1.0 + (float64(jitterPercent)/100.0)*(2*rand.Float64()-1)
	}

	return time.Duration(int(delayMs)) * time.Millisecond
}

// msDuration converts a millisecond count from config into a Duration.
func msDuration(ms int) time.Duration { return time.Duration(ms) * time.Millisecond }

// publishHADiscovery publishes discovery payloads for every configured entity.
func publishHADiscovery(client *mqtt.Client, registersConfig *config.RegistersConfig, appConfig *config.AppConfig, log *logger.Logger) {
	di := appConfig.HADiscovery.DeviceInfo
	discovery := mqtt.Discovery{
		DeviceID:          appConfig.DeviceID,
		Prefix:            appConfig.HADiscovery.Prefix,
		Device:            mqtt.CreateDeviceInfo(appConfig.DeviceID, di.Name, di.Manufacturer, di.Model, di.SWVersion),
		AvailabilityTopic: appConfig.MQTT.AvailabilityTopic,
		ExpireAfter:       appConfig.HADiscovery.ExpireAfter,
	}

	published, failed := 0, 0
	publish := func(what, topic string, payload any) {
		if err := client.PublishDiscovery(topic, payload); err != nil {
			log.Warn("Failed to publish discovery for %s: %v", what, err)
			failed++
			return
		}
		published++
	}

	for _, reg := range registersConfig.ReadRegisters {
		if reg.HA.Name == "" {
			continue
		}
		topic, payload := discovery.SensorConfig(reg)
		publish(reg.Name, topic, payload)
	}

	for _, reg := range registersConfig.WriteRegisters {
		if reg.HA.Name == "" {
			continue
		}
		component, err := client.PublishCommandConfig(discovery, reg)
		if err != nil {
			log.Warn("Failed to publish discovery for %s: %v", reg.Name, err)
			failed++
			continue
		}
		log.Debug("Discovery: %s -> %s entity", reg.Name, component)
		published++
	}

	for _, reg := range registersConfig.DerivedRegisters {
		if reg.HA.Name == "" {
			continue
		}
		topic, payload := discovery.DerivedSensorConfig(reg)
		publish(reg.Name, topic, payload)
	}

	if failed > 0 {
		log.Warn("Home Assistant discovery: %d published, %d failed", published, failed)
		return
	}
	log.Info("Home Assistant discovery: %d entities published", published)
}
