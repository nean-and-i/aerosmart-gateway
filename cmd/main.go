package main

import (
	"context"
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
	version       = "1.0.0"
)

func init() {
	flag.StringVar(&configPath, "config", "config.yaml", "Path to config file")
	flag.StringVar(&registersPath, "registers", "registers.yaml", "Path to registers file")
	flag.BoolVar(&showVersion, "version", false, "Show version information")
}

func main() {
	flag.Parse()

	if showVersion {
		fmt.Printf("aerosmart-gateway version %s\n", version)
		os.Exit(0)
	}

	// Load configuration
	appConfig, err := config.LoadAppConfig(configPath)
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	// Set sw_version from build version if not configured
	if appConfig.HADiscovery.DeviceInfo.SWVersion == "" {
		appConfig.HADiscovery.DeviceInfo.SWVersion = version
	}

	// Load registers configuration
	registersConfig, err := config.LoadRegistersConfig(registersPath)
	if err != nil {
		fmt.Printf("Error loading registers config: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger with configuration
	logConfig := logger.Config{
		Level:          appConfig.LogLevel,
		LogFile:        appConfig.Logging.LogFile,
		FileLogging:    appConfig.Logging.FileLogging,
		ConsoleLogging: appConfig.Logging.ConsoleLogging,
	}
	log := logger.NewWithConfig(logConfig)
	log.Info("Starting Aerosmart Gateway")

	// Log logging configuration
	if appConfig.Logging.FileLogging && appConfig.Logging.LogFile != "" {
		log.Info("File logging enabled: %s", appConfig.Logging.LogFile)
	} else {
		log.Info("File logging disabled")
	}
	if appConfig.Logging.ConsoleLogging {
		log.Info("Console logging enabled")
	} else {
		log.Info("Console logging disabled")
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channel to signal force quit
	forceQuit := make(chan struct{})

	// Handle signals for graceful then force shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.SignalReceived(sig)

		// Cancel context for graceful shutdown
		cancel()

		// Wait for graceful shutdown with timeout
		select {
		case <-forceQuit:
			// Already shut down gracefully
			return
		case <-time.After(5 * time.Second):
			// Force shutdown after timeout
			log.Warn("Graceful shutdown timed out, forcing exit...")
			os.Exit(1)
		}
	}()

	// Initialize serial port
	serialPort := serial.NewSerialPort(
		appConfig.Serial.Port,
		appConfig.Serial.BaudRate,
		appConfig.Serial.Timeout,
		appConfig.Serial.WriteTimeout,
		appConfig.Serial.XonXoff,
		appConfig.Serial.ReadTimeout,
		appConfig.Serial.MaxReopens,
		appConfig.Serial.DeadlineTimeout,
		appConfig.Serial.WriteWithRetryDelay,
		appConfig.Serial.DeviceResponseDelay,
		appConfig.Serial.ReadWithRetryDelay,
		appConfig.Serial.ReadMaxRetries,
		appConfig.Serial.WriteMaxRetries,
		appConfig.Serial.MaxRetries,
	)

	// Connect to serial device with retry
	log.Info("Connecting to serial device %s...", appConfig.Serial.Port)
	if err := connectSerialWithRetry(ctx, serialPort, log, 10, appConfig.Serial.ConnectRetryInitialDelayMs, appConfig.Serial.ConnectRetryMaxDelayMs, appConfig.Serial.ConnectRetryJitterPercent); err != nil {
		log.Error("Failed to connect to serial device: %v", err)
		os.Exit(1)
	}
	log.Info("Serial device connected")

	// Initialize MQTT client
	mqttConfig := &mqtt.MQTTConfig{
		Broker:            appConfig.MQTT.Broker,
		Port:              appConfig.MQTT.Port,
		Username:          appConfig.MQTT.Username,
		Password:          appConfig.MQTT.Password,
		ClientID:          appConfig.MQTT.ClientID,
		QOS:               appConfig.MQTT.QOS,
		Retain:            appConfig.MQTT.Retain,
		PublishRetryCount: appConfig.MQTT.PublishRetryCount,
	}

	mqttClient := mqtt.NewClient(mqttConfig, appConfig.DeviceID, appConfig.HADiscovery.Prefix)

	// Connect to MQTT broker with retry
	log.Info("Connecting to MQTT broker %s:%d...", appConfig.MQTT.Broker, appConfig.MQTT.Port)
	if err := connectMQTTWithRetry(ctx, mqttClient, log, 10, appConfig.MQTT.ConnectRetryInitialDelayMs, appConfig.MQTT.ConnectRetryMaxDelayMs, appConfig.MQTT.ConnectRetryJitterPercent); err != nil {
		log.Error("Failed to connect to MQTT broker: %v", err)
		_ = serialPort.Close()
		os.Exit(1)
	}
	log.Info("MQTT broker connected")

	// Publish Home Assistant discovery configs
	if appConfig.HADiscovery.Enabled {
		log.Info("Publishing Home Assistant discovery configs...")
		publishHADiscovery(mqttClient, registersConfig, appConfig, log)
		log.Info("Home Assistant discovery configs published")
	}

	// Initialize register reader
	reader := registers.NewReader(serialPort, mqttClient, log, registersConfig.ReadRegisters)

	// Initialize register writer
	writer := registers.NewWriter(serialPort, mqttClient, log, registersConfig.WriteRegisters)

	// Connect writer to reader for verification after fan control
	writer.SetReader(reader)

	// Initialize derived register calculator and connect it to the writer so
	// derived values are computed and published after each full readout
	derivedCalc := registers.NewDerivedCalculator(mqttClient, log, registersConfig.DerivedRegisters)
	writer.SetDerivedCalculator(derivedCalc)

	// Subscribe to write register topics
	writeTopics := writer.GetSubscribeTopics()
	if len(writeTopics) > 0 {
		log.Info("Subscribing to write register topics: %v", writeTopics)
		for _, topic := range writeTopics {
			handler := func(t string, msg string) {
				log.Debug("Received MQTT message on %s: %s", t, msg)
				if err := writer.HandleMessage(t, msg); err != nil {
					log.Error("Error handling write register message: %v", err)
				}
			}
			if err := mqttClient.Subscribe(topic, handler); err != nil {
				log.Error("Failed to subscribe to %s: %v", topic, err)
			}
		}
	}

	// Main loop - Writer triggers periodic reads
	log.Info("Starting periodic read loop (interval: %d seconds)", appConfig.ReadInterval)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Duration(appConfig.ReadInterval) * time.Second)
		defer ticker.Stop()

		// Initial read using Writer
		_ = writer.TriggerFullReadout()

		for {
			select {
			case <-ctx.Done():
				// Signal graceful shutdown completion
				close(forceQuit)
				log.Info("Main loop stopped")
				return
			case <-ticker.C:
				// Trigger full readout via Writer
				if err := writer.TriggerFullReadout(); err != nil {
					log.Error("Periodic readout failed: %v", err)
				}
			}
		}
	}()

	// Wait for shutdown signal
	<-ctx.Done()

	// Wait for the read-loop goroutine to finish its in-flight operation before
	// closing the serial port, so we don't tear down resources mid-read.
	// The signal handler's 5s timeout bounds this wait as a backstop.
	log.Info("Shutdown initiated, waiting for read loop to finish...")
	wg.Wait()

	// Disconnect MQTT first
	mqttClient.Disconnect()

	// Close serial port
	if err := serialPort.Close(); err != nil {
		log.Error("Error closing serial port: %v", err)
	}

	log.Info("Aerosmart Gateway stopped")
}

// connectSerialWithRetry connects to serial port with exponential backoff, jitter, and context support
func connectSerialWithRetry(ctx context.Context, sp *serial.SerialPort, log *logger.Logger, maxRetries int, initialDelayMs int, maxDelayMs int, jitterPercent int) error {
	var err error
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return fmt.Errorf("serial connection cancelled: %w", ctx.Err())
		default:
		}

		err = sp.Open()
		if err == nil {
			return nil
		}

		log.Warn("Failed to open serial port (attempt %d/%d): %v", attempt+1, maxRetries, err)

		// Calculate delay with exponential backoff and jitter
		if attempt < maxRetries-1 {
			delay := calculateBackoffWithJitter(attempt, initialDelayMs, maxDelayMs, jitterPercent)
			log.Debug("Serial connection retry delay: %v", delay)

			select {
			case <-ctx.Done():
				return fmt.Errorf("serial connection cancelled during retry: %w", ctx.Err())
			case <-time.After(delay):
			}
		}
	}
	return fmt.Errorf("failed to connect after %d attempts: %w", maxRetries, err)
}

// connectMQTTWithRetry connects to MQTT broker with exponential backoff, jitter, and context support
func connectMQTTWithRetry(ctx context.Context, client *mqtt.Client, log *logger.Logger, maxRetries int, initialDelayMs int, maxDelayMs int, jitterPercent int) error {
	var err error
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Check for context cancellation
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

		// Calculate delay with exponential backoff and jitter
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

// calculateBackoffWithJitter calculates delay with exponential backoff and jitter
// Formula: delay = min(initialDelay * 2^attempt, maxDelay) * (1 + random(-jitter, +jitter))
func calculateBackoffWithJitter(attempt int, initialDelayMs int, maxDelayMs int, jitterPercent int) time.Duration {
	// Calculate exponential backoff: initialDelay * 2^attempt
	delayMs := float64(initialDelayMs)
	for i := 0; i < attempt; i++ {
		delayMs *= 2
	}

	// Cap at max delay
	if float64(maxDelayMs) < delayMs {
		delayMs = float64(maxDelayMs)
	}

	// Add jitter: random value between -jitterPercent% and +jitterPercent%
	// rand.Float64() yields [0,1); (2*x-1) maps that to [-1,1).
	if jitterPercent > 0 {
		jitterMultiplier := 1.0 + (float64(jitterPercent)/100.0)*(2*rand.Float64()-1)
		delayMs *= jitterMultiplier
	}

	return time.Duration(int(delayMs)) * time.Millisecond
}

func publishHADiscovery(mqttClient *mqtt.Client, registersConfig *config.RegistersConfig, appConfig *config.AppConfig, log *logger.Logger) {
	di := appConfig.HADiscovery.DeviceInfo
	deviceID := appConfig.DeviceID
	deviceInfo := mqtt.CreateDeviceInfo(deviceID, di.Name, di.Manufacturer, di.Model, di.SWVersion)

	// Publish sensor discovery configs
	for _, reg := range registersConfig.ReadRegisters {
		if reg.HA.Name == "" {
			continue
		}
		sensorConfig := mqtt.HASensorConfig{
			Name:        reg.HA.Name,
			StateTopic:  reg.Topic,
			Unit:        reg.HA.Unit,
			DeviceClass: reg.HA.DeviceClass,
			UniqueID:    fmt.Sprintf("%s_%s", deviceID, reg.Name),
			Device:      deviceInfo,
		}
		if err := mqttClient.PublishSensorDiscovery(&sensorConfig); err != nil {
			log.Warn("Failed to publish sensor discovery for %s: %v", reg.Name, err)
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Publish switch discovery configs
	for _, reg := range registersConfig.WriteRegisters {
		if reg.HA.Name == "" {
			continue
		}
		switchConfig := mqtt.HASwitchConfig{
			Name:         reg.HA.Name,
			CommandTopic: reg.HA.CommandTopic,
			StateTopic:   reg.Topic,
			UniqueID:     fmt.Sprintf("%s_%s", deviceID, reg.Name),
			Device:       deviceInfo,
		}
		if err := mqttClient.PublishSwitchDiscovery(&switchConfig); err != nil {
			log.Warn("Failed to publish switch discovery for %s: %v", reg.Name, err)
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Publish derived sensor discovery configs
	for _, reg := range registersConfig.DerivedRegisters {
		if reg.HA.Name == "" {
			continue
		}
		sensorConfig := mqtt.HASensorConfig{
			Name:        reg.HA.Name,
			StateTopic:  reg.Topic,
			Unit:        reg.HA.Unit,
			DeviceClass: reg.HA.DeviceClass,
			UniqueID:    fmt.Sprintf("%s_%s", deviceID, reg.Name),
			Device:      deviceInfo,
		}
		if err := mqttClient.PublishSensorDiscovery(&sensorConfig); err != nil {
			log.Warn("Failed to publish sensor discovery for %s: %v", reg.Name, err)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
