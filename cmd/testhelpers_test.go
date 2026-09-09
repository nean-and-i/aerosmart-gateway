package main

import "github.com/nean/aerosmart-gateway/internal/logger"

// testMainLogger discards output: several tests exercise error paths.
func testMainLogger() *logger.Logger {
	return logger.NewWithConfig(logger.Config{Level: "error", ConsoleLogging: false})
}
