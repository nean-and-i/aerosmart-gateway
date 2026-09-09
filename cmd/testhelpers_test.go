package main

import "github.com/nean/aerosmart-gateway/internal/logger"

func testMainLogger() *logger.Logger { return logger.New("error") }
