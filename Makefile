# Makefile for Aerosmart Gateway
# Provides common development commands

# Binary name
BINARY_NAME=aerosmart-gateway
MAIN_PATH=./cmd/main.go

# Go build variables
GO=go
GOFLAGS=-v
LDFLAGS=-ldflags "-s -w"

# Colors for output
RED=\033[0;31m
GREEN=\033[0;32m
YELLOW=\033[0;33m
NC=\033[0m # No Color

.PHONY: help build test lint clean docker-build docker-run install check-config

# Default target
help:
	@echo "Aerosmart Gateway - Makefile Commands"
	@echo ""
	@echo "Available targets:"
	@echo "  build           Build the application"
	@echo "  test            Run tests"
	@echo "  lint            Run linter (golangci-lint)"
	@echo "  clean           Clean build artifacts"
	@echo "  docker-build    Build Docker image"
	@echo "  docker-run      Run Docker container"
	@echo "  install         Install dependencies"
	@echo "  check-config    Validate configuration files"
	@echo ""

# Build the application
build:
	@echo "$(GREEN)Building $(BINARY_NAME)...$(NC)"
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BINARY_NAME) $(MAIN_PATH)
	@echo "$(GREEN)Build complete: $(BINARY_NAME)$(NC)"

# Build for different platforms
build-linux-arm:
	@echo "$(GREEN)Building for Linux ARM...$(NC)"
	GOOS=linux GOARCH=arm GOARM=6 $(GO) build $(GOFLAGS) -o $(BINARY_NAME)-linux-armv6 $(MAIN_PATH)

build-linux-armv7:
	@echo "$(GREEN)Building for Linux ARMv7...$(NC)"
	GOOS=linux GOARCH=arm GOARM=7 $(GO) build $(GOFLAGS) -o $(BINARY_NAME)-linux-armv7 $(MAIN_PATH)

build-linux-arm64:
	@echo "$(GREEN)Building for Linux ARM64...$(NC)"
	GOOS=linux GOARCH=arm64 $(GO) build $(GOFLAGS) -o $(BINARY_NAME)-linux-arm64 $(MAIN_PATH)

build-all: build build-linux-arm build-linux-arm64
	@echo "$(GREEN)All builds complete$(NC)"

# Run tests
test:
	@echo "$(GREEN)Running tests...$(NC)"
	$(GO) test -v -race -coverprofile=coverage.txt -covermode=atomic ./...
	@echo "$(GREEN)Tests complete. Coverage report: coverage.txt$(NC)"

# Run tests with verbose output
test-verbose:
	@echo "$(GREEN)Running tests (verbose)...$(NC)"
	$(GO) test -v ./...

# Run linter
lint:
	@echo "$(YELLOW)Running linter...$(NC)"
	golangci-lint run --timeout=5m
	@echo "$(GREEN)Linting complete$(NC)"

# Run linter with auto-fix
lint-fix:
	@echo "$(YELLOW)Running linter with auto-fix...$(NC)"
	golangci-lint run --fix --timeout=5m
	@echo "$(GREEN)Linting complete$(NC)"

# Clean build artifacts
clean:
	@echo "$(YELLOW)Cleaning build artifacts...$(NC)"
	rm -f $(BINARY_NAME)
	rm -f $(BINARY_NAME)-*
	rm -f coverage.txt coverage.html
	rm -rf /tmp/aerosmart-*
	@echo "$(GREEN)Clean complete$(NC)"

# Install dependencies
install:
	@echo "$(GREEN)Installing dependencies...$(NC)"
	$(GO) mod download
	$(GO) mod tidy
	@echo "$(GREEN)Dependencies installed$(NC)"

# Build Docker image
docker-build:
	@echo "$(GREEN)Building Docker image...$(NC)"
	docker build -t $(BINARY_NAME):latest .
	@echo "$(GREEN)Docker image built: $(BINARY_NAME):latest$(NC)"

# Run Docker container
docker-run:
	@echo "$(GREEN)Running Docker container...$(NC)"
	docker run -d \
		--name $(BINARY_NAME) \
		--device /dev/ttyUSB0:/dev/ttyUSB0 \
		-v $(shell pwd)/config.yaml:/app/config.yaml:ro \
		-v $(shell pwd)/registers.yaml:/app/registers.yaml:ro \
		$(BINARY_NAME):latest

# Stop Docker container
docker-stop:
	@echo "$(YELLOW)Stopping Docker container...$(NC)"
	docker stop $(BINARY_NAME) || true
	docker rm $(BINARY_NAME) || true

# Validate configuration files
check-config:
	@echo "$(GREEN)Validating configuration files...$(NC)"
	@if [ -f config.yaml ]; then \
		echo "config.yaml exists"; \
	else \
		echo "$(RED)config.yaml not found$(NC)"; \
		exit 1; \
	fi
	@if [ -f registers.yaml ]; then \
		echo "registers.yaml exists"; \
	else \
		echo "$(RED)registers.yaml not found$(NC)"; \
		exit 1; \
	fi
	@echo "$(GREEN)Configuration files valid$(NC)"

# Run the application (requires config.yaml and registers.yaml)
run: check-config
	@echo "$(GREEN)Running $(BINARY_NAME)...$(NC)"
	./$(BINARY_NAME) -config config.yaml -registers registers.yaml

# Format code
fmt:
	@echo "$(GREEN)Formatting code...$(NC)"
	$(GO) fmt ./...
	$(GO) vet ./...
	@echo "$(GREEN)Formatting complete$(NC)"

# Show version
version:
	@$(GO) run $(MAIN_PATH) -version
