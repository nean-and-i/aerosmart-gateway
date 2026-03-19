FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the application
COPY . .

# Build the application
RUN go build -o aerosmart-gateway ./cmd/main.go

# Final stage - minimal runtime image
FROM alpine:latest

# Install ca-certificates for HTTPS connections
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/aerosmart-gateway .

# Copy configuration files
COPY config.yaml .
COPY registers.yaml .

# Create non-root user for security
RUN adduser -D -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose MQTT port (1883) and any other ports if needed
EXPOSE 1883

# Run the application
CMD ["./aerosmart-gateway"]
