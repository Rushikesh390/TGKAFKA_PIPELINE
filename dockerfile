# Multi-stage build for optimized Docker image
# Stage 1: Build stage with Go compiler
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Tidy after copying source (in case new dependencies are found)
RUN go mod tidy

# Build binaries with optimization flags (strip debug symbols to reduce size)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o producer ./cmd/producer/main.go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o consumer ./cmd/consumer/main.go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o merger ./cmd/consumer/merge_main.go

# Stage 2: Runtime stage - Alpine only (7MB base vs 210MB golang image)
FROM alpine:latest

WORKDIR /app

# Install only runtime dependencies (bash for scripts)
# Keep dependencies minimal for smaller image
RUN apk add --no-cache bash ca-certificates

# Copy binaries from builder (only what we need)
COPY --from=builder /app/producer /app/consumer /app/merger ./

# Copy scripts for automation
COPY scripts/ ./scripts/

# Create output directory with proper permissions
RUN mkdir -p /app/output && chmod 777 /app/output

# Set build metadata
LABEL maintainer="Kafka Pipeline" \
      description="High-performance Kafka streaming pipeline - 50M records, 3-way sorting" \
      version="1.0" \
      optimization="Index-based sorting, parallel merges, batch processing, 2GB RAM limit"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD test -f /app/producer || exit 1

# Default entrypoint
ENTRYPOINT ["/bin/bash"]
CMD ["/app/scripts/docker-entrypoint.sh"]
