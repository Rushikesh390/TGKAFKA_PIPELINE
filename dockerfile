# Multi-stage build for optimized Docker image
# Stage 1: Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build binaries with optimization flags
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o producer ./cmd/producer/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o consumer ./cmd/consumer/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o merger ./cmd/consumer/merge_main.go

# Stage 2: Runtime stage  
FROM golang:1.22-alpine

WORKDIR /app

# Install runtime dependencies (bash for scripts)
RUN apk add --no-cache bash coreutils

# Copy binaries from builder
COPY --from=builder /app/producer /app/consumer /app/merger ./

# Copy scripts and source for reference
COPY scripts/ ./scripts/
COPY pkg/ ./pkg/
COPY internal/ ./internal/

# Create output directory with proper permissions
RUN mkdir -p /app/output && chmod 777 /app/output

# Set build metadata
LABEL maintainer="You" \
      description="Kafka Streaming Pipeline - 50M records sorting" \
      version="1.0" \
      optimization="Index-based sorting, parallel processing, 2GB RAM"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD test -f /app/producer || exit 1

# Memory and CPU constraints are specified at runtime
# Example: docker run --memory=2g --cpus=4 ...

# Default to showing help
CMD ["echo", "Kafka Pipeline Container. Run ./scripts/run_all.sh"]