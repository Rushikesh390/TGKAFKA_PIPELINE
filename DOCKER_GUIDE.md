# Docker Build & Run Guide

## Quick Start with Docker

### Option 1: Docker Compose (Easiest)

```bash
# Start everything (Kafka + Zookeeper + Pipeline)
docker-compose up -d

# Run pipeline
docker-compose exec kafka-pipeline ./scripts/run_all.sh

# View logs
docker-compose logs -f
```

### Option 2: Build Your Own Image

```bash
# Build Docker image
docker build -t kafka-pipeline:latest .

# Run with memory/CPU constraints
docker run --memory=2g --cpus=4 \
  --network host \
  -v $(pwd)/output:/app/output \
  -e TOTAL_RECORDS=50000000 \
  kafka-pipeline:latest

# Check results
ls -lh output/
```

---

## Docker Image Details

### Dockerfile Explanation

```dockerfile
FROM golang:1.22
# ↑ Official Go image with latest compiler

WORKDIR /app
# ↑ Set working directory

COPY . .
# ↑ Copy all source code

RUN go mod tidy
# ↑ Download dependencies (kafka-go, etc)

# Build three binaries
RUN go build -o producer ./cmd/producer
RUN go build -o consumer ./cmd/consumer
RUN go build -o merger ./cmd/consumer/merge_main.go

# Create output directory
RUN mkdir -p /app/output

CMD ["bash"]
# ↑ Default command (interactive shell)
```

**Image size**: ~400-500 MB (large due to Go binaries)

### Optimization for Production

For a smaller production image, use multi-stage build:

```dockerfile
# Build stage
FROM golang:1.22 AS builder
WORKDIR /app
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 go build -o producer ./cmd/producer
RUN CGO_ENABLED=0 go build -o consumer ./cmd/consumer
RUN CGO_ENABLED=0 go build -o merger ./cmd/consumer/merge_main.go

# Runtime stage
FROM alpine:latest
RUN apk add --no-cache bash
COPY --from=builder /app/producer /app/consumer /app/merger /app/
COPY --from=builder /app/scripts /app/scripts/
RUN mkdir -p /app/output
WORKDIR /app
CMD ["bash"]
```

**Optimized size**: ~50-100 MB

---

## Docker Compose Configuration

### Recommended docker-compose.yml

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    deploy:
      resources:
        limits:
          memory: 512M
          cpus: '0.5'

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_LOG_RETENTION_HOURS: 1
      KAFKA_LOG_SEGMENT_BYTES: 1073741824
    deploy:
      resources:
        limits:
          memory: 1.3G
          cpus: '1.5'

  kafka-pipeline:
    build: .
    depends_on:
      - kafka
    network_mode: host
    volumes:
      - ./output:/app/output
      - ./scripts:/app/scripts
    environment:
      TOTAL_RECORDS: 50000000
      KAFKA_BROKER: localhost:9092
    command: bash /app/scripts/run_all.sh
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '4'
```

**Total resource allocation**:
- Zookeeper: 512MB + 0.5 cores
- Kafka: 1.3GB + 1.5 cores
- Pipeline: 2GB + 4 cores
- **Total: 3.8GB + 6 cores** (fits in 4GB machine with 8 cores)

---

## Building Docker Image

### Local Build

```bash
# Build image (first time takes 2-3 minutes)
docker build -t kafka-pipeline:v1.0 .

# Tag for DockerHub
docker tag kafka-pipeline:v1.0 YOUR_USERNAME/kafka-pipeline:v1.0

# List images
docker images | grep kafka-pipeline
```

### Build with BuildKit (Faster)

```bash
# Enable BuildKit
export DOCKER_BUILDKIT=1

# Build with progress output
docker build -t kafka-pipeline:v1.0 --progress=plain .
```

### Build Across Platforms

```bash
# Build for Linux/AMD64 and ARM64
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t your-username/kafka-pipeline:latest \
  --push .
```

---

## Running Docker Container

### Basic Run

```bash
docker run -it kafka-pipeline:v1.0
# Interactive shell in container
# Then manually run: go run cmd/producer/main.go
```

### Run Full Pipeline

```bash
docker run \
  --memory=2g \
  --cpus=4 \
  --name kafka-pipeline-run \
  -v $(pwd)/output:/app/output \
  kafka-pipeline:v1.0 \
  /app/scripts/run_all.sh
```

### Run Specific Components

```bash
# Only producer
docker run --memory=2g --cpus=4 kafka-pipeline:v1.0 /app/producer

# Only consumer
docker run --memory=2g --cpus=4 kafka-pipeline:v1.0 /app/consumer

# Only merger
docker run --memory=2g --cpus=4 kafka-pipeline:v1.0 /app/merger
```

### Debug Running Container

```bash
# Execute command in running container
docker exec -it kafka-pipeline-run bash

# Check processes
docker exec kafka-pipeline-run ps aux

# Check memory
docker exec kafka-pipeline-run free -m

# Check disk
docker exec kafka-pipeline-run df -h
```

---

## Docker Compose Workflow

### Start Services

```bash
# Start Zookeeper + Kafka (detached)
docker-compose up -d zookeeper kafka

# Wait for Kafka to be ready
sleep 30

# Verify Kafka is running
docker-compose logs kafka | grep "started"
```

### Run Pipeline

```bash
# Execute in container
docker-compose exec kafka-pipeline bash /app/scripts/run_all.sh

# Or run specific stage
docker-compose exec kafka-pipeline /app/producer
docker-compose exec kafka-pipeline /app/consumer
docker-compose exec kafka-pipeline /app/merger
```

### Monitor

```bash
# View all logs
docker-compose logs -f

# Follow specific service
docker-compose logs -f kafka

# Check resource usage
docker stats

# List containers
docker-compose ps
```

### Clean Up

```bash
# Stop services
docker-compose down

# Remove images
docker-compose down --rmi all

# Remove volumes
docker-compose down -v
```

---

## Publishing to DockerHub

### Prerequisites

```bash
# Create DockerHub account at https://hub.docker.com
# Create access token:
# 1. Account Settings → Security → New Access Token
# 2. Save token locally
```

### Publish Steps

```bash
# 1. Login to DockerHub
docker login
# Enter username and token when prompted

# 2. Build image
docker build -t YOUR_USERNAME/kafka-pipeline:latest .

# 3. Tag versions
docker tag YOUR_USERNAME/kafka-pipeline:latest \
  YOUR_USERNAME/kafka-pipeline:v1.0

# 4. Push to DockerHub
docker push YOUR_USERNAME/kafka-pipeline:latest
docker push YOUR_USERNAME/kafka-pipeline:v1.0

# 5. Verify on DockerHub
# Visit: https://hub.docker.com/r/YOUR_USERNAME/kafka-pipeline/
```

### DockerHub Repository Settings

**Recommended README for DockerHub** (add to repo description):

```markdown
# Kafka Streaming Pipeline

High-performance data sorting pipeline using Kafka for message streaming.

- 50 million records
- 3-way sorting (ID, Name, Continent)
- 2GB RAM, 4-core CPU
- 25-45 minute runtime

## Quick Start

\`\`\`bash
docker run -it YOUR_USERNAME/kafka-pipeline:latest
\`\`\`

See [README.md](https://github.com/YOUR_USERNAME/repo) for details.
```

---

## Environment Variables

All components support configuration via environment variables. If not set, sensible defaults are used.

### Pipeline Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `TOTAL_RECORDS` | 50000000 | Total records to generate and process |
| `NUM_WORKERS` | 4 | Number of producer worker threads |
| `BATCH_SIZE` | 10000 | Batch size for producer |

### Kafka Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | localhost:9092 | Kafka broker address (use `kafka:9092` in docker-compose) |
| `KAFKA_BATCH_SIZE` | 5000 | Records per Kafka write batch |
| `KAFKA_BATCH_TIMEOUT` | 50 | Kafka batch timeout in milliseconds |

### Consumer Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CONSUMER_NUM_WORKERS` | 4 | Number of consumer sort workers |
| `CONSUMER_BATCH_SIZE` | 1000000 | Consumer batch size (records per sort iteration) |
| `CONSUMER_MIN_BYTES` | 100000 | Minimum bytes to read from Kafka |
| `CONSUMER_MAX_BYTES` | 50000000 | Maximum bytes to read from Kafka |

### Merger Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MERGER_BATCH_SIZE` | 20000 | Records per Kafka write batch during merge (Phase 2 optimization) |

### Setting Environment Variables

**In shell:**
```bash
export TOTAL_RECORDS=50000000
export KAFKA_BROKER=kafka:9092
export MERGER_BATCH_SIZE=20000
./producer
```

**In docker-compose.yml:**
```yaml
services:
  pipeline:
    environment:
      TOTAL_RECORDS: 50000000
      KAFKA_BROKER: kafka:9092
      MERGER_BATCH_SIZE: 20000
```

**In docker run:**
```bash
docker run \
  -e TOTAL_RECORDS=50000000 \
  -e KAFKA_BROKER=kafka:9092 \
  -e MERGER_BATCH_SIZE=20000 \
  kafka-pipeline:latest
```

### Tuning Parameters

**For 100M records (double the default):**
```bash
export TOTAL_RECORDS=100000000
export CONSUMER_BATCH_SIZE=2000000
export MERGER_BATCH_SIZE=40000
```

**For limited resources (1GB RAM, 2 cores):**
```bash
export TOTAL_RECORDS=10000000
export NUM_WORKERS=2
export CONSUMER_BATCH_SIZE=500000
export MERGER_BATCH_SIZE=10000
```

**For high performance (8GB RAM, 8 cores):**
```bash
export TOTAL_RECORDS=100000000
export NUM_WORKERS=8
export CONSUMER_NUM_WORKERS=8
export CONSUMER_BATCH_SIZE=2000000
export MERGER_BATCH_SIZE=50000
export KAFKA_BATCH_SIZE=10000
```

---


## Performance Tuning in Docker

### Memory Limits

```bash
# Set strict memory limit
docker run --memory=2g \
  --memory-swap=2g \  # Disable swap
  --memory-reservation=1.8g \  # Reserve minimum
  kafka-pipeline:v1.0
```

### CPU Limits

```bash
# Limit to 4 CPU cores
docker run --cpus=4 \
  --cpuset-cpus="0-3" \  # Use cores 0,1,2,3
  kafka-pipeline:v1.0
```

### Network Optimization

```bash
# Use host network (better performance)
docker run --network host kafka-pipeline:v1.0

# Or specific bridge with custom settings
docker network create --driver bridge \
  --opt "com.docker.network.bridge.name"="br0" \
  kafka-net
```

### Storage Optimization

```bash
# Use tmpfs for temporary files (faster than disk)
docker run --tmpfs /tmp:size=1g \
  kafka-pipeline:v1.0

# Bind mount output directory
docker run -v /fast-ssd/output:/app/output \
  kafka-pipeline:v1.0
```

---

## Troubleshooting Docker Issues

### Issue: Container crashes immediately

```bash
# Check logs
docker logs kafka-pipeline-run

# Run with interactive shell
docker run -it kafka-pipeline:v1.0 bash

# Test Kafka connection
docker run --network host kafka-pipeline:v1.0 \
  go run cmd/test/main.go
```

### Issue: Out of memory

```bash
# Check memory limits
docker inspect kafka-pipeline-run | grep -i memory

# Increase limit
docker run --memory=3g kafka-pipeline:v1.0

# Or reduce batch sizes in code (recompile Docker image)
```

### Issue: Slow performance

```bash
# Check CPU usage
docker stats kafka-pipeline-run
# Should show ~99% CPU

# Check I/O
docker logs kafka-pipeline-run | grep "throughput"

# Check if Kafka is I/O bound
docker exec kafka-pipeline-run iostat -x 1 5
```

### Issue: Network connectivity

```bash
# Test Kafka from container
docker run --network host kafka-pipeline:v1.0 \
  kafka-console-producer --bootstrap-server localhost:9092

# Check Kafka broker
docker logs kafka | grep "started"

# Inspect network
docker network inspect bridge
```

---

## Docker Best Practices

### 1. Use `.dockerignore`

```
# .dockerignore
output/
*.log
.git/
.github/
node_modules/
*.test.go
```

### 2. Use Non-Root User

```dockerfile
RUN useradd -m app
USER app
```

### 3. Health Checks

```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD go run cmd/test/main.go || exit 1
```

### 4. Multi-Stage Builds

Reduces image size significantly (shown above).

### 5. Pin Base Image Version

```dockerfile
FROM golang:1.22.0  # Use specific version, not 'latest'
```

---

## Kubernetes Deployment (Optional)

### Deployment Manifest

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-pipeline
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka-pipeline
  template:
    metadata:
      labels:
        app: kafka-pipeline
    spec:
      containers:
      - name: kafka-pipeline
        image: your-username/kafka-pipeline:latest
        resources:
          requests:
            memory: "2Gi"
            cpu: "4"
          limits:
            memory: "2Gi"
            cpu: "4"
        volumeMounts:
        - name: output
          mountPath: /app/output
      volumes:
      - name: output
        persistentVolumeClaim:
          claimName: kafka-output-pvc
```

---

## See Also

- [README.md](README.md) - Quick start guide
- [ARCHITECTURE.md](ARCHITECTURE.md) - System design
- [DOCKER_GUIDE.md](DOCKER_GUIDE.md) - This file
- [Dockerfile](dockerfile) - Container definition
- [docker-compose.yml](docker-compose.yml) - Multi-container setup