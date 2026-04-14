# Docker Image Setup & Quick Start Guide

**Image:** `docrushi/kafka-pipeline:v1`

This guide helps you run the Kafka Pipeline from DockerHub without needing source code.

---

## Prerequisites

1. **Docker** installed (any recent version)
2. **Docker Compose** installed (for Kafka orchestration)
3. **~30GB disk space** for intermediate files
4. **4 CPU cores** and **2GB RAM** available

---

## Quick Start (3 Steps)

### Step 1: Pull the Image

```bash
docker pull docrushi/kafka-pipeline:v1
```

**Image Size:** ~80MB (optimized multi-stage build)

### Step 2: Create docker-compose.yml

Create a `docker-compose.yml` in your working directory:

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"
    deploy:
      resources:
        limits:
          memory: 256M
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
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    deploy:
      resources:
        limits:
          memory: 1G
          cpus: '2'
```

### Step 3: Run the Pipeline

```bash
# Start Kafka infrastructure
docker-compose up -d

# Wait for Kafka to be ready
sleep 30

# Run the full pipeline
docker run --rm \
  --network host \
  -e KAFKA_BROKER=localhost:9092 \
  docrushi/kafka-pipeline:v1

# Check status
docker-compose logs -f kafka
```

**Expected Output:**
```
=== Kafka Streaming Pipeline - Full Run ===
=== Checking Kafka connectivity ===
Using Kafka broker: kafka:9092
=== Starting Producer (generates 50M records) ===
...
=== Pipeline Execution Complete! ===
```

**Expected Runtime:** 35-40 minutes total

---

## Run Individual Components

### Run Only Producer

```bash
docker run --rm \
  --network host \
  -e KAFKA_BROKER=localhost:9092 \
  docrushi/kafka-pipeline:v1 \
  /app/producer
```

**Expected Time:** 2-3 minutes  
**Output:** 50M records in Kafka topic `source`

### Run Only Consumer

```bash
docker run --rm \
  --network host \
  -e KAFKA_BROKER=localhost:9092 \
  docrushi/kafka-pipeline:v1 \
  /app/consumer
```

**Expected Time:** 10-15 minutes  
**Output:** Chunk files in container (sorts 50M records)

### Run Only Merger

```bash
docker run --rm \
  --network host \
  -e KAFKA_BROKER=localhost:9092 \
  docrushi/kafka-pipeline:v1 \
  /app/merger
```

**Expected Time:** 25 minutes  
**Output:** Final sorted data in Kafka topics

---

## Environment Variables

Control pipeline behavior with environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | localhost:9092 | Kafka broker address |
| `TOTAL_RECORDS` | 50000000 | Number of records to generate |
| `NUM_WORKERS` | 4 | Producer worker threads |
| `CONSUMER_BATCH_SIZE` | 1000000 | Consumer batch size (records) |
| `MERGER_BATCH_SIZE` | 20000 | Merger batch size (Kafka writes) |

### Example: Generate 10M Records Instead of 50M

```bash
docker run --rm \
  --network host \
  -e KAFKA_BROKER=localhost:9092 \
  -e TOTAL_RECORDS=10000000 \
  docrushi/kafka-pipeline:v1
```

**Expected Runtime:** ~7-8 minutes

---

## Configuration for Different Scenarios

### Scenario 1: Limited Resources (1GB RAM, 2 cores)

```bash
docker run --rm \
  --memory=1g \
  --cpus=2 \
  --network host \
  -e KAFKA_BROKER=localhost:9092 \
  -e TOTAL_RECORDS=10000000 \
  -e CONSUMER_BATCH_SIZE=500000 \
  docrushi/kafka-pipeline:v1
```

### Scenario 2: High Performance (8GB RAM, 8 cores)

```bash
docker run --rm \
  --memory=8g \
  --cpus=8 \
  --network host \
  -e KAFKA_BROKER=localhost:9092 \
  -e TOTAL_RECORDS=100000000 \
  -e NUM_WORKERS=8 \
  docrushi/kafka-pipeline:v1
```

### Scenario 3: Verification Mode (Small test run)

```bash
# Run with 1M records to verify setup works
docker run --rm \
  --network host \
  -e KAFKA_BROKER=localhost:9092 \
  -e TOTAL_RECORDS=1000000 \
  docrushi/kafka-pipeline:v1
```

**Expected Runtime:** ~2-3 minutes (fast verification)

---

## Verification

### Check Kafka Topics

```bash
# List all topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Expected output:
# source
# id-sorted
# name-sorted
# continent-sorted
```

### Count Records in Topic

```bash
# Count records in id-sorted topic
docker exec kafka kafka-run-class kafka.tools.JmxTool \
  --bootstrap-server localhost:9092 \
  --object-name kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec \
  --attributes Count
```

Or manually verify:

```bash
# Read first 3 records from id-sorted
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic id-sorted \
  --from-beginning \
  --max-messages 3

# Should show:
# 1,KlMnOpQrSt,123 XyZ street,North America
# 2,ABCdEfGhIj,456 AbC avenue,Europe
# 3,UvWxYzAbCd,789 DeF road,Asia
```

---

## Troubleshooting

### Issue: Container can't connect to Kafka

**Solution:** Use `--network host` or ensure Kafka is accessible

```bash
# Test connectivity
docker run --rm --network host alpine:latest \
  nc -zv localhost:9092
```

### Issue: Container runs out of memory

**Solution:** Reduce batch size or record count

```bash
docker run --rm \
  --memory=1g \
  --network host \
  -e KAFKA_BROKER=localhost:9092 \
  -e TOTAL_RECORDS=10000000 \
  -e CONSUMER_BATCH_SIZE=500000 \
  docrushi/kafka-pipeline:v1
```

### Issue: Kafka topic not created automatically

**Solution:** Create manually before running

```bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic source \
  --partitions 1 \
  --replication-factor 1
```

### Issue: "No such file or directory" errors

**Solution:** Ensure docker-compose.yml is in working directory

```bash
pwd  # Check current directory
ls docker-compose.yml  # Verify file exists
```

---

## Performance Notes

### Typical Execution Timeline (50M records)

```
Time    Event
─────────────────────────────────────────
0:00    Start
2-3min  Producer finishes (373K rec/sec)
        Kafka receives 50M records
10-15min Consumer finishes (sorts by 3D)
        50 chunk files created per dimension
25-30min Merger finishes (k-way merge)
        Final data in 3 Kafka topics
─────────────────────────────────────────
35-40min Total wall-clock time
```

### Resource Usage During Execution

- **Memory:** 512MB per container (if limited to 2GB as per requirements)
- **CPU:** All 4 cores utilized at 70-80%
- **Disk:** ~50GB for intermediate chunk files
- **Network:** Stream reads/writes via Kafka

---

## Cleanup

After testing:

```bash
# Stop Kafka infrastructure
docker-compose down

# Remove image
docker rmi docrushi/kafka-pipeline:v1

# Remove volumes
docker-compose down -v
```

---

## Architecture Details

For detailed architecture, algorithms, and optimization explanations, see:
- [ARCHITECTURE.md](ARCHITECTURE.md) - Complete system design
- [README.md](README.md) - General overview

---

## Support

For issues or questions:
1. Check logs: `docker logs <container_id>`
2. Verify Kafka: `docker-compose logs kafka`
3. Review component documentation in source code
4. Check memory limits: `docker stats`

