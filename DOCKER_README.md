# Docker Quick Start

## 1. Build

```bash
docker build -t kafka-pipeline:latest .
```

## 2. Start Everything

```bash
docker compose up --abort-on-container-exit --exit-code-from pipeline
```

This single command starts ZooKeeper, Kafka, and the pipeline image.

## 3. Inspect Results

Chunk files:

```bash
ls output/
```

Kafka sample:

```bash
./scripts/verify.sh
```

## 4. Stop Everything

```bash
docker compose down -v
```
