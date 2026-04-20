# How to Run

## Run the Published Image

Use this path when you want the default container image declared in `docker-compose.yml`.

**Prerequisites:** Docker and Docker Compose.
### 1. Create an Empty folder 
### 2. Create a docker-compose.yml file and copy the code in it. 
### 3. Clean any previous run

```bash
docker compose down -v
```

### 4. Start the pipeline

```bash
docker compose up
```

This will:

- start ZooKeeper and Kafka
- create topics `source`, `id`, `name`, and `continent`
- run the producer, consumer, and merger inside the `pipeline` container
- write logs to `logs/`

### 5. Wait for completion

When the pipeline finishes, check:

- `logs/producer.log`
- `logs/consumer.log`
- `logs/merger.log`
- `logs/overall_report.txt`

### 6. Verify output

```bash
./scripts/verify.sh
```

The script prints the first 10 records from:

- `id`
- `name`
- `continent`

Expected order:

- `id`: ascending numeric order by first column
- `name`: ascending alphabetic order by second column, then `id`
- `continent`: ascending alphabetic order by fourth column, then `id`

### 7. Stop the stack

```bash
docker compose down
```

## Build From Local Source

This repository is configured primarily for pull-and-run evaluation using the published image.

If you still want to test a local image before publishing it:

### 1. Build the image manually

```bash
docker build -t kafka-pipeline:local .
```

### 2. Run Compose with the local image

```bash
PIPELINE_IMAGE=kafka-pipeline:local docker compose up
```

On Windows PowerShell:

```powershell
$env:PIPELINE_IMAGE='kafka-pipeline:local'
docker compose up
```

## Local Host Run

If Kafka is already running and you want to execute the stages from the host:

```bash
./scripts/run_all.sh
```

If Kafka and ZooKeeper are already up, skip stack startup:

```bash
./scripts/run_all.sh --no-kafka-start
```

## Recommended Defaults

The repository is tuned to match the accepted reference structure and reduce end-to-end runtime:

- `SOURCE_TOPIC_PARTITIONS=6`
- `OUTPUT_TOPIC_PARTITIONS=1`
- `CONSUMER_BATCH_SIZE=1000000`
- `MERGER_BATCH_SIZE=50000`
- total Docker cap remains within 2GB RAM and 4 CPUs

## Notes

- temporary chunk files are created under `output/` during sorting and removed after a successful merge
- the final sorted results live in Kafka topics, not as final CSV files on disk
- for a clean rerun, always use `docker compose down -v` before starting again
