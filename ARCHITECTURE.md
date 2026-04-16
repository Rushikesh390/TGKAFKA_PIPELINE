# Architecture

## Pipeline Overview

The system is a three-stage external sorting pipeline built around Kafka.

```text
Producer
  -> topic: source
Consumer
  -> output/id_chunk_*.csv
  -> output/name_chunk_*.csv
  -> output/continent_chunk_*.csv
Merger
  -> topic: id
  -> topic: name
  -> topic: continent
```

## Stage 1: Producer

Entrypoint: `cmd/producer`

Responsibilities:

- split the total record count across producer workers
- generate records that match the assignment schema
- batch Kafka writes to the `source` topic

Implementation notes:

- each worker uses its own `math/rand.Rand` instance to avoid global RNG lock contention
- IDs are assigned from disjoint ranges so every generated record is unique
- producer write errors cancel the run instead of being logged and ignored

## Stage 2: Consumer

Entrypoint: `cmd/consumer`

Responsibilities:

- read all partitions of `source`
- accumulate a bounded batch in memory
- sort that batch three ways
- write sorted chunk files to disk

### Why external sorting

Sorting all 50 million records in memory is not realistic under the assignment limits, so the dataset is processed in bounded batches and merged later.

### Batch flow

For each batch:

1. read up to `CONSUMER_BATCH_SIZE` records from Kafka
2. sort indices by `id`
3. sort indices by `name`
4. sort indices by `continent`
5. write three chunk files using the sorted indices

### Indexed sorting

The consumer does not create three copied `[]Record` slices for the three sort orders.
Instead, it sorts `[]int` index arrays against the original record slice:

```go
sort.Slice(indices, func(i, j int) bool {
    return records[indices[i]].ID < records[indices[j]].ID
})
```

This keeps the main memory cost to:

- the original batch
- three index arrays
- small writer buffers

### Memory control

The current defaults are intentionally conservative:

- `CONSUMER_BATCH_SIZE=200000`
- `CONSUMER_NUM_WORKERS=1`
- channel buffer size `1`

That means the consumer keeps at most one queued batch plus one active batch in play, rather than several copied million-record batches.

## Stage 3: Merger

Entrypoint: `cmd/merger`

Responsibilities:

- open all chunk files for a sort dimension
- perform k-way merge using a min-heap
- stream merged records to Kafka

### Heap strategy

Each heap item stores:

- the parsed `Record`
- the original CSV line
- the source file id

The comparator now uses parsed record fields directly:

- `Record.ID` for `id`
- `Record.Name` for `name`
- `Record.Continent` for `continent`

The merger writes the cached CSV line back to Kafka, which avoids:

- reparsing during every comparison
- reformatting every record during Kafka writes

## Kafka and Topic Model

Topics:

- `source`
- `id`
- `name`
- `continent`

Compose exposes Kafka in two ways:

- `localhost:9092` for host tools
- `kafka:29092` for sibling containers

This avoids the earlier mismatch where the broker advertised `localhost` even for container-to-container traffic.

## Rerun Strategy

Clean reruns are important for this assignment because old records in Kafka would corrupt count verification.

Current behavior:

- `scripts/start.sh` resets the Compose stack with `down -v`
- topics are recreated explicitly
- the consumer uses a run-unique group id by default

That combination ensures a fresh read path when you use the supported script flow.

## Bottlenecks

The main bottlenecks are:

1. Kafka I/O  
   Producer and merger throughput are constrained by broker writes and batching behavior.

2. Sort CPU  
   The consumer does three comparison-based sorts per batch.

3. Disk I/O  
   The consumer writes many chunk files and the merger rereads them.

4. Merge comparisons  
   K-way merge is efficient at `O(n log k)`, but still executes a huge number of comparisons across 50 million records.

## Optimizations Applied

- indexed sorting to avoid three full data copies
- bounded in-flight consumer batches
- per-worker RNG instances
- cached CSV lines in the merger
- direct record-field comparisons in heap ordering
- shared config and exact topic naming across code and scripts
- runtime reporting in `scripts/unified_run.sh`

## If We Had More Data or More Machines

For larger datasets:

- increase Kafka partitions and parallelize producer/consumer groups accordingly
- shard chunk output per partition or worker
- run merge stages on separate workers per dimension
- move intermediate chunk storage to fast local SSD or distributed storage
- replace single-node Kafka/ZooKeeper with a proper multi-broker cluster
- coordinate final merge fan-in as a tree merge instead of a single-node merge

The current project is intentionally single-node and assignment-focused, but the overall pattern scales naturally to partitioned distributed processing.
