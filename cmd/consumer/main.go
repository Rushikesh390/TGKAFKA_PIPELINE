package main

import (
	"context"
	"fmt"
	"kafka-pipeline/internal/config"
	kafkaclient "kafka-pipeline/internal/kafka"
	"kafka-pipeline/internal/sorter"
	"kafka-pipeline/pkg/models"
	"kafka-pipeline/pkg/utils"
	"log"
	"os"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// BatchJob holds both the batch data and its chunk ID
type BatchJob struct {
	data    []models.Record
	chunkID int
}

func main() {
	cfg := config.GetConfig()
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		log.Fatalf("create output directory: %v", err)
	}

	startTime := time.Now()

	reader := kafkaclient.NewReader(cfg.SourceTopic, cfg.ConsumerGroupID)
	defer reader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batchChan := make(chan BatchJob, cfg.ConsumerNumWorkers)
	errCh := make(chan error, cfg.ConsumerNumWorkers)

	var wg sync.WaitGroup

	for i := 0; i < cfg.ConsumerNumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-batchChan:
					if !ok {
						return
					}
					if err := processChunk(cfg, job.data, job.chunkID); err != nil {
						select {
						case errCh <- err:
						default:
						}
						cancel()
						return
					}
				}
			}
		}()
	}

	batch := make([]models.Record, 0, cfg.ConsumerBatchSize)
	pendingCommits := make([]kafka.Message, 0, cfg.ConsumerCommitSize)
	chunkID := 0
	totalRecords := 0
	chunkStartTime := time.Now()

	log.Println("Consumer started (optimized)...")

	for totalRecords < cfg.TotalRecords {
		select {
		case err := <-errCh:
			log.Fatal(err)
		default:
		}

		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Fatalf("fetch from kafka: %v", err)
		}

		record := utils.FastFromCSV(string(msg.Value))
		batch = append(batch, record)
		pendingCommits = append(pendingCommits, msg)
		totalRecords++

		if len(pendingCommits) >= cfg.ConsumerCommitSize {
			if err := reader.CommitMessages(ctx, pendingCommits...); err != nil {
				log.Fatalf("commit consumer offsets: %v", err)
			}
			pendingCommits = pendingCommits[:0]
		}

		if len(batch) >= cfg.ConsumerBatchSize {
			cycleDuration := time.Since(chunkStartTime)
			log.Printf("Chunk %d: Total cycle time (read + queue) = %v, Total records so far: %d\n", chunkID, cycleDuration, totalRecords)

			job := BatchJob{data: batch, chunkID: chunkID}
			batch = make([]models.Record, 0, cfg.ConsumerBatchSize)

			select {
			case batchChan <- job:
			case <-ctx.Done():
				log.Fatal("consumer cancelled while dispatching chunk")
			}

			chunkID++
			chunkStartTime = time.Now()
		}
	}

	if len(pendingCommits) > 0 {
		if err := reader.CommitMessages(ctx, pendingCommits...); err != nil {
			log.Fatalf("commit final consumer offsets: %v", err)
		}
	}

	if len(batch) > 0 {
		select {
		case batchChan <- BatchJob{data: batch, chunkID: chunkID}:
		case <-ctx.Done():
			log.Fatal("consumer cancelled before final chunk")
		}
	}

	close(batchChan)
	wg.Wait()
	close(errCh)

	if err, ok := <-errCh; ok {
		log.Fatal(err)
	}

	elapsed := time.Since(startTime)

	log.Printf("Consumer finished in %v\n", elapsed)
	log.Printf("Total records processed: %d\n", totalRecords)
}

func processChunk(cfg *config.Config, records []models.Record, chunkID int) error {
	chunkStart := time.Now()

	log.Printf("Processing chunk %d (%d records)...\n", chunkID, len(records))

	var idIndices, nameIndices, continentIndices []int
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		idIndices = sorter.SortByIDIndexed(records)
	}()

	go func() {
		defer wg.Done()
		nameIndices = sorter.SortByNameIndexed(records)
	}()

	go func() {
		defer wg.Done()
		continentIndices = sorter.SortByContinentIndexed(records)
	}()

	wg.Wait()

	writeErrCh := make(chan error, 3)
	wg.Add(3)

	go func() {
		defer wg.Done()
		if err := sorter.WriteChunkByIndices(records, idIndices, fmt.Sprintf("%s/id_chunk_%d.csv", cfg.OutputDir, chunkID)); err != nil {
			writeErrCh <- err
		}
	}()

	go func() {
		defer wg.Done()
		if err := sorter.WriteChunkByIndices(records, nameIndices, fmt.Sprintf("%s/name_chunk_%d.csv", cfg.OutputDir, chunkID)); err != nil {
			writeErrCh <- err
		}
	}()

	go func() {
		defer wg.Done()
		if err := sorter.WriteChunkByIndices(records, continentIndices, fmt.Sprintf("%s/continent_chunk_%d.csv", cfg.OutputDir, chunkID)); err != nil {
			writeErrCh <- err
		}
	}()

	wg.Wait()
	close(writeErrCh)

	if err, ok := <-writeErrCh; ok {
		return fmt.Errorf("write chunk %d: %w", chunkID, err)
	}

	elapsed := time.Since(chunkStart)
	log.Printf("Chunk %d done in %v\n", chunkID, elapsed)
	return nil
}
