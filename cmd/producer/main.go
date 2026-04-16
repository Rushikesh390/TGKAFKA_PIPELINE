package main

import (
	"context"
	"fmt"
	"kafka-pipeline/internal/config"
	"kafka-pipeline/internal/generator"
	kafkaproducer "kafka-pipeline/internal/kafka"
	"kafka-pipeline/pkg/utils"
	"log"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	cfg := config.GetConfig()
	start := time.Now()
	writer := kafkaproducer.NewWriter(cfg.SourceTopic)
	defer writer.Close()

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, cfg.ProducerNumWorkers)
	recordsPerWorker := cfg.TotalRecords / cfg.ProducerNumWorkers
	remainder := cfg.TotalRecords % cfg.ProducerNumWorkers

	log.Printf("Starting producer: %d total records across %d workers\n", cfg.TotalRecords, cfg.ProducerNumWorkers)
	log.Printf("Distribution: %d records per worker + %d remainder\n", recordsPerWorker, remainder)

	for i := 0; i < cfg.ProducerNumWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			gen := generator.New(time.Now().UnixNano() + int64(workerID))
			messages := make([]kafka.Message, 0, cfg.ProducerBatchSize)
			batchCount := 0

			// Calculate records for this worker (distribute remainder to workers)
			workRecords := recordsPerWorker
			if workerID < remainder {
				workRecords++ // First 'remainder' workers get one extra record
			}

			// Calculate global ID offset for this worker
			globalIDOffset := int64(workerID)*int64(recordsPerWorker) + int64(min(workerID, remainder))

			for j := int64(0); j < int64(workRecords); j++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				rec := gen.GenerateRecord(int32(globalIDOffset + j))
				csv := utils.TOCSV(rec)
				messages = append(messages, kafka.Message{Value: []byte(csv)})
				if len(messages) == cfg.ProducerBatchSize {
					err := kafkaproducer.WriteBatch(writer, messages)
					if err != nil {
						select {
						case errCh <- fmt.Errorf("worker %d write batch: %w", workerID, err):
						default:
						}
						cancel()
						return
					}
					batchCount++
					if batchCount%100 == 0 {
						log.Printf("Worker %d: Processed %d batches (%d records)\n", workerID, batchCount, batchCount*cfg.ProducerBatchSize)
					}
					messages = messages[:0] // Clear the slice for the next batch
				}
			}

			// flush remaining
			if len(messages) > 0 {
				err := kafkaproducer.WriteBatch(writer, messages)
				if err != nil {
					select {
					case errCh <- fmt.Errorf("worker %d flush batch: %w", workerID, err):
					default:
					}
					cancel()
					return
				}
			}
			log.Printf("Worker %d: Finished - processed %d batches\n", workerID, batchCount)
		}(i)
	}

	wg.Wait()
	close(errCh)

	if err, ok := <-errCh; ok {
		log.Fatal(err)
	}

	elapsed := time.Since(start)
	throughput := float64(cfg.TotalRecords) / elapsed.Seconds()
	log.Printf("Total time: %v\n", elapsed)
	log.Printf("Throughput: %.0f records/sec\n", throughput)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
