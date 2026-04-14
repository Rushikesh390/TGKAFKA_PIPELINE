package main

import (
	"kafka-pipeline/internal/generator"
	kafkaproducer "kafka-pipeline/internal/kafka"
	"kafka-pipeline/pkg/utils"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// Load from environment with defaults
func getEnv(key string, defaultVal int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	intVal, err := strconv.Atoi(val)
	if err != nil {
		log.Printf("Warning: Invalid %s value '%s', using default %d\n", key, val, defaultVal)
		return defaultVal
	}
	return intVal
}

func getEnvString(key, defaultVal string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	return val
}

var (
	totalRecords = getEnv("TOTAL_RECORDS", 50_000_000)
	numWorkers   = getEnv("NUM_WORKERS", 4)
	batchSize    = getEnv("BATCH_SIZE", 10000)
	kafkaBroker  = getEnvString("KAFKA_BROKER", "localhost:9092")
)

func main() {
	start := time.Now()
	writer := kafkaproducer.NewWriter("source")
	defer writer.Close()
	var wg sync.WaitGroup
	recordsPerWorker := totalRecords / numWorkers
	remainder := totalRecords % numWorkers

	log.Printf("Starting producer: %d total records across %d workers\n", totalRecords, numWorkers)
	log.Printf("Distribution: %d records per worker + %d remainder\n", recordsPerWorker, remainder)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			messages := make([]kafka.Message, 0, batchSize)
			batchCount := 0

			// Calculate records for this worker (distribute remainder to workers)
			workRecords := recordsPerWorker
			if workerID < remainder {
				workRecords++ // First 'remainder' workers get one extra record
			}

			// Calculate global ID offset for this worker
			globalIDOffset := int64(workerID)*int64(recordsPerWorker) + int64(min(workerID, remainder))

			for j := int64(0); j < int64(workRecords); j++ {
				rec := generator.GenerateRecord(int32(globalIDOffset + j))
				csv := utils.TOCSV(rec)
				messages = append(messages, kafka.Message{Value: []byte(csv)})
				if len(messages) == batchSize {
					err := kafkaproducer.WriteBatch(writer, messages)
					if err != nil {
						log.Printf("Worker %d: Error writing batch: %v\n", workerID, err)
					}
					batchCount++
					if batchCount%100 == 0 {
						log.Printf("Worker %d: Processed %d batches (%d records)\n", workerID, batchCount, batchCount*batchSize)
					}
					messages = messages[:0] // Clear the slice for the next batch
				}
			}

			// flush remaining
			if len(messages) > 0 {
				err := kafkaproducer.WriteBatch(writer, messages)
				if err != nil {
					log.Printf("Worker %d: Error flushing batch: %v\n", workerID, err)
				}
			}
			log.Printf("Worker %d: Finished - processed %d batches\n", workerID, batchCount)
		}(i)
	}

	wg.Wait() // Log the total time taken to generate and send all records
	elapsed := time.Since(start)
	throughput := float64(totalRecords) / elapsed.Seconds()
	log.Printf("Total time: %v\n", elapsed)
	log.Printf("Throughput: %.0f records/sec\n", throughput)

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
