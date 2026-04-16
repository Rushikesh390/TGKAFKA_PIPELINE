package merger

import (
	"container/heap"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"kafka-pipeline/pkg/utils"

	kafka "github.com/segmentio/kafka-go"
)

// Helper functions to get environment variables
func getEnvInt(key string, defaultVal int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	intVal, _ := strconv.Atoi(val)
	return intVal
}

func getEnvString(key, defaultVal string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	return val
}

func MergeFiles(filePattern string, numFiles int, topic string, less func(a, b Item) bool) error {

	start := time.Now()

	readers := make([]*FileReader, numFiles)

	h := &MinHeap{
		Items:    []Item{},
		LessFunc: less,
	}

	heap.Init(h)

	log.Printf("Opening %d files for topic %s...", numFiles, topic)
	for i := 0; i < numFiles; i++ {
		filename := fmt.Sprintf(filePattern, i)

		fr, err := NewFileReader(filename)
		if err != nil {
			return fmt.Errorf("open file %s: %w", filename, err)
		}

		readers[i] = fr

		line, ok := fr.Next()
		if ok {
			rec := utils.FastFromCSV(line)
			heap.Push(h, Item{Record: rec, CSVLine: line, FileID: i})
		}
	}
	log.Printf("All files opened. Heap has %d items. Starting merge...", h.Len())

	broker := getEnvString("KAFKA_BROKER", "localhost:9092")
	batchSize := getEnvInt("MERGER_BATCH_SIZE", 20000)
	batchTimeout := getEnvInt("KAFKA_BATCH_TIMEOUT", 50)

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{broker},
		Topic:        topic,
		BatchSize:    batchSize,
		BatchTimeout: time.Duration(batchTimeout) * time.Millisecond,
	})
	defer writer.Close()

	ctx := context.Background()
	batch := make([]kafka.Message, 0, batchSize)
	recordsWritten := 0
	batchesWritten := 0
	for h.Len() > 0 {
		item := heap.Pop(h).(Item)

		batch = append(batch, kafka.Message{
			Value: []byte(item.CSVLine),
		})
		recordsWritten++

		if len(batch) >= batchSize {
			batchesWritten++
			if batchesWritten%10 == 0 {
				log.Printf("Progress: topic %s has written %d records (%d batches)", topic, recordsWritten, batchesWritten)
			}
			err := writer.WriteMessages(ctx, batch...)
			if err != nil {
				closeReaders(readers)
				return fmt.Errorf("write batch to topic %s: %w", topic, err)
			}
			batch = batch[:0]
		}

		line, ok := readers[item.FileID].Next()
		if ok {
			rec := utils.FastFromCSV(line)
			heap.Push(h, Item{
				Record:  rec,
				CSVLine: line,
				FileID:  item.FileID,
			})
		}
	}

	if len(batch) > 0 {
		log.Printf("Flushing final batch for topic %s (total records: %d)", topic, recordsWritten)
		if err := writer.WriteMessages(ctx, batch...); err != nil {
			closeReaders(readers)
			return fmt.Errorf("flush final batch to topic %s: %w", topic, err)
		}
	}

	log.Printf("Closing %d file readers for topic %s...", len(readers), topic)
	closeReaders(readers)

	elapsed := time.Since(start)
	log.Printf("Merge completed for topic %s in %v (total records: %d)\n", topic, elapsed, recordsWritten)
	return nil
}

func closeReaders(readers []*FileReader) {
	for _, r := range readers {
		if r != nil {
			r.Close()
		}
	}
}
