package merger

import (
	"container/heap"
	"context"
	"fmt"
	"log"
	"time"

	"kafka-pipeline/pkg/utils"

	kafka "github.com/segmentio/kafka-go"
)

func MergeFiles(filePattern string, numFiles int, topic string, less func(a, b string) bool) {

	start := time.Now()

	//  OLD: readers created but merge inside loop
	/*
		for i := 0; i < numFiles; i++ {
	*/

	//  NEW: initialize all readers first
	readers := make([]*FileReader, numFiles)

	h := &MinHeap{
		Items:    []Item{},
		LessFunc: less, // Now passes CSV strings directly - no formatting!
	}

	heap.Init(h)

	//  OPEN ALL FILES FIRST
	log.Printf("Opening %d files for topic %s...", numFiles, topic)
	for i := 0; i < numFiles; i++ {
		filename := fmt.Sprintf(filePattern, i)

		fr, err := NewFileReader(filename)
		if err != nil {
			log.Fatalf("error opening file %s: %v", filename, err)
		}

		readers[i] = fr

		line, ok := fr.Next()
		if ok {
			rec := utils.FastFromCSV(line) // OPTIMIZED: use FastFromCSV
			// OPTIMIZATION: Cache CSV string to avoid repeated formatting in heap comparisons
			heap.Push(h, Item{Record: rec, CSVLine: line, FileID: i})
		}
	}
	log.Printf("All files opened. Heap has %d items. Starting merge...", h.Len())

	//  OLD: writer inside loop
	/*
		writer := kafka.NewWriter(topic)
		defer writer.Close()
	*/

	//  NEW: single writer
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        topic,
		BatchSize:    5000,
		BatchTimeout: 50 * time.Millisecond,
	})
	defer writer.Close()

	ctx := context.Background()

	//  batching
	batch := make([]kafka.Message, 0, 5000)

	//  OLD: merge inside file loop
	/*
		for h.Len() > 0 {
	*/

	//  CORRECT MERGE LOOP
	recordsWritten := 0
	batchesWritten := 0
	for h.Len() > 0 {

		item := heap.Pop(h).(Item)

		batch = append(batch, kafka.Message{
			Value: []byte(utils.TOCSV(item.Record)),
		})
		recordsWritten++

		// flush batch
		if len(batch) >= 5000 {
			batchesWritten++
			// Log progress every 10 batches (50K records) instead of every batch
			if batchesWritten%10 == 0 {
				log.Printf("Progress: topic %s has written %d records (%d batches)", topic, recordsWritten, batchesWritten)
			}
			err := writer.WriteMessages(ctx, batch...)
			if err != nil {
				log.Printf("ERROR writing batch to topic %s: %v", topic, err)
			}
			batch = batch[:0]
		}

		// read next from same file
		line, ok := readers[item.FileID].Next()
		if ok {
			rec := utils.FastFromCSV(line) // OPTIMIZED: use FastFromCSV
			// OPTIMIZATION: Cache CSV to avoid repeated formatting
			heap.Push(h, Item{
				Record:   rec,
				CSVLine:  line,
				FileID:   item.FileID,
			})
		}
	}

	// flush remaining
	if len(batch) > 0 {
		log.Printf("Flushing final batch for topic %s (total records: %d)", topic, recordsWritten)
		writer.WriteMessages(ctx, batch...)
	}

	//  OLD: incorrect placement
	/*
		for _, r := range readers {
			r.Close()
		}
	*/

	//  NEW: close all readers properly
	log.Printf("Closing %d file readers for topic %s...", len(readers), topic)
	for _, r := range readers {
		r.Close()
	}

	elapsed := time.Since(start)
	log.Printf("Merge completed for topic %s in %v (total records: %d)\n", topic, elapsed, recordsWritten)
}
