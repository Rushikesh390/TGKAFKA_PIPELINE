package main

import (
	"context"
	"fmt"
	"kafka-pipeline/internal/kafka"
	"kafka-pipeline/internal/sorter"
	"kafka-pipeline/pkg/models"
	"kafka-pipeline/pkg/utils"
	"log"
	"sync"
	"time"
)

const (
	// batchSize = 200000  //  OLD (small batch)
	batchSize = 1_000_000 //  OPTIMIZED: 1M for better memory efficiency with 2GB RAM

	numWorkers = 4 //  parallel workers
)

// BatchJob holds both the batch data and its chunk ID
type BatchJob struct {
	data    []models.Record
	chunkID int
}

func main() {

	startTime := time.Now()

	reader := kafka.NewReader("source")
	defer reader.Close()

	//  NEW: channel for decoupling read & process
	batchChan := make(chan BatchJob, numWorkers)

	var wg sync.WaitGroup

	//  WORKER POOL
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for job := range batchChan {
				processChunk(job.data, job.chunkID) // parallel processing with correct chunk ID
			}
		}(i)
	}

	var batch []models.Record
	chunkID := 0
	totalRecords := 0
	chunkStartTime := time.Now()
	expectedRecords := 50_000_000
	idleTimeout := 10 * time.Second

	log.Println("Consumer started (optimized)...")

	for {
		// Create context with 10-second timeout to detect idle/EOF
		readCtx, cancel := context.WithTimeout(context.Background(), idleTimeout)
		msg, err := reader.ReadMessage(readCtx)
		cancel()

		if err != nil {
			// Check if we've reached expected record count
			if totalRecords >= expectedRecords {
				log.Printf("Reached expected record count (%d). Finishing...\n", expectedRecords)
				break
			}
			log.Println("Finished reading:", err)
			break
		}

		record := utils.FastFromCSV(string(msg.Value)) // OPTIMIZED: use FastFromCSV instead
		batch = append(batch, record)
		totalRecords++

		if len(batch) >= batchSize {

			//  NEW NON-BLOCKING
			batchCopy := make([]models.Record, len(batch))
			copy(batchCopy, batch)

			// Log total time for this chunk cycle (read + queue)
			cycleDuration := time.Since(chunkStartTime)
			log.Printf("Chunk %d: Total cycle time (read + queue) = %v, Total records so far: %d\n", chunkID, cycleDuration, totalRecords)

			batchChan <- BatchJob{data: batchCopy, chunkID: chunkID}

			chunkID++
			batch = nil
			chunkStartTime = time.Now() // Reset timer for next chunk
		}
	}

	// last batch
	if len(batch) > 0 {
		batchChan <- BatchJob{data: batch, chunkID: chunkID}
	}

	close(batchChan)
	wg.Wait()

	elapsed := time.Since(startTime)

	log.Printf("Consumer finished in %v\n", elapsed)
	log.Printf("Total records processed: %d\n", totalRecords)
}

func processChunk(records []models.Record, chunkID int) {

	chunkStart := time.Now()

	log.Printf("Processing chunk %d (%d records)...\n", chunkID, len(records))

	//  OPTIMIZED: NO 3 DATA COPIES!
	//  Instead, we sort indices and write using those indices
	//  This saves 2-3GB of memory and eliminates copy overhead

	//  NEW PARALLEL INDEX SORTING (no copy)
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

	//  PARALLEL WRITE using indices (NO COPY!)
	wg.Add(3)

	go func() {
		defer wg.Done()
		sorter.WriteChunkByIndices(records, idIndices, fmt.Sprintf("output/id_chunk_%d.csv", chunkID))
	}()

	go func() {
		defer wg.Done()
		sorter.WriteChunkByIndices(records, nameIndices, fmt.Sprintf("output/name_chunk_%d.csv", chunkID))
	}()

	go func() {
		defer wg.Done()
		sorter.WriteChunkByIndices(records, continentIndices, fmt.Sprintf("output/continent_chunk_%d.csv", chunkID))
	}()

	wg.Wait()

	elapsed := time.Since(chunkStart)
	log.Printf("Chunk %d done in %v\n", chunkID, elapsed)
}
