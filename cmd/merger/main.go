package main

import (
	"log"
	"path/filepath"
	"sync"

	"kafka-pipeline/internal/config"
	"kafka-pipeline/internal/merger"
)

func main() {
	cfg := config.GetConfig()

	files, err := filepath.Glob(filepath.Join(cfg.OutputDir, "id_chunk_*.csv"))
	if err != nil {
		log.Fatalf("glob chunk files: %v", err)
	}

	numChunks := len(files)
	if numChunks == 0 {
		log.Fatal("no chunk files found; make sure the consumer completed successfully")
	}

	log.Printf("Found %d chunks, starting parallel merge...\n", numChunks)

	var wg sync.WaitGroup
	errCh := make(chan error, 3)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := merger.MergeFiles(filepath.Join(cfg.OutputDir, "id_chunk_%d.csv"), numChunks, cfg.IDTopic, func(a, b merger.Item) bool {
			return a.Record.ID < b.Record.ID
		}); err != nil {
			errCh <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := merger.MergeFiles(filepath.Join(cfg.OutputDir, "name_chunk_%d.csv"), numChunks, cfg.NameTopic, func(a, b merger.Item) bool {
			if a.Record.Name == b.Record.Name {
				return a.Record.ID < b.Record.ID
			}
			return a.Record.Name < b.Record.Name
		}); err != nil {
			errCh <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := merger.MergeFiles(filepath.Join(cfg.OutputDir, "continent_chunk_%d.csv"), numChunks, cfg.ContinentTopic, func(a, b merger.Item) bool {
			if a.Record.Continent == b.Record.Continent {
				return a.Record.ID < b.Record.ID
			}
			return a.Record.Continent < b.Record.Continent
		}); err != nil {
			errCh <- err
		}
	}()

	wg.Wait()
	close(errCh)

	if err, ok := <-errCh; ok {
		log.Fatal(err)
	}

	log.Println("All merges completed successfully in parallel!")
}
