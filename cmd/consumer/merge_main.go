package main

import (
	"log"
	"path/filepath"
	"strconv"
	"strings"

	"kafka-pipeline/internal/merger"
)

func main() {

	// Count actual chunks from output directory (more reliable)
	files, err := filepath.Glob("output/id_chunk_*.csv")
	if err != nil {
		log.Fatalf("Error globbing files: %v", err)
	}

	numChunks := len(files)
	if numChunks == 0 {
		log.Fatal("No chunk files found. Make sure consumer has completed.")
	}

	log.Printf("Found %d chunks, starting merge...\n", numChunks)

	// ID sort - compare numeric IDs (field 0)
	merger.MergeFiles("output/id_chunk_%d.csv", numChunks, "id-sorted", func(a, b string) bool {
		idA := extractField(a, 0)
		idB := extractField(b, 0)
		numA, _ := strconv.ParseInt(idA, 10, 64)
		numB, _ := strconv.ParseInt(idB, 10, 64)
		return numA < numB
	})

	// Name sort - compare names (field 1)
	merger.MergeFiles("output/name_chunk_%d.csv", numChunks, "name-sorted", func(a, b string) bool {
		nameA := extractField(a, 1)
		nameB := extractField(b, 1)
		return nameA < nameB
	})

	// Continent sort - compare continents (field 3)
	merger.MergeFiles("output/continent_chunk_%d.csv", numChunks, "continent-sorted", func(a, b string) bool {
		contA := extractField(a, 3)
		contB := extractField(b, 3)
		return contA < contB
	})

	log.Println("All merges completed successfully!")
}

// extractField extracts the field at index from a CSV line (assumes unquoted)
func extractField(line string, fieldIndex int) string {
	fields := strings.Split(line, ",")
	if fieldIndex >= len(fields) {
		return ""
	}
	return fields[fieldIndex]
}
