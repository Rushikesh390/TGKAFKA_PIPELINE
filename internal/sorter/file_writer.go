package sorter

import (
	"bufio"
	"fmt"
	"os"
	"kafka-pipeline/pkg/models"
)

func WriteChunkToFile(records []models.Record, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, r := range records {
		line := fmt.Sprintf("%d,%s,%s,%s\n",
			r.ID, r.Name, r.Address, r.Continent)
		writer.WriteString(line)
	}
	return writer.Flush()
}

// WriteChunkByIndices writes records in sorted order using indices (NO COPY!)
func WriteChunkByIndices(records []models.Record, indices []int, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, idx := range indices {
		r := records[idx]
		line := fmt.Sprintf("%d,%s,%s,%s\n",
			r.ID, r.Name, r.Address, r.Continent)
		writer.WriteString(line)
	}
	return writer.Flush()
}