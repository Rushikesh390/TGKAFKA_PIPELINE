package sorter

import (
	"bufio"
	"kafka-pipeline/pkg/models"
	"kafka-pipeline/pkg/utils"
	"os"
)

func WriteChunkToFile(records []models.Record, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriterSize(file, 4<<20)
	buf := make([]byte, 0, 96)
	for _, r := range records {
		buf = utils.AppendCSVLine(buf[:0], r)
		if _, err := writer.Write(buf); err != nil {
			return err
		}
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

	writer := bufio.NewWriterSize(file, 4<<20)
	buf := make([]byte, 0, 96)
	for _, idx := range indices {
		r := records[idx]
		buf = utils.AppendCSVLine(buf[:0], r)
		if _, err := writer.Write(buf); err != nil {
			return err
		}
	}
	return writer.Flush()
}
