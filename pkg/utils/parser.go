package utils

import (
	"kafka-pipeline/pkg/models"
	"strconv"
	"strings"
)

// FastFromCSV uses fast string split instead of csv.Reader (10x faster)
func FastFromCSV(line string) models.Record {
	parts := strings.Split(line, ",")
	if len(parts) < 4 {
		return models.Record{}
	}

	id, _ := strconv.Atoi(parts[0])
	return models.Record{
		ID:        int32(id),
		Name:      parts[1],
		Address:   parts[2],
		Continent: parts[3],
	}
}

func FromCSV(line string) models.Record {
	parts := strings.Split(line, ",")
	if len(parts) < 4 {
		return models.Record{}
	}

	id, _ := strconv.Atoi(parts[0])
	return models.Record{
		ID:        int32(id),
		Name:      parts[1],
		Address:   parts[2],
		Continent: parts[3],
	}
}
