package utils

import (
	"kafka-pipeline/pkg/models"
	"strconv"
	"strings"
)

func FastFromCSV(line string) models.Record {
	first := strings.IndexByte(line, ',')
	if first <= 0 {
		return models.Record{}
	}

	secondOffset := strings.IndexByte(line[first+1:], ',')
	if secondOffset == -1 {
		return models.Record{}
	}
	second := first + 1 + secondOffset

	last := strings.LastIndexByte(line, ',')
	if last <= second {
		return models.Record{}
	}

	id, _ := strconv.Atoi(line[:first])
	return models.Record{
		ID:        int32(id),
		Name:      line[first+1 : second],
		Address:   line[second+1 : last],
		Continent: line[last+1:],
	}
}

func FromCSV(line string) models.Record {
	return FastFromCSV(line)
}
