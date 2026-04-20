package utils

import (
	"kafka-pipeline/pkg/models"
	"strconv"
)

func TOCSV(records models.Record) string {
	return string(AppendCSV(nil, records))
}

func AppendCSV(dst []byte, record models.Record) []byte {
	dst = strconv.AppendInt(dst, int64(record.ID), 10)
	dst = append(dst, ',')
	dst = append(dst, record.Name...)
	dst = append(dst, ',')
	dst = append(dst, record.Address...)
	dst = append(dst, ',')
	dst = append(dst, record.Continent...)
	return dst
}

func AppendCSVLine(dst []byte, record models.Record) []byte {
	dst = AppendCSV(dst, record)
	dst = append(dst, '\n')
	return dst
}
