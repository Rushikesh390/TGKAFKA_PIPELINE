package utils

import (
	"fmt"
	"kafka-pipeline/pkg/models"
)

func TOCSV(records models.Record) string {
	return fmt.Sprintf("%d,%s,%s,%s", records.ID, records.Name, records.Address, records.Continent)
}