package sorter

import (
	"kafka-pipeline/pkg/models"
	"sort"
)

// SortByID uses indexed sorting (no data copy)
func SortByID(records []models.Record) {
	sort.Slice(records, func(i, j int) bool {
		return records[i].ID < records[j].ID
	})
}

// SortByIDIndexed returns indices sorted by ID (for reference-based sorting)
func SortByIDIndexed(records []models.Record) []int {
	n := len(records)
	indices := make([]int, n)
	for i := 0; i < n; i++ {
		indices[i] = i
	}

	sort.Slice(indices, func(i, j int) bool {
		return records[indices[i]].ID < records[indices[j]].ID
	})
	return indices
}

func SortByName(records []models.Record) {
	sort.Slice(records, func(i, j int) bool {
		return records[i].Name < records[j].Name
	})
}

// SortByNameIndexed returns indices sorted by Name
func SortByNameIndexed(records []models.Record) []int {
	n := len(records)
	indices := make([]int, n)
	for i := 0; i < n; i++ {
		indices[i] = i
	}

	sort.Slice(indices, func(i, j int) bool {
		left := records[indices[i]]
		right := records[indices[j]]
		if left.Name == right.Name {
			return left.ID < right.ID
		}
		return left.Name < right.Name
	})
	return indices
}

func SortByContinent(records []models.Record) {
	sort.Slice(records, func(i, j int) bool {
		return records[i].Continent < records[j].Continent
	})
}

// SortByContinentIndexed returns indices sorted by Continent
func SortByContinentIndexed(records []models.Record) []int {
	n := len(records)
	indices := make([]int, n)
	for i := 0; i < n; i++ {
		indices[i] = i
	}

	sort.Slice(indices, func(i, j int) bool {
		left := records[indices[i]]
		right := records[indices[j]]
		if left.Continent == right.Continent {
			return left.ID < right.ID
		}
		return left.Continent < right.Continent
	})
	return indices
}
