package sorter

import (
	"testing"

	"kafka-pipeline/pkg/models"
)

func TestSortByIDIndexed(t *testing.T) {
	records := []models.Record{
		{ID: 30, Name: "ccc", Continent: "Europe"},
		{ID: 10, Name: "bbb", Continent: "Asia"},
		{ID: 20, Name: "aaa", Continent: "Africa"},
	}

	indices := SortByIDIndexed(records)

	want := []int32{10, 20, 30}
	for i, id := range want {
		if got := records[indices[i]].ID; got != id {
			t.Fatalf("position %d: got ID %d, want %d", i, got, id)
		}
	}
}

func TestSortByNameIndexed(t *testing.T) {
	records := []models.Record{
		{ID: 1, Name: "zoe", Continent: "Europe"},
		{ID: 2, Name: "anna", Continent: "Asia"},
		{ID: 3, Name: "mike", Continent: "Africa"},
	}

	indices := SortByNameIndexed(records)

	want := []string{"anna", "mike", "zoe"}
	for i, name := range want {
		if got := records[indices[i]].Name; got != name {
			t.Fatalf("position %d: got name %q, want %q", i, got, name)
		}
	}
}

func TestSortByContinentIndexed(t *testing.T) {
	records := []models.Record{
		{ID: 1, Name: "a", Continent: "South America"},
		{ID: 2, Name: "b", Continent: "Africa"},
		{ID: 3, Name: "c", Continent: "Europe"},
	}

	indices := SortByContinentIndexed(records)

	want := []string{"Africa", "Europe", "South America"}
	for i, continent := range want {
		if got := records[indices[i]].Continent; got != continent {
			t.Fatalf("position %d: got continent %q, want %q", i, got, continent)
		}
	}
}
