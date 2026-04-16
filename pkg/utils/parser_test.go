package utils

import "testing"

func TestFastFromCSV(t *testing.T) {
	record := FastFromCSV("21,abcdefghij,12 abc dfsf LdUE,Asia")

	if record.ID != 21 {
		t.Fatalf("got ID %d, want 21", record.ID)
	}
	if record.Name != "abcdefghij" {
		t.Fatalf("got Name %q", record.Name)
	}
	if record.Address != "12 abc dfsf LdUE" {
		t.Fatalf("got Address %q", record.Address)
	}
	if record.Continent != "Asia" {
		t.Fatalf("got Continent %q", record.Continent)
	}
}
