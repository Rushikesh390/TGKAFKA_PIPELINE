package generator

import (
	"strings"
	"testing"
)

func TestGenerateRecordMatchesSchema(t *testing.T) {
	gen := New(42)
	record := gen.GenerateRecord(123)

	if record.ID != 123 {
		t.Fatalf("got ID %d, want 123", record.ID)
	}
	if len(record.Name) < 10 || len(record.Name) > 15 {
		t.Fatalf("unexpected name length %d", len(record.Name))
	}
	if len(record.Address) < 15 || len(record.Address) > 20 {
		t.Fatalf("unexpected address length %d", len(record.Address))
	}

	validContinents := map[string]bool{
		"Africa":        true,
		"Asia":          true,
		"Australia":     true,
		"Europe":        true,
		"North America": true,
		"South America": true,
	}
	if !validContinents[record.Continent] {
		t.Fatalf("unexpected continent %q", record.Continent)
	}

	if strings.Contains(record.Name, " ") {
		t.Fatalf("name contains spaces: %q", record.Name)
	}
}
