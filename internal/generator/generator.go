package generator

import (
	"kafka-pipeline/pkg/models"
	"math/rand"
	"time"
)

var continents = []string{"Asia", "Africa", "North America", "South America", "Europe", "Australia"}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func init() {
	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())
}

func randomString(length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func randomAddress(length int) string {
	chars := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ")
	b := make([]rune, length)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func GenerateRecord(id int32) models.Record {
	return models.Record{
		ID:        id,
		Name:      randomString(10 + rand.Intn(6)),
		Address:   randomAddress(15 + rand.Intn(6)),
		Continent: continents[rand.Intn(len(continents))],
	}
}
