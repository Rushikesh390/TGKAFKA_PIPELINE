package generator

import (
	"kafka-pipeline/pkg/models"
	"math/rand"
)

var continents = []string{"Asia", "Africa", "North America", "South America", "Europe", "Australia"}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var addressChars = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ")

type Generator struct {
	rnd *rand.Rand
}

func New(seed int64) *Generator {
	return &Generator{
		rnd: rand.New(rand.NewSource(seed)),
	}
}

func (g *Generator) randomString(length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = letters[g.rnd.Intn(len(letters))]
	}
	return string(b)
}

func (g *Generator) randomAddress(length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = addressChars[g.rnd.Intn(len(addressChars))]
	}
	return string(b)
}

func (g *Generator) GenerateRecord(id int32) models.Record {
	return models.Record{
		ID:        id,
		Name:      g.randomString(10 + g.rnd.Intn(6)),
		Address:   g.randomAddress(15 + g.rnd.Intn(6)),
		Continent: continents[g.rnd.Intn(len(continents))],
	}
}
