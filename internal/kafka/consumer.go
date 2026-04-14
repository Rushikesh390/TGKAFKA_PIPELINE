package kafka

import (
	"github.com/segmentio/kafka-go"
)

func NewReader(topic string) *kafka.Reader {
	broker := getEnvString("KAFKA_BROKER", "localhost:9092")
	minBytes := getEnvInt("CONSUMER_MIN_BYTES", 100_000)
	maxBytes := getEnvInt("CONSUMER_MAX_BYTES", 50_000_000)

	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          topic,
		GroupID:        "group-1",
		MinBytes:       minBytes,
		MaxBytes:       maxBytes,
		CommitInterval: 1000,
		StartOffset:    -2,
	})
}
