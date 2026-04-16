package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"
)

func NewReader(topic, groupID string) *kafka.Reader {
	broker := getEnvString("KAFKA_BROKER", "localhost:9092")
	minBytes := getEnvInt("CONSUMER_MIN_BYTES", 100_000)
	maxBytes := getEnvInt("CONSUMER_MAX_BYTES", 50_000_000)

	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       minBytes,
		MaxBytes:       maxBytes,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})
}
