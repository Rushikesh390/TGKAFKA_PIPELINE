package kafka

import (
	"github.com/segmentio/kafka-go"
)

func NewReader(topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          topic,
		GroupID:        "group-1",
		MinBytes:       100e3, // 100KB (was 10KB)
		MaxBytes:       50e6,  // 50MB (was 10MB) - read bigger chunks
		CommitInterval: 1000,  // batch commit
		StartOffset:    -2,    // from beginning
	})

}
