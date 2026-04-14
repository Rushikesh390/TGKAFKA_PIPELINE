package main

import (
	"context"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
)

func getEnvString(key, defaultVal string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	return val
}

func main() {
	broker := getEnvString("KAFKA_BROKER", "localhost:9092")

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   "source",
	})

	err := writer.WriteMessages(context.Background(),
		kafka.Message{
			Value: []byte("Hello, World!"),
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Message sent to kafka topic source")
}
