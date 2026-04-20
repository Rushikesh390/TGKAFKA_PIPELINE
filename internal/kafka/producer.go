package kafka

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

func getEnvInt(key string, defaultVal int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	intVal, _ := strconv.Atoi(val)
	return intVal
}

func getEnvString(key, defaultVal string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	return val
}

func NewWriter(topic string) *kafka.Writer {
	broker := getEnvString("KAFKA_BROKER", "localhost:9092")
	batchSize := getEnvInt("KAFKA_BATCH_SIZE", 10_000)
	batchTimeout := getEnvInt("KAFKA_BATCH_TIMEOUT", 100)

	return &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    batchSize,
		BatchTimeout: time.Duration(batchTimeout) * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Compression:  kafka.Snappy,
	}
}

func WriteBatch(writer *kafka.Writer, messages []kafka.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return writer.WriteMessages(ctx, messages...)
}
