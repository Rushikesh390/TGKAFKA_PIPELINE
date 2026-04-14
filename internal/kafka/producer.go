package kafka

import ("github.com/segmentio/kafka-go"
"context"
"time")

func NewWriter(topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr: kafka.TCP("localhost:9092"),
		Topic: topic,
		Balancer: &kafka.LeastBytes{},
		BatchSize: 5000,           // OPTIMIZED: was 1000, increased 5x
		BatchTimeout: 50 * time.Millisecond,  // OPTIMIZED: was 10ms
		RequiredAcks: kafka.RequireOne,
		Compression: kafka.Snappy,  // OPTIMIZED: add compression
	}
}


func WriteBatch(writer *kafka.Writer, messages []kafka.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)  // OPTIMIZED: was 10s
	defer cancel()

	return writer.WriteMessages(ctx, messages...)
}