package main

import ("github.com/segmentio/kafka-go"
	"context"
	"log")

func main() {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic: "source",
	})

	err := writer.WriteMessages(context.Background(),
kafka.Message{
	Value: []byte("Hello, World!"),
},)
 if err !=nil{
	log.Fatal(err)
	
 }
    log.Println("Message sent to kafka topic source")
}