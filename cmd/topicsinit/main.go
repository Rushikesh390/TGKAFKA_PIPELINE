package main

import (
	"fmt"
	"kafka-pipeline/internal/config"
	"log"
	"net"
	"strings"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	cfg := config.GetConfig()

	conn, err := kafka.Dial("tcp", cfg.KafkaBroker)
	if err != nil {
		log.Fatalf("dial kafka broker: %v", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		log.Fatalf("lookup controller: %v", err)
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port)))
	if err != nil {
		log.Fatalf("dial controller: %v", err)
	}
	defer controllerConn.Close()

	topics := []kafka.TopicConfig{
		{
			Topic:             cfg.SourceTopic,
			NumPartitions:     cfg.SourceTopicPartitions,
			ReplicationFactor: 1,
		},
		{
			Topic:             cfg.IDTopic,
			NumPartitions:     cfg.OutputTopicPartitions,
			ReplicationFactor: 1,
		},
		{
			Topic:             cfg.NameTopic,
			NumPartitions:     cfg.OutputTopicPartitions,
			ReplicationFactor: 1,
		},
		{
			Topic:             cfg.ContinentTopic,
			NumPartitions:     cfg.OutputTopicPartitions,
			ReplicationFactor: 1,
		},
	}

	if err := controllerConn.CreateTopics(topics...); err != nil && !isTopicExistsErr(err) {
		log.Fatalf("create topics: %v", err)
	}

	log.Printf("Topics are ready: %s, %s, %s, %s",
		cfg.SourceTopic, cfg.IDTopic, cfg.NameTopic, cfg.ContinentTopic)
}

func isTopicExistsErr(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "Topic with this name already exists") ||
		strings.Contains(err.Error(), "topic already exists") ||
		strings.Contains(err.Error(), "TopicAlreadyExists")
}
