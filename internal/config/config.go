package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Config holds all environment configuration
type Config struct {
	// Pipeline configuration
	TotalRecords       int
	ProducerNumWorkers int
	ProducerBatchSize  int
	OutputDir          string
	TopicPartitions    int

	// Kafka configuration
	KafkaBroker       string
	KafkaBatchSize    int
	KafkaBatchTimeout int // milliseconds

	// Topic configuration
	SourceTopic    string
	IDTopic        string
	NameTopic      string
	ContinentTopic string

	// Consumer configuration
	ConsumerMinBytes   int
	ConsumerMaxBytes   int
	ConsumerNumWorkers int
	ConsumerBatchSize  int
	ConsumerGroupID    string
	ConsumerIdleSecs   int
	ConsumerIdleMaxes  int

	// Merger configuration
	MergerBatchSize int
}

// GetConfig loads configuration from environment variables
func GetConfig() *Config {
	cfg := &Config{
		// Pipeline defaults
		TotalRecords:       getEnvInt("TOTAL_RECORDS", 50_000_000),
		ProducerNumWorkers: getEnvInt("PRODUCER_NUM_WORKERS", getEnvInt("NUM_WORKERS", 4)),
		ProducerBatchSize:  getEnvInt("PRODUCER_BATCH_SIZE", getEnvInt("BATCH_SIZE", 10_000)),
		OutputDir:          getEnvString("OUTPUT_DIR", "output"),
		TopicPartitions:    getEnvInt("TOPIC_PARTITIONS", 4),

		// Kafka defaults
		KafkaBroker:       getEnvString("KAFKA_BROKER", "localhost:9092"),
		KafkaBatchSize:    getEnvInt("KAFKA_BATCH_SIZE", 5000),
		KafkaBatchTimeout: getEnvInt("KAFKA_BATCH_TIMEOUT", 50),

		// Topic defaults
		SourceTopic:    getEnvString("SOURCE_TOPIC", "source"),
		IDTopic:        getEnvString("ID_TOPIC", "id"),
		NameTopic:      getEnvString("NAME_TOPIC", "name"),
		ContinentTopic: getEnvString("CONTINENT_TOPIC", "continent"),

		// Consumer defaults
		ConsumerMinBytes:   getEnvInt("CONSUMER_MIN_BYTES", 100_000),
		ConsumerMaxBytes:   getEnvInt("CONSUMER_MAX_BYTES", 50_000_000),
		ConsumerNumWorkers: getEnvInt("CONSUMER_NUM_WORKERS", 1),
		ConsumerBatchSize:  getEnvInt("CONSUMER_BATCH_SIZE", 200_000),
		ConsumerGroupID:    getEnvString("CONSUMER_GROUP_ID", fmt.Sprintf("pipeline-consumer-%d", time.Now().UnixNano())),
		ConsumerIdleSecs:   getEnvInt("CONSUMER_IDLE_TIMEOUT_SECS", 10),
		ConsumerIdleMaxes:  getEnvInt("CONSUMER_IDLE_MAX_ATTEMPTS", 6),

		// Merger defaults
		MergerBatchSize: getEnvInt("MERGER_BATCH_SIZE", 20000),
	}

	log.Printf("Configuration loaded:")
	log.Printf("  Total Records: %d", cfg.TotalRecords)
	log.Printf("  Producer Workers: %d", cfg.ProducerNumWorkers)
	log.Printf("  Consumer Workers: %d", cfg.ConsumerNumWorkers)
	log.Printf("  Consumer Batch Size: %d", cfg.ConsumerBatchSize)
	log.Printf("  Kafka Broker: %s", cfg.KafkaBroker)
	log.Printf("  Topics: source=%s id=%s name=%s continent=%s",
		cfg.SourceTopic, cfg.IDTopic, cfg.NameTopic, cfg.ContinentTopic)
	log.Printf("  Merger Batch Size: %d", cfg.MergerBatchSize)

	return cfg
}

// Helper functions
func getEnvInt(key string, defaultVal int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	intVal, err := strconv.Atoi(val)
	if err != nil {
		log.Printf("Warning: Invalid %s value '%s', using default %d\n", key, val, defaultVal)
		return defaultVal
	}
	return intVal
}

func getEnvString(key, defaultVal string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	return val
}
