package config

import (
	"log"
	"os"
	"strconv"
)

// Config holds all environment configuration
type Config struct {
	// Pipeline configuration
	TotalRecords int
	NumWorkers   int
	BatchSize    int

	// Kafka configuration
	KafkaBroker       string
	KafkaBatchSize    int
	KafkaBatchTimeout int // milliseconds

	// Consumer configuration
	ConsumerMinBytes   int
	ConsumerMaxBytes   int
	ConsumerNumWorkers int
	ConsumerBatchSize  int

	// Merger configuration
	MergerBatchSize int
}

// GetConfig loads configuration from environment variables
func GetConfig() *Config {
	cfg := &Config{
		// Pipeline defaults
		TotalRecords: getEnvInt("TOTAL_RECORDS", 50_000_000),
		NumWorkers:   getEnvInt("NUM_WORKERS", 4),
		BatchSize:    getEnvInt("BATCH_SIZE", 10000),

		// Kafka defaults
		KafkaBroker:       getEnvString("KAFKA_BROKER", "localhost:9092"),
		KafkaBatchSize:    getEnvInt("KAFKA_BATCH_SIZE", 5000),
		KafkaBatchTimeout: getEnvInt("KAFKA_BATCH_TIMEOUT", 50),

		// Consumer defaults
		ConsumerMinBytes:   getEnvInt("CONSUMER_MIN_BYTES", 100_000),
		ConsumerMaxBytes:   getEnvInt("CONSUMER_MAX_BYTES", 50_000_000),
		ConsumerNumWorkers: getEnvInt("CONSUMER_NUM_WORKERS", 4),
		ConsumerBatchSize:  getEnvInt("CONSUMER_BATCH_SIZE", 1_000_000),

		// Merger defaults
		MergerBatchSize: getEnvInt("MERGER_BATCH_SIZE", 20000),
	}

	log.Printf("Configuration loaded:")
	log.Printf("  Total Records: %d", cfg.TotalRecords)
	log.Printf("  Pipeline Workers: %d", cfg.NumWorkers)
	log.Printf("  Kafka Broker: %s", cfg.KafkaBroker)
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
