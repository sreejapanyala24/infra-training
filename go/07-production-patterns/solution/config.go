package main

import (
	"os"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL     string
	KafkaBrokers    []string
	KafkaTopic      string
	KafkaGroupID    string
	DLQTopic        string
	ShutdownTimeout time.Duration
}

func LoadConfig() Config {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "host=localhost port=5432 user=postgres password=postgres dbname=eventsdb sslmode=disable"
	}

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}

	dlqTopic := os.Getenv("DLQ_TOPIC")
	if dlqTopic == "" {
		dlqTopic = "ad-events-dlq"
	}

	return Config{
		DatabaseURL:     dbURL,
		KafkaBrokers:    strings.Split(kafkaBrokers, ","),
		KafkaTopic:      "ad-events",
		KafkaGroupID:    "ad-consumer-group-v5",
		DLQTopic:        dlqTopic,
		ShutdownTimeout: 5 * time.Second,
	}
}
