package main

import (
	"context"
	"encoding/json"
	"kafka-integration/go/07-production_patterns/solution"
	"log/slog"
	"math"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type ProducerEvent struct {
	Event  string `json:"event"`
	UserID int    `json:"user_id"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}

	brokers := strings.Split(kafkaBrokers, ",")

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Topic:   "ad-events",
	})
	defer writer.Close()

	event := ProducerEvent{Event: "ad_click", UserID: 2}
	data, err := json.Marshal(event)
	if err != nil {
		logger.Error("failed to marshal", "error", err)
		os.Exit(1)
	}

	// Retry with exponential backoff
	for attempt := 1; attempt <= 3; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := writer.WriteMessages(ctx, kafka.Message{Value: data})
		cancel()

		if err == nil {
			logger.Info("event sent", "event", event.Event, "user_id", event.UserID)
			os.Exit(0)
		}

		// Fail fast on permanent errors
		if !solution.IsTransientKafkaError(err) {
			logger.Error("permanent error, not retrying", "error", err)
			os.Exit(1)
		}

		if attempt < 3 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * time.Second
			logger.Error("transient error, retrying", "error", err, "attempt", attempt, "backoff_ms", backoff.Milliseconds())
			time.Sleep(backoff)
		}
	}

	logger.Error("failed after all retries")
	os.Exit(1)
}
