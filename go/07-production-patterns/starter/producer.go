package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type AdEvent struct {
	Event  string `json:"event"`
	UserID int    `json:"user_id"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "ad-events",
	})
	defer writer.Close()

	event := AdEvent{
		Event:  "ad_click",
		UserID: 2,
	}

	data, err := json.Marshal(event)
	if err != nil {
		logger.Error("invalid event", "error", err)
		return
	}

	maxRetries := 3

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := writer.WriteMessages(
			context.Background(),
			kafka.Message{
				Value: data,
			},
		)

		if err == nil {
			logger.Info("event sent")
			return
		}

		logger.Error("kafka unavailable",
			"retry", attempt,
			"error", err,
		)

		time.Sleep(2 * time.Second)
	}

	logger.Error("failed to send event")
}
