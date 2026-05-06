package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type AdEvent struct {
	Event  string `json:"event"`
	UserID int    `json:"user_id"`
}

func main() {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "ad-events",
	})
	defer writer.Close()

	event := AdEvent{
		Event:  "ad_click",
		UserID: 123,
	}

	data, err := json.Marshal(event)
	if err != nil {
		fmt.Println("error: invalid event")
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
			fmt.Println("event sent")
			return
		}

		fmt.Printf("error: kafka unavailable retry=%d\n", attempt)
		time.Sleep(2 * time.Second)
	}

	fmt.Println("error: failed to send event")
}
