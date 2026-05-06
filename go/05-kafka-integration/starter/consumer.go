package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Event struct {
	Event  string `json:"event"`
	UserID int    `json:"user_id"`
}

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       "ad-events",
		GroupID:     "ad-consumer-group",
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println("error : failed to read message")
			continue
		}

		var event Event

		err = json.Unmarshal(message.Value, &event)
		if err != nil {
			fmt.Println("error: invalid event")
			continue
		}
		fmt.Printf(
			"received event %s user=%d\n",
			event.Event,
			event.UserID,
		)
	}
}
