package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type Event struct {
	Event  string `json:"event"`
	UserID int    `json:"user_id"`
}

func main() {
	db, err := sql.Open(
		"postgres",
		"host=localhost port=5432 user=postgres password=postgres dbname=eventsdb sslmode=disable",
	)
	if err != nil {
		fmt.Println("error: database unavailable")
		return
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	defer db.Close()
	
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
			fmt.Println("error: invalid payload")
			continue
		}
		_, err = db.Exec(
			"INSERT INTO events (type, payload) VALUES ($1, $2)",
			event.Event,
			message.Value,
		)

		if err != nil {
			fmt.Println("error: failed to insert event")
			continue
		}
		fmt.Printf(
			"stored event %s user=%d\n",
			event.Event,
			event.UserID,
		)
	}
}
