package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type Event struct {
	Event  string `json:"event"`
	UserID int    `json:"user_id"`
}

const (
	maxRetries              = 3
	dbTimeout               = 2 * time.Second
	initialBackoff          = 1 * time.Second
	gracefulShutdownTimeout = 30 * time.Second
)

func insertEventWithRetry(ctx context.Context, db *sql.DB, logger *slog.Logger, event Event, payload []byte) error {
	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, dbTimeout)

		_, err := db.ExecContext(ctxWithTimeout, "INSERT INTO events (type, payload) VALUES ($1, $2)",
			event.Event, payload)
		cancel()

		if err == nil {
			logger.Info("event stored", "event", event.Event, "user_id", event.UserID, "attempt", attempt)
			return nil
		}

		logger.Error("insert failed", "error", err, "event", event.Event, "user_id", event.UserID, "attempt", attempt)

		if attempt == maxRetries {
			logger.Error("insert failed after all retries", "event", event.Event, "user_id", event.UserID, "max_attempts", maxRetries)
			return err
		}

		backoffDuration := time.Duration(math.Pow(2, float64(attempt-1))) * initialBackoff
		logger.Info("retrying insert", "event", event.Event, "user_id", event.UserID, "attempt", attempt, "backoff_ms", backoffDuration.Milliseconds())

		time.Sleep(backoffDuration)
	}

	return nil
}

func main() {
	logger := slog.Default()
	db, err := sql.Open(
		"postgres",
		"host=localhost port=5432 user=postgres password=postgres dbname=eventsdb sslmode=disable",
	)
	if err != nil {
		logger.Error("database unavailable", "error", err)
		return
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	defer db.Close()

	logger.Info("database connected")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       "ad-events",
		GroupID:     "ad-consumer-group-v2",
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	logger.Info("kafka reader initialized")

	shutdownSignal := make(chan os.Signal, 1)

	signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopProcessing := make(chan struct{})

	go func() {
		sig := <-shutdownSignal
		logger.Info("shutdown signal received", "signal", sig.String())

		// Signal main loop to stop accepting new messages
		close(stopProcessing)
	}()

	for {
		select {
		case <-stopProcessing:
			logger.Info("stopping message processing, waiting for current message to finish")
			wg.Wait()
			logger.Info("all messages processed, starting graceful shutdown")
			goto shutdown

		default:
			message, err := reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					logger.Info("message fetch cancelled due to shutdown")
					goto shutdown
				}
				logger.Error("failed to read message", "error", err)
				continue
			}

			wg.Add(1)

			var event Event
			err = json.Unmarshal(message.Value, &event)
			if err != nil {
				logger.Error("invalid payload", "error", err, "payload", string(message.Value))
				reader.CommitMessages(ctx, message)
				wg.Done()
				continue
			}

			err = insertEventWithRetry(ctx, db, logger, event, message.Value)
			if err != nil {
				logger.Error("skipping event after failed retries", "event", event.Event, "user_id", event.UserID, "offset", message.Offset)
			}

			err = reader.CommitMessages(ctx, message)
			if err != nil {
				logger.Error("failed to commit message offset", "error", err, "offset", message.Offset)
			}
			wg.Done()
		}
	}

shutdown:
	logger.Info("starting graceful shutdown", "timeout_seconds", gracefulShutdownTimeout.Seconds())

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
	defer shutdownCancel()

	shutdownDone := make(chan struct{})

	go func() {
		wg.Wait()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		logger.Info("graceful shutdown completed successfully")
	case <-shutdownCtx.Done():
		logger.Error("graceful shutdown timeout exceeded", "timeout_seconds", gracefulShutdownTimeout.Seconds())
		os.Exit(1)
	}

	if err := reader.Close(); err != nil {
		logger.Error("error closing kafka reader", "error", err)
	} else {
		logger.Info("kafka reader closed")
	}

	if err := db.Close(); err != nil {
		logger.Error("error closing database", "error", err)
	} else {
		logger.Info("database connection closed")
	}

	logger.Info("consumer shutdown complete, exiting")
	os.Exit(0)
}
