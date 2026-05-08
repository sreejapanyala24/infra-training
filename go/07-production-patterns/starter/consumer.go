package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"kafka-integration/go/07-production_patterns/solution"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type Event struct {
	Event  string `json:"event"`
	UserID int    `json:"user_id"`
}

func main() {
	logger := slog.Default()
	cfg := solution.LoadConfig()

	// Database
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		logger.Error("database error", "error", err)
		os.Exit(1)
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	defer db.Close()

	logger.Info("database connected")

	// Kafka reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       cfg.KafkaTopic,
		GroupID:     cfg.KafkaGroupID,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	logger.Info("kafka reader initialized")

	// DLQ writer
	dlqWriter, err := solution.NewDLQWriter(cfg.KafkaBrokers, cfg.DLQTopic, logger)
	if err != nil {
		logger.Error("failed to create DLQ writer", "error", err)
		os.Exit(1)
	}
	defer dlqWriter.Close()

	// Signal handling
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopProcessing := make(chan struct{})
	processed := &atomic.Int64{}

	// Signal handler
	go func() {
		sig := <-shutdownSignal
		logger.Info("shutdown signal received", "signal", sig.String())
		close(stopProcessing)
	}()

	// Consumer lag monitor
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stats := reader.Stats()
				logger.Info("consumer lag", "lag", stats.Lag, "processed", processed.Load())
			}
		}
	}()

	// Message processing loop
	for {
		select {
		case <-stopProcessing:
			logger.Info("waiting for in-flight messages")
			wg.Wait()
			goto shutdown

		default:
			message, err := reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					goto shutdown
				}
				if solution.IsTransientKafkaError(err) {
					logger.Error("transient kafka error", "error", err)
					time.Sleep(1 * time.Second)
				}
				continue
			}

			wg.Add(1)

			var event Event
			err = json.Unmarshal(message.Value, &event)
			if err != nil {
				logger.Error("invalid payload", "error", err)
				dlqWriter.Send(ctx, message, message.Offset, "invalid_json")
				reader.CommitMessages(ctx, message)
				wg.Done()
				continue
			}

			// Insert with retry
			err = insertWithRetry(ctx, db, logger, event, message.Value)
			if err != nil {
				logger.Error("max retries exceeded, sending to DLQ", "offset", message.Offset)
				dlqWriter.Send(ctx, message, message.Offset, "max_retries_exceeded")
			}

			reader.CommitMessages(ctx, message)
			if err == nil {
				processed.Add(1)
			}
			wg.Done()
		}
	}

shutdown:
	logger.Info("graceful shutdown", "timeout_seconds", cfg.ShutdownTimeout.Seconds())

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()

	shutdownDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		logger.Info("shutdown complete", "total_processed", processed.Load())
	case <-shutdownCtx.Done():
		logger.Error("shutdown timeout exceeded")
		os.Exit(1)
	}

	logger.Info("exiting")
	os.Exit(0)
}

// insertWithRetry inserts event with exponential backoff
func insertWithRetry(ctx context.Context, db *sql.DB, logger *slog.Logger, event Event, payload []byte) error {
	for attempt := 1; attempt <= 3; attempt++ {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)

		_, err := db.ExecContext(ctxWithTimeout, "INSERT INTO events (type, payload) VALUES ($1, $2)", event.Event, payload)
		cancel()

		if err == nil {
			logger.Info("event stored", "event", event.Event, "user_id", event.UserID)
			return nil
		}

		if !solution.IsTransientError(err) {
			logger.Error("permanent error", "error", err)
			return err
		}

		if attempt < 3 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * time.Second
			logger.Info("retrying", "attempt", attempt, "backoff_ms", backoff.Milliseconds())
			time.Sleep(backoff)
		}
	}

	return nil
}
