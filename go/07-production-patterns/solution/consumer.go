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
	cfg := LoadConfig()

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
		Brokers:        cfg.KafkaBrokers,
		Topic:          cfg.KafkaTopic,
		GroupID:        cfg.KafkaGroupID,
		StartOffset:    kafka.LastOffset,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	defer reader.Close()

	logger.Info("kafka reader initialized")

	// DLQ writer
	dlqWriter, err := NewDLQWriter(cfg.KafkaBrokers, cfg.DLQTopic, logger)
	if err != nil {
		logger.Error("failed to create DLQ writer", "error", err)
		os.Exit(1)
	}
	defer dlqWriter.Close()

	logger.Info("kafka DLQ writer initialized")

	// Shutdown coordination
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	processed := &atomic.Int64{}
	shutdown := &atomic.Bool{}

	// Consumer lag monitor
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("lag monitor started")
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if shutdown.Load() {
					logger.Info("lag monitor shutting down")
					return
				}
				stats := reader.Stats()
				logger.Info("consumer lag", "lag", stats.Lag, "processed", processed.Load())
			}
		}
	}()

	// Signal handler
	wg.Add(1)
	go func() {
		defer wg.Done()
		sig := <-shutdownSignal
		logger.Info("shutdown signal received", "signal", sig.String())
		shutdown.Store(true)
	}()

	// Message processing loop
	logger.Info("message processing started")
	for {
		if shutdown.Load() {
			logger.Info("shutdown requested, stopping message processing")
			break
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		message, err := reader.FetchMessage(ctx)
		cancel()

		if err != nil {
			if err == context.DeadlineExceeded {
				continue
			}
			if IsTransientKafkaError(err) {
				logger.Error("transient kafka error", "error", err)
				time.Sleep(1 * time.Second)
				continue
			}
			logger.Error("kafka error", "error", err)
			continue
		}

		if shutdown.Load() {
			break
		}

		logger.Info("processing message", "offset", message.Offset)

		wg.Add(1)

		var event Event
		err = json.Unmarshal(message.Value, &event)
		if err != nil {
			logger.Error("invalid payload", "error", err)
			dlqWriter.Send(context.Background(), message, message.Offset, "invalid_json")
			reader.CommitMessages(context.Background(), message)
			logger.Info("committed invalid message", "offset", message.Offset)
			wg.Done()
			continue
		}

		logger.Info("parsed event", "event", event.Event, "user_id", event.UserID)

		// Insert with retry - ONLY commit if success
		//insertErr := insertWithRetry(context.Background(), db, logger, event, message.Value)
		insertErr := insertWithRetry(context.Background(), db, logger, event, message.Value, message)

		if insertErr != nil {
			logger.Error("failed to insert, sending to DLQ", "offset", message.Offset, "error", insertErr)
			dlqWriter.Send(context.Background(), message, message.Offset, "max_retries_exceeded")
			// Commit only after DLQ send
			reader.CommitMessages(context.Background(), message)
			logger.Info("committed failed message to DLQ", "offset", message.Offset)
		} else {
			logger.Info("event stored successfully", "event", event.Event, "user_id", event.UserID)
			// Commit ONLY after successful insert
			reader.CommitMessages(context.Background(), message)
			logger.Info("committed message", "offset", message.Offset)
			processed.Add(1)
		}

		wg.Done()
	}

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
func insertWithRetry(ctx context.Context, db *sql.DB, logger *slog.Logger, event Event, rawPayload []byte, message kafka.Message) error {
	//func insertWithRetry(ctx context.Context, db *sql.DB, logger *slog.Logger, event Event, payload []byte) error {
	var insertErr error

	for attempt := 1; attempt <= 3; attempt++ {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
		//_, insertErr = //db.ExecContext(ctxWithTimeout, "INSERT INTO events (type, payload) VALUES ($1, $2)", event.Event, payload)
		_, insertErr = db.ExecContext(ctxWithTimeout,
			`INSERT INTO events (type,payload,kafka_topic,kafka_partition,kafka_offset)VALUES ($1, $2, $3, $4, $5)ON CONFLICT (kafka_topic, kafka_partition, kafka_offset)DO NOTHING`,
			event.Event,
			string(rawPayload),
			message.Topic,
			message.Partition,
			message.Offset,
		)

		cancel()

		if insertErr == nil {
			logger.Info("event inserted", "event", event.Event, "attempt", attempt)
			return nil
		}

		if !IsTransientError(insertErr) {
			logger.Error("permanent error", "error", insertErr)
			return insertErr
		}

		if attempt < 3 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * time.Second
			logger.Info("retrying", "attempt", attempt, "backoff_ms", backoff.Milliseconds())
			time.Sleep(backoff)
		}
	}

	return insertErr
}
