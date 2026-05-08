package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type DLQWriter struct {
	writer *kafka.Writer
	logger *slog.Logger
}

func NewDLQWriter(brokers []string, topic string, logger *slog.Logger) (*DLQWriter, error) {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Topic:   topic,
	})
	return &DLQWriter{writer: writer, logger: logger}, nil
}

func (d *DLQWriter) Send(ctx context.Context, message kafka.Message, offset int64, reason string) error {
	dlqMsg := kafka.Message{
		Value: message.Value,
		Headers: []kafka.Header{
			{Key: "original_offset", Value: []byte(string(rune(offset)))},
			{Key: "failure_reason", Value: []byte(reason)},
			{Key: "timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	}

	err := d.writer.WriteMessages(ctx, dlqMsg)
	if err != nil {
		d.logger.Error("failed to send to DLQ", "error", err, "offset", offset, "reason", reason)
		return err
	}
	d.logger.Info("message sent to DLQ", "offset", offset, "reason", reason)
	return nil
}

func (d *DLQWriter) Close() error {
	if d.writer == nil {
		return nil
	}
	return d.writer.Close()
}
