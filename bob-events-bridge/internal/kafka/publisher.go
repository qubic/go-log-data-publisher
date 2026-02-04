package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"go.uber.org/zap"
)

// Publisher is the interface for publishing events to Kafka.
type Publisher interface {
	PublishEvent(ctx context.Context, msg *EventMessage) error
	Close() error
}

// Producer is the real Kafka publisher using segmentio/kafka-go.
type Producer struct {
	writer *kafkago.Writer
	logger *zap.Logger
}

// NewProducer creates a new Kafka producer.
func NewProducer(brokers []string, topic string, logger *zap.Logger) *Producer {
	w := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll,
		Compression:  compress.Lz4,
	}

	return &Producer{
		writer: w,
		logger: logger,
	}
}

// PublishEvent serializes an EventMessage to JSON and writes it to Kafka.
// The message key is the tick number so events for the same tick go to the same partition.
func (p *Producer) PublishEvent(ctx context.Context, msg *EventMessage) error {
	value, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal event message: %w", err)
	}

	key := []byte(fmt.Sprintf("%d", msg.TickNumber))

	if err := p.writer.WriteMessages(ctx, kafkago.Message{
		Key:   key,
		Value: value,
	}); err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}

	p.logger.Debug("Published event to Kafka",
		zap.Uint64("logId", msg.LogID),
		zap.Uint32("tick", msg.TickNumber),
		zap.Uint32("type", msg.Type))

	return nil
}

// Close flushes and closes the Kafka writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}
