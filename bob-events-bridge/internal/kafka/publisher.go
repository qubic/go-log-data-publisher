package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/qubic/bob-events-bridge/internal/metrics"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"go.uber.org/zap"
)

// Publisher is the interface for publishing events to Kafka.
type Publisher interface {
	PublishEvent(ctx context.Context, msg *EventMessage) error
	PublishEvents(ctx context.Context, msgs []*EventMessage) error
	Close() error
}

// Producer is the real Kafka publisher using segmentio/kafka-go.
type Producer struct {
	writer  *kafkago.Writer
	logger  *zap.Logger
	metrics *metrics.BridgeMetrics
}

// NewProducer creates a new Kafka producer.
func NewProducer(brokers []string, topic string, logger *zap.Logger, metrics *metrics.BridgeMetrics) *Producer {
	w := &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireAll,
		Compression:            compress.Lz4,
		AllowAutoTopicCreation: false,
	}

	return &Producer{
		writer:  w,
		logger:  logger,
		metrics: metrics,
	}
}

// PublishEvent serializes an EventMessage to JSON and writes it to Kafka.
// The message key is the tick number so events for the same tick go to the same partition.
func (p *Producer) PublishEvent(ctx context.Context, msg *EventMessage) error {
	value, err := json.Marshal(msg)
	if err != nil {
		p.metrics.IncKafkaPublishErrors("marshal_error")
		return fmt.Errorf("failed to marshal event message: %w", err)
	}

	key := []byte(fmt.Sprintf("%d", msg.TickNumber))

	if err := p.writer.WriteMessages(ctx, kafkago.Message{
		Key:   key,
		Value: value,
	}); err != nil {
		p.metrics.IncKafkaPublishErrors("publish_error")
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}

	p.metrics.IncKafkaMessagesPublished(msg.Type, p.writer.Topic)

	p.logger.Debug("Published event to Kafka",
		zap.Uint64("logId", msg.LogID),
		zap.Uint32("tick", msg.TickNumber),
		zap.Uint32("type", msg.Type))

	return nil
}

// PublishEvents serializes multiple EventMessages and writes them to Kafka in a single batch call.
func (p *Producer) PublishEvents(ctx context.Context, msgs []*EventMessage) error {
	if len(msgs) == 0 {
		return nil
	}

	kafkaMsgs := make([]kafkago.Message, 0, len(msgs))
	for _, msg := range msgs {
		value, err := json.Marshal(msg)
		if err != nil {
			p.metrics.IncKafkaPublishErrors("marshal_error")
			return fmt.Errorf("failed to marshal event message: %w", err)
		}
		key := []byte(fmt.Sprintf("%d", msg.TickNumber))
		kafkaMsgs = append(kafkaMsgs, kafkago.Message{
			Key:   key,
			Value: value,
		})
	}

	if err := p.writer.WriteMessages(ctx, kafkaMsgs...); err != nil {
		p.metrics.IncKafkaPublishErrors("publish_error")
		return fmt.Errorf("failed to write messages to kafka: %w", err)
	}

	for _, msg := range msgs {
		p.metrics.IncKafkaMessagesPublished(msg.Type, p.writer.Topic)
	}

	p.logger.Debug("Published batch to Kafka",
		zap.Int("count", len(msgs)),
		zap.Uint32("tick", msgs[0].TickNumber))

	return nil
}

// Close flushes and closes the Kafka writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}
