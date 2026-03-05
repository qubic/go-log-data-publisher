package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
	"go.uber.org/zap"
)

// Publisher is the interface for publishing events to Kafka.
type Publisher interface {
	PublishEvent(ctx context.Context, msg *EventMessage) error
	PublishEvents(ctx context.Context, msgs []*EventMessage) error
	Close() error
}

// Producer is the real Kafka publisher using franz-go.
type Producer struct {
	client *kgo.Client
	logger *zap.Logger
}

// NewProducer creates a new Kafka producer using franz-go with kprom metrics.
func NewProducer(brokers []string, topic string, logger *zap.Logger, registerer prometheus.Registerer, gatherer prometheus.Gatherer, metricsNamespace string) (*Producer, error) {
	kpromMetrics := kprom.NewMetrics(metricsNamespace,
		kprom.Registerer(registerer),
		kprom.Gatherer(gatherer),
	)

	client, err := kgo.NewClient(
		kgo.WithHooks(kpromMetrics),
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerLinger(0),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
	)
	if err != nil {
		return nil, fmt.Errorf("creating franz-go client: %w", err)
	}

	return &Producer{
		client: client,
		logger: logger,
	}, nil
}

// PublishEvent serializes an EventMessage to JSON and writes it to Kafka.
// The message key is the tick number so events for the same tick go to the same partition.
func (p *Producer) PublishEvent(ctx context.Context, msg *EventMessage) error {
	value, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal event message: %w", err)
	}

	key := []byte(fmt.Sprintf("%d", msg.TickNumber))

	results := p.client.ProduceSync(ctx, &kgo.Record{
		Key:   key,
		Value: value,
	})

	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}

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

	records := make([]*kgo.Record, 0, len(msgs))
	for _, msg := range msgs {
		value, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("failed to marshal event message: %w", err)
		}
		key := []byte(fmt.Sprintf("%d", msg.TickNumber))
		records = append(records, &kgo.Record{
			Key:   key,
			Value: value,
		})
	}

	results := p.client.ProduceSync(ctx, records...)

	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("failed to write messages to kafka: %w", err)
	}

	p.logger.Debug("Published batch to Kafka",
		zap.Int("count", len(msgs)),
		zap.Uint32("tick", msgs[0].TickNumber))

	return nil
}

// Close closes the franz-go client.
func (p *Producer) Close() error {
	p.client.Close()
	return nil
}
