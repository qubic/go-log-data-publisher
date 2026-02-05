package consume

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/qubic/log-events-consumer/domain"
	"github.com/qubic/log-events-consumer/elastic"
	"github.com/qubic/log-events-consumer/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaClient interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	CommitUncommittedOffsets(ctx context.Context) error
	AllowRebalance()
}

type ElasticClient interface {
	BulkIndex(ctx context.Context, data []*elastic.EsDocument) error
}

type Consumer struct {
	kafkaClient    KafkaClient
	elasticClient  ElasticClient
	consumeMetrics *metrics.Metrics
	currentTick    uint32
	currentEpoch   uint32
}

func NewConsumer(kafkaClient KafkaClient, elasticClient ElasticClient, metrics *metrics.Metrics) *Consumer {
	return &Consumer{
		kafkaClient:    kafkaClient,
		elasticClient:  elasticClient,
		consumeMetrics: metrics,
	}
}

func (c *Consumer) Consume(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Println("Shutdown signal received, stopping consumer.")
			return nil

		default:
			count, err := c.consumeBatch(ctx)
			if err != nil {
				return fmt.Errorf("consuming batch: %w", err)
			}
			log.Printf("Processed [%d] records. Tick: [%d]", count, c.currentTick)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (c *Consumer) consumeBatch(ctx context.Context) (int, error) {
	defer c.kafkaClient.AllowRebalance()
	fetches := c.kafkaClient.PollRecords(ctx, 10000)
	if errors := fetches.Errors(); len(errors) > 0 {
		for _, err := range errors {
			log.Printf("Fetches error: %v", err)
		}
		return -1, fmt.Errorf("fetching records")
	}

	var documents []*elastic.EsDocument
	iter := fetches.RecordIter()
	for !iter.Done() {
		record := iter.Next()

		var logEvent domain.LogEvent

		err := unmarshallLogEvent(record, &logEvent)
		if err != nil {
			return -1, fmt.Errorf("unmarshalling log event [value=%s]: %w", string(record.Value), err)
		}

		logEventElastic, err := domain.LogEventToElastic(logEvent)
		if err != nil {
			return -1, fmt.Errorf("converting log event from kafka to elastic format [value=%s]: %w", string(record.Value), err)
		}

		val, err := json.Marshal(logEventElastic)
		if err != nil {
			return -1, fmt.Errorf("marshalling log event [value=%v]: %w", logEvent, err)
		}

		// Check for potential overflow when converting uint64 to int
		if logEvent.LogId > math.MaxInt {
			return -1, fmt.Errorf("logId %d exceeds maximum int value (%d), cannot convert to document ID", logEvent.LogId, math.MaxInt)
		}

		documents = append(documents, &elastic.EsDocument{
			Id:      strconv.Itoa(int(logEvent.LogId)),
			Payload: val,
		})

		if logEvent.TickNumber > c.currentTick {
			c.currentTick = logEvent.TickNumber
			c.currentEpoch = logEvent.Epoch
		}
		c.consumeMetrics.IncProcessedMessages()
	}

	err := c.elasticClient.BulkIndex(ctx, documents)
	if err != nil {
		return -1, fmt.Errorf("bulk indexing [%d] documents: %w", len(documents), err)
	}
	c.consumeMetrics.SetProcessedTick(c.currentEpoch, c.currentTick)

	err = c.kafkaClient.CommitUncommittedOffsets(ctx)
	if err != nil {
		return -1, fmt.Errorf("committing offsets: %w", err)
	}

	return len(documents), nil
}

func unmarshallLogEvent(record *kgo.Record, logEvent *domain.LogEvent) error {

	err := json.Unmarshal(record.Value, &logEvent)
	if err != nil {
		return fmt.Errorf("unmarshalling kafka record: %w", err)
	}
	return nil
}
