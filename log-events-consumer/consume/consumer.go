package consume

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	ticker := time.Tick(100 * time.Millisecond)
	for range ticker {

		select {
		case <-ctx.Done():
			log.Println("Shutdown signal received, stopping consumer.")
			return nil

		default:
			count, err := c.consumeBatch(ctx)
			if err != nil {
				return fmt.Errorf("consuming batch: %w", err)
			}
			if count > 0 {
				log.Printf("Processed [%d] records. Tick: [%d]", count, c.currentTick)
			}

		}
	}
	return nil
}

func (c *Consumer) consumeBatch(ctx context.Context) (int, error) {
	defer c.kafkaClient.AllowRebalance()
	fetches := c.kafkaClient.PollRecords(ctx, 1000)
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

		var raw domain.LogEventPtr
		err := unmarshallLogEvent(record, &raw)
		if err != nil {
			return -1, fmt.Errorf("unmarshalling raw log event [%s]: %w", string(record.Value), err)
		}

		logEvent, err := raw.ToLogEvent()
		if err != nil {
			return -1, fmt.Errorf("converting to log event: %w", err)
		}

		// basic filters before elastic conversion
		if !logEvent.IsSupported() {
			continue
		}

		logEventElastic, err := logEvent.ToLogEventElastic()
		if err != nil {
			return -1, fmt.Errorf("converting to elastic format [%s]: %w", string(record.Value), err)
		}

		// filter event logs that need conversion
		if !logEventElastic.IsSupported() {
			continue
		}

		val, err := json.Marshal(logEventElastic)
		if err != nil {
			return -1, fmt.Errorf("marshalling log event [value=%v]: %w", logEvent, err)
		}

		// Use separator to prevent ID collisions (e.g., epoch=1,logId=23 vs epoch=12,logId=3).
		documents = append(documents, &elastic.EsDocument{
			Id:      strconv.FormatUint(uint64(logEvent.Epoch), 10) + "-" + strconv.FormatUint(logEvent.LogId, 10),
			Payload: val,
		})

		// we know that tick number cannot exceed uint32 according to current core code
		if uint32(logEvent.TickNumber) > c.currentTick {
			c.currentTick = uint32(logEvent.TickNumber)
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

func unmarshallLogEvent(record *kgo.Record, target any) error {
	err := json.Unmarshal(record.Value, target)
	if err != nil {
		return fmt.Errorf("unmarshalling kafka record: %w", err)
	}
	return nil
}
