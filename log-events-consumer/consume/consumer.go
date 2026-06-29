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
	"github.com/redis/go-redis/v9"
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

type RedisClient interface {
	Pipeline() redis.Pipeliner
	HGetUint64(ctx context.Context, key, field string) (uint64, error)
	HSet(ctx context.Context, key string, values ...interface{}) (int64, error)
	ZRange(ctx context.Context, z redis.ZRangeArgs) ([]string, error)
}

type TickStore interface {
	AddProcessed(log domain.LogEvent)
	AddSkipped(log domain.LogEvent)
	UpdateTickHeight(ctx context.Context) error
}

type Consumer struct {
	kafkaClient       KafkaClient
	elasticClient     ElasticClient
	tickStore         TickStore
	consumeMetrics    *metrics.Metrics
	supportedLogTypes map[uint64][]int16
	pollInterval      time.Duration
	pollMaxRecords    int
	highestTick       uint32
	currentEpoch      uint32
}

type ConsumerOptions struct {
	SupportedEventLogTypes map[uint64][]int16
	PollInterval           time.Duration
	PollMaxRecords         int
}

func NewConsumer(kafkaClient KafkaClient, elasticClient ElasticClient, tickStore TickStore, metrics *metrics.Metrics, consumerOptions ConsumerOptions) *Consumer {
	return &Consumer{
		kafkaClient:       kafkaClient,
		elasticClient:     elasticClient,
		tickStore:         tickStore,
		consumeMetrics:    metrics,
		supportedLogTypes: consumerOptions.SupportedEventLogTypes,
		pollInterval:      consumerOptions.PollInterval,
		pollMaxRecords:    consumerOptions.PollMaxRecords,
	}
}

func (c *Consumer) Consume(ctx context.Context) error {
	ticker := time.Tick(c.pollInterval)
	for range ticker {

		select {
		case <-ctx.Done():
			log.Println("Shutdown signal received, stopping consumer.")
			return nil

		default:
			records, docs, err := c.consumeBatch(ctx)
			if err != nil {
				return fmt.Errorf("consuming batch: %w", err)
			}
			if records > 0 {
				log.Printf("Processed: [%d]. Indexed: [%d]. Highest tick: [%d]", records, docs, c.highestTick)
			}

		}
	}
	return nil
}

func (c *Consumer) consumeBatch(ctx context.Context) (int, int, error) {
	defer c.kafkaClient.AllowRebalance()
	fetches := c.kafkaClient.PollRecords(ctx, c.pollMaxRecords)
	if errors := fetches.Errors(); len(errors) > 0 {
		for _, err := range errors {
			log.Printf("Fetches error: %v", err)
		}
		return -1, -1, fmt.Errorf("fetching records")
	}

	var recordCount int
	var documents []*elastic.EsDocument
	iter := fetches.RecordIter()
	for !iter.Done() {
		record := iter.Next()
		recordCount++

		var raw domain.LogEventPtr
		err := unmarshallLogEvent(record, &raw)
		if err != nil {
			return -1, -1, fmt.Errorf("unmarshalling raw log event [%s]: %w", string(record.Value), err)
		}

		logEvent, err := raw.ToLogEvent()
		if err != nil {
			log.Printf("[ERROR] converting raw log event [%+v]: %v", raw, err)
			return -1, -1, fmt.Errorf("converting to log event: %w", err)
		}

		// basic filters before elastic conversion
		if !logEvent.IsSupported(c.supportedLogTypes) {
			c.tickStore.AddSkipped(logEvent)
			continue
		}

		logEventElastic, err := logEvent.ToLogEventElastic()
		if err != nil {
			return -1, -1, fmt.Errorf("converting to elastic format [%s]: %w", string(record.Value), err)
		}

		// filter event logs that need conversion
		if !logEventElastic.IsSupported() {
			c.tickStore.AddSkipped(logEvent)
			continue
		}

		val, err := json.Marshal(logEventElastic)
		if err != nil {
			return -1, -1, fmt.Errorf("marshalling log event [value=%v]: %w", logEvent, err)
		}

		// Use separator to prevent ID collisions (e.g., epoch=1,logId=23 vs epoch=12,logId=3).
		documents = append(documents, &elastic.EsDocument{
			Id:      strconv.FormatUint(uint64(logEvent.Epoch), 10) + "-" + strconv.FormatUint(logEvent.LogId, 10),
			Payload: val,
		})

		// we know that tick number cannot exceed uint32 according to current core code
		if uint32(logEvent.TickNumber) > c.highestTick {
			c.highestTick = uint32(logEvent.TickNumber)
			c.currentEpoch = logEvent.Epoch
		}
		c.consumeMetrics.IncProcessedMessages()
		c.tickStore.AddProcessed(logEvent)
	}

	if len(documents) > 0 { // only try to index if there are documents to index
		err := c.elasticClient.BulkIndex(ctx, documents)
		if err != nil {
			return -1, -1, fmt.Errorf("bulk indexing [%d] documents: %w", len(documents), err)
		}
	}
	c.consumeMetrics.SetProcessedTick(c.currentEpoch, c.highestTick)

	err := c.kafkaClient.CommitUncommittedOffsets(ctx)
	if err != nil {
		return -1, -1, fmt.Errorf("committing offsets: %w", err)
	}

	err = c.tickStore.UpdateTickHeight(ctx) // for processing status
	if err != nil {
		// maybe we want only log here
		return -1, -1, fmt.Errorf("updating tick height: %w", err)
	}
	return recordCount, len(documents), nil
}

func unmarshallLogEvent(record *kgo.Record, target any) error {
	err := json.Unmarshal(record.Value, target)
	if err != nil {
		return fmt.Errorf("unmarshalling kafka record: %w", err)
	}
	return nil
}
