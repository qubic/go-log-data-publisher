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
	highestTick       uint32
	currentEpoch      uint32
}

func NewConsumer(kafkaClient KafkaClient, elasticClient ElasticClient, tickStore TickStore, metrics *metrics.Metrics, supportedEventLogTypes map[uint64][]int16) *Consumer {
	return &Consumer{
		kafkaClient:       kafkaClient,
		elasticClient:     elasticClient,
		tickStore:         tickStore,
		consumeMetrics:    metrics,
		supportedLogTypes: supportedEventLogTypes,
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
				log.Printf("Ingested [%d] records. Highest tick: [%d]", count, c.highestTick)
			}

		}
	}
	return nil
}

func (c *Consumer) consumeBatch(ctx context.Context) (int, error) {
	var err error
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
			log.Printf("[ERROR] converting raw log event [%v]: %v", raw, err)
			return -1, fmt.Errorf("converting to log event: %w", err)
		}

		// basic filters before elastic conversion
		if !logEvent.IsSupported(c.supportedLogTypes) {
			c.tickStore.AddSkipped(logEvent)
			continue
		}

		logEventElastic, err := logEvent.ToLogEventElastic()
		if err != nil {
			return -1, fmt.Errorf("converting to elastic format [%s]: %w", string(record.Value), err)
		}

		// filter event logs that need conversion
		if !logEventElastic.IsSupported() {
			c.tickStore.AddSkipped(logEvent)
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
		if uint32(logEvent.TickNumber) > c.highestTick {
			c.highestTick = uint32(logEvent.TickNumber)
			c.currentEpoch = logEvent.Epoch
		}
		c.consumeMetrics.IncProcessedMessages()
		c.tickStore.AddProcessed(logEvent)
	}

	if len(documents) > 0 { // only try to index if there are documents to index
		err = c.elasticClient.BulkIndex(ctx, documents)
		if err != nil {
			return -1, fmt.Errorf("bulk indexing [%d] documents: %w", len(documents), err)
		}
	}
	c.consumeMetrics.SetProcessedTick(c.currentEpoch, c.highestTick)

	err = c.kafkaClient.CommitUncommittedOffsets(ctx)
	if err != nil {
		return -1, fmt.Errorf("committing offsets: %w", err)
	}

	err = c.tickStore.UpdateTickHeight(ctx)
	if err != nil {
		// maybe we want only log here
		return -1, fmt.Errorf("updating redis processing status: %w", err)
	}
	return len(documents), nil
}

//const KeyHighestTick = "tick:highest"
//const KeyTicksProcessed = "ticks:processed"
//const KeyTickWithNumber = "tick:%d"
//
//// TODO move code into redis store
//func (c *Consumer) updateRedisProcessingStatus(ctx context.Context) error {
//
//	// get highest processed tick
//	storedCompletedTickNumber, err := c.redisClient.HGetUint64(ctx, KeyHighestTick, "tickNumber")
//	if err != nil {
//		return fmt.Errorf("fetching highest tick number: %w", err)
//	}
//
//	// update in redis
//	pipe := c.redisClient.Pipeline()
//	batch := c.accumulator.Drain()
//	for tickNumber, status := range batch {
//		if tickNumber > storedCompletedTickNumber { // otherwise irrelevant
//			// add to ticks so that we can find it again later
//			pipe.ZAdd(ctx, KeyTicksProcessed, redis.Z{Score: float64(tickNumber), Member: tickNumber})
//			// update tick status
//			key := tickKey(tickNumber)
//			pipe.HIncrBy(ctx, key, "processed", status.Processed)
//			pipe.HIncrBy(ctx, key, "skipped", status.Processed)
//			if status.Total > 0 {
//				pipe.HSet(ctx, key, "total", status.Total)
//			}
//			log.Printf("[DEBUG] tick %d: processed %d, skipped %d, total %d", tickNumber, status.Processed, status.Skipped, status.Total)
//		}
//	}
//	if _, err := pipe.Exec(ctx); err != nil {
//		return fmt.Errorf("flushing update pipeline: %w", err)
//	}
//
//	// read from redis to calculate newest highest tick
//	pipe = c.redisClient.Pipeline()
//	tickNumbers := make([]uint64, 0, len(batch))
//	for tickNumber := range batch {
//		if tickNumber > storedCompletedTickNumber {
//			tickNumbers = append(tickNumbers, tickNumber)
//			pipe.HMGet(ctx, tickKey(tickNumber), "total", "processed", "skipped")
//		}
//	}
//	commands, err := pipe.Exec(ctx)
//	if err != nil {
//		return fmt.Errorf("flushing readback pipeline: %w", err)
//	}
//
//	var newHighestTickNumber uint64
//	var newHighestCount int64
//
//	for i, tickNumber := range tickNumbers {
//		vals := commands[i].(*redis.SliceCmd).Val()
//		total, err := parseNumericField(vals[0])
//		if err != nil {
//			return fmt.Errorf("parsing total value: %w", err)
//		}
//		processed, err := parseNumericField(vals[1])
//		if err != nil {
//			return fmt.Errorf("parsing processed value: %w", err)
//		}
//		skipped, err := parseNumericField(vals[2])
//		if err != nil {
//			return fmt.Errorf("parsing skipped value: %w", err)
//		}
//		if isCompletedTick(total, processed, skipped) && isNewHighestTick(tickNumber, newHighestTickNumber, storedCompletedTickNumber) {
//			newHighestTickNumber = tickNumber
//			newHighestCount = processed
//		}
//	}
//
//	// update highest tick
//	if newHighestTickNumber > storedCompletedTickNumber {
//		log.Printf("[DEBUG] highest tick [%d] with [%d] logs.", newHighestTickNumber, newHighestCount)
//		_, err = c.redisClient.HSet(ctx, "tick:highest", map[string]any{
//			"tickNumber": newHighestTickNumber,
//			"count":      newHighestCount,
//		})
//		if err != nil {
//			return fmt.Errorf("setting highest tick: %w", err)
//		}
//	}
//
//	// clean obsolete ticks
//	return c.cleanUp(ctx, newHighestCount)
//
//}
//
//func isCompletedTick(total int64, processed int64, skipped int64) bool {
//	return total > 0 && (processed+skipped) >= total
//}
//
//func isNewHighestTick(tickNumber, newHighestTickNumber, storedTickNumber uint64) bool {
//	return tickNumber > newHighestTickNumber && tickNumber > storedTickNumber
//}
//
//// TODO move code into redis store
//func (c *Consumer) cleanUp(ctx context.Context, upTo int64) error {
//	obsoleteTicks, err := c.redisClient.ZRange(ctx, redis.ZRangeArgs{
//		Key:     KeyTicksProcessed,
//		Start:   "-inf",                   // all smaller members
//		Stop:    fmt.Sprintf("(%d", upTo), // exclusive
//		ByScore: true,
//	})
//	if err != nil {
//		return fmt.Errorf("fetching obsolete processed ticks: %w", err)
//	}
//	if len(obsoleteTicks) == 0 {
//		return nil
//	}
//
//	pipe := c.redisClient.Pipeline()
//	for _, obs := range obsoleteTicks {
//		tickNumber, err := strconv.ParseUint(obs, 10, 64)
//		if err != nil {
//			return fmt.Errorf("parsing tick number: %w", err)
//		}
//		pipe.Del(ctx, tickKey(tickNumber))
//	}
//	pipe.ZRemRangeByScore(ctx, KeyTicksProcessed, "-inf", fmt.Sprintf("(%d", upTo))
//
//	_, err = pipe.Exec(ctx)
//	if err != nil {
//		return fmt.Errorf("flushing cleanup pipeline: %w", err)
//	}
//	return nil
//}
//
//func tickKey(number uint64) string {
//	return fmt.Sprintf(KeyTickWithNumber, number)
//}
//
//func parseNumericField(val any) (int64, error) {
//	if val == nil {
//		return 0, nil
//	}
//	return strconv.ParseInt(val.(string), 10, 64)
//}

func unmarshallLogEvent(record *kgo.Record, target any) error {
	err := json.Unmarshal(record.Value, target)
	if err != nil {
		return fmt.Errorf("unmarshalling kafka record: %w", err)
	}
	return nil
}
