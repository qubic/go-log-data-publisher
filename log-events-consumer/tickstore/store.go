package tickstore

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/qubic/log-events-consumer/domain"
	goredis "github.com/redis/go-redis/v9"
)

const KeyHighestTick = "tick:highest"
const KeyTicksProcessed = "ticks:processed"
const KeyTickWithNumber = "tick:%d"

type RedisClient interface {
	Pipeline() goredis.Pipeliner
	HGetUint64(ctx context.Context, key, field string) (uint64, error)
	HSet(ctx context.Context, key string, values ...interface{}) (int64, error)
	ZRange(ctx context.Context, z goredis.ZRangeArgs) ([]string, error)
}

type Store struct {
	redisClient RedisClient
	accumulator *Accumulator
}

func New(redisClient RedisClient) *Store {
	return &Store{
		redisClient: redisClient,
		accumulator: NewAccumulator(),
	}
}

func (s *Store) AddProcessed(log domain.LogEvent) {
	s.accumulator.AddProcessed(log.TickNumber, log.Index, log.LastLogForTick)
}

func (s *Store) AddSkipped(log domain.LogEvent) {
	s.accumulator.AddSkipped(log.TickNumber, log.Index, log.LastLogForTick)
}

func (s *Store) UpdateTickHeight(ctx context.Context) error {

	// get highest processed tick
	storedCompletedTickNumber, err := s.redisClient.HGetUint64(ctx, KeyHighestTick, "tickNumber")
	if err != nil {
		return fmt.Errorf("fetching highest tick number: %w", err)
	}

	// update in redis
	pipe := s.redisClient.Pipeline()
	batch := s.accumulator.Drain()
	for tickNumber, status := range batch {
		if tickNumber > storedCompletedTickNumber { // otherwise irrelevant
			// add to ticks so that we can find it again later
			pipe.ZAdd(ctx, KeyTicksProcessed, goredis.Z{Score: float64(tickNumber), Member: tickNumber})
			// update tick status
			key := tickKey(tickNumber)
			pipe.HIncrBy(ctx, key, "processed", status.Processed)
			pipe.HIncrBy(ctx, key, "skipped", status.Processed)
			if status.Total > 0 {
				pipe.HSet(ctx, key, "total", status.Total)
			}
			log.Printf("[DEBUG] tick %d: processed %d, skipped %d, total %d", tickNumber, status.Processed, status.Skipped, status.Total)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("flushing update pipeline: %w", err)
	}

	// read from redis to calculate newest highest tick
	pipe = s.redisClient.Pipeline()
	tickNumbers := make([]uint64, 0, len(batch))
	for tickNumber := range batch {
		if tickNumber > storedCompletedTickNumber {
			tickNumbers = append(tickNumbers, tickNumber)
			pipe.HMGet(ctx, tickKey(tickNumber), "total", "processed", "skipped")
		}
	}
	commands, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("flushing readback pipeline: %w", err)
	}

	var newHighestTickNumber uint64
	var newHighestCount int64

	for i, tickNumber := range tickNumbers {
		vals := commands[i].(*goredis.SliceCmd).Val()
		total, err := parseNumericField(vals[0])
		if err != nil {
			return fmt.Errorf("parsing total value: %w", err)
		}
		processed, err := parseNumericField(vals[1])
		if err != nil {
			return fmt.Errorf("parsing processed value: %w", err)
		}
		skipped, err := parseNumericField(vals[2])
		if err != nil {
			return fmt.Errorf("parsing skipped value: %w", err)
		}
		if isCompletedTick(total, processed, skipped) && isNewHighestTick(tickNumber, newHighestTickNumber, storedCompletedTickNumber) {
			newHighestTickNumber = tickNumber
			newHighestCount = processed
		}
	}

	// update highest tick
	if newHighestTickNumber > storedCompletedTickNumber {
		log.Printf("[DEBUG] highest tick [%d] with [%d] logs.", newHighestTickNumber, newHighestCount)
		_, err = s.redisClient.HSet(ctx, "tick:highest", map[string]any{
			"tickNumber": newHighestTickNumber,
			"count":      newHighestCount,
		})
		if err != nil {
			return fmt.Errorf("setting highest tick: %w", err)
		}
	}

	// clean obsolete ticks
	return s.cleanUp(ctx, newHighestCount)

}

func isCompletedTick(total int64, processed int64, skipped int64) bool {
	return total > 0 && (processed+skipped) >= total
}

func isNewHighestTick(tickNumber, newHighestTickNumber, storedTickNumber uint64) bool {
	return tickNumber > newHighestTickNumber && tickNumber > storedTickNumber
}

func (s *Store) cleanUp(ctx context.Context, upTo int64) error {
	obsoleteTicks, err := s.redisClient.ZRange(ctx, goredis.ZRangeArgs{
		Key:     KeyTicksProcessed,
		Start:   "-inf",                   // all smaller members
		Stop:    fmt.Sprintf("(%d", upTo), // exclusive
		ByScore: true,
	})
	if err != nil {
		return fmt.Errorf("fetching obsolete processed ticks: %w", err)
	}
	if len(obsoleteTicks) == 0 {
		return nil
	}

	pipe := s.redisClient.Pipeline()
	for _, obs := range obsoleteTicks {
		tickNumber, err := strconv.ParseUint(obs, 10, 64)
		if err != nil {
			return fmt.Errorf("parsing tick number: %w", err)
		}
		pipe.Del(ctx, tickKey(tickNumber))
	}
	pipe.ZRemRangeByScore(ctx, KeyTicksProcessed, "-inf", fmt.Sprintf("(%d", upTo))

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("flushing cleanup pipeline: %w", err)
	}
	return nil
}

func tickKey(number uint64) string {
	return fmt.Sprintf(KeyTickWithNumber, number)
}

func parseNumericField(val any) (int64, error) {
	if val == nil {
		return 0, nil
	}
	return strconv.ParseInt(val.(string), 10, 64)
}
