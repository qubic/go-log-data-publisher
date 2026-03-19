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

func NewStore(redisClient RedisClient) *Store {
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
	highestTickNumber, err := s.redisClient.HGetUint64(ctx, KeyHighestTick, "tickNumber")
	if err != nil {
		return fmt.Errorf("fetching highest tick number: %w", err)
	}

	// at the moment we only update the highest tick that we find.
	// if we want to switch to consecutive tick number updates we have to do changes here and in other places in the
	// tick store where tick numbers are filtered (like tick number > highest tick number)

	updateBatch, err := s.updateStatusInRedis(ctx, highestTickNumber)
	if err != nil {
		return fmt.Errorf("updating status in redis: %w", err)
	}

	updatedTickNumbers, updateResults, err := s.readUpdatedStatusBack(ctx, updateBatch, highestTickNumber)
	if err != nil {
		return fmt.Errorf("reading back updated status: %w", err)
	}

	newHighestTickNumber, err := s.setHighestTick(ctx, updatedTickNumbers, updateResults, highestTickNumber)
	if err != nil {
		return fmt.Errorf("setting highest tick: %w", err)
	}

	// clean obsolete ticks if highest changed
	if newHighestTickNumber > highestTickNumber {
		return s.cleanUp(ctx, newHighestTickNumber)
	}

	return nil
}

func (s *Store) updateStatusInRedis(ctx context.Context, highestTickNumber uint64) (map[uint64]*TickStatus, error) {
	// update in redis
	pipe := s.redisClient.Pipeline()
	updateBatch := s.accumulator.Drain()
	for tickNumber, status := range updateBatch {
		if tickNumber > highestTickNumber { // otherwise irrelevant
			// add to ticks so that we can find it again later
			pipe.ZAdd(ctx, KeyTicksProcessed, goredis.Z{Score: float64(tickNumber), Member: tickNumber})
			// update tick status
			key := tickKey(tickNumber)
			pipe.HIncrBy(ctx, key, "processed", int64(status.Processed))
			pipe.HIncrBy(ctx, key, "skipped", int64(status.Skipped))
			if status.Total > 0 {
				pipe.HSet(ctx, key, "total", status.Total)
			}
			log.Printf("[DEBUG] tick %d: processed %d, skipped %d, total %d", tickNumber, status.Processed, status.Skipped, status.Total)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("flushing update pipeline: %w", err)
	}
	return updateBatch, nil
}

func (s *Store) readUpdatedStatusBack(ctx context.Context, updateBatch map[uint64]*TickStatus, highestTickNumber uint64) ([]uint64, []goredis.Cmder, error) {
	// read from redis to calculate newest highest tick
	pipe := s.redisClient.Pipeline()
	tickNumbers := make([]uint64, 0, len(updateBatch))
	for tickNumber := range updateBatch {
		if tickNumber > highestTickNumber {
			tickNumbers = append(tickNumbers, tickNumber)
			pipe.HMGet(ctx, tickKey(tickNumber), "total", "processed", "skipped")
		}
	}
	commandResults, err := pipe.Exec(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("flushing readback pipeline: %w", err)
	}
	return tickNumbers, commandResults, nil
}

func (s *Store) setHighestTick(ctx context.Context, updatedTickNumbers []uint64, updateResults []goredis.Cmder, highestTickNumber uint64) (uint64, error) {
	var newHighestTickNumber uint64
	var newHighestCount uint64

	for i, tickNumber := range updatedTickNumbers {
		vals := updateResults[i].(*goredis.SliceCmd).Val()
		total, err := parsePositiveNumericField(vals[0])
		if err != nil {
			return 0, fmt.Errorf("parsing total value: %w", err)
		}
		processed, err := parsePositiveNumericField(vals[1])
		if err != nil {
			return 0, fmt.Errorf("parsing processed value: %w", err)
		}
		skipped, err := parsePositiveNumericField(vals[2])
		if err != nil {
			return 0, fmt.Errorf("parsing skipped value: %w", err)
		}
		if isCompletedTick(total, processed, skipped) && isNewHighestTick(tickNumber, newHighestTickNumber, highestTickNumber) {
			newHighestTickNumber = tickNumber
			newHighestCount = processed
		}
	}

	// update highest tick number and count
	if newHighestTickNumber > highestTickNumber {
		log.Printf("[DEBUG] highest tick [%d] with [%d] logs.", newHighestTickNumber, newHighestCount)
		_, err := s.redisClient.HSet(ctx, "tick:highest", map[string]any{
			"tickNumber": newHighestTickNumber,
			"count":      newHighestCount,
		})
		if err != nil {
			return 0, fmt.Errorf("setting highest tick: %w", err)
		}
	}
	return newHighestTickNumber, nil
}

func (s *Store) cleanUp(ctx context.Context, upTo uint64) error {
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

func isCompletedTick(total, processed, skipped uint64) bool {
	return total > 0 && (processed+skipped) >= total
}

func isNewHighestTick(tickNumber, currentHighest, storedTickNumber uint64) bool {
	return tickNumber > currentHighest && tickNumber > storedTickNumber
}

func tickKey(number uint64) string {
	return fmt.Sprintf(KeyTickWithNumber, number)
}

func parsePositiveNumericField(val any) (uint64, error) {
	if val == nil {
		return 0, nil
	}
	s, ok := val.(string)
	if !ok {
		return 0, fmt.Errorf("unexpected type: %T", val)
	}
	res, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return res, nil
}
