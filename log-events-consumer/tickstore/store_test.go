package tickstore

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockRedisClient is a mock of RedisClient interface
type MockRedisClient struct {
	mock.Mock
}

func (m *MockRedisClient) Pipeline() redis.Pipeliner {
	args := m.Called()
	return args.Get(0).(redis.Pipeliner)
}

func (m *MockRedisClient) HGetUint64(ctx context.Context, key, field string) (uint64, error) {
	args := m.Called(ctx, key, field)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *MockRedisClient) HSet(ctx context.Context, key string, values ...interface{}) (int64, error) {
	args := m.Called(ctx, key, values)
	return int64(args.Int(0)), args.Error(1)
}

func (m *MockRedisClient) ZRange(ctx context.Context, z redis.ZRangeArgs) ([]string, error) {
	args := m.Called(ctx, z)
	return args.Get(0).([]string), args.Error(1)
}

// MockPipeliner is a mock of goredis.Pipeliner interface
type MockPipeliner struct {
	mock.Mock
	redis.Pipeliner
}

func (m *MockPipeliner) ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	m.Called(ctx, key, members)
	return redis.NewIntCmd(ctx)
}

func (m *MockPipeliner) HIncrBy(ctx context.Context, key, field string, incr int64) *redis.IntCmd {
	m.Called(ctx, key, field, incr)
	return redis.NewIntCmd(ctx)
}

func (m *MockPipeliner) HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	m.Called(ctx, key, values)
	return redis.NewIntCmd(ctx)
}

func (m *MockPipeliner) HMGet(ctx context.Context, key string, fields ...string) *redis.SliceCmd {
	args := m.Called(ctx, key, fields)
	cmd := redis.NewSliceCmd(ctx)
	if args.Get(0) != nil {
		cmd.SetVal(args.Get(0).([]interface{}))
	}
	return cmd
}

func (m *MockPipeliner) Exec(ctx context.Context) ([]redis.Cmder, error) {
	args := m.Called(ctx)
	return args.Get(0).([]redis.Cmder), args.Error(1)
}

func (m *MockPipeliner) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	m.Called(ctx, keys)
	return redis.NewIntCmd(ctx)
}

func (m *MockPipeliner) ZRemRangeByScore(ctx context.Context, key, min, max string) *redis.IntCmd {
	m.Called(ctx, key, min, max)
	return redis.NewIntCmd(ctx)
}

func TestUpdateStatusInRedis(t *testing.T) {
	mockRedis := &MockRedisClient{}
	mockPipe := &MockPipeliner{}
	store := &Store{
		redisClient: mockRedis,
		accumulator: NewAccumulator(),
	}

	ctx := context.Background()
	highestTick := uint64(100)

	// Add some data to accumulator
	store.accumulator.AddProcessed(101, 10, false)
	store.accumulator.AddSkipped(101, 12, true) // Skipped 1, Total 13
	store.accumulator.AddProcessed(101, 11, false)
	store.accumulator.AddProcessed(99, 1, true) // Should be ignored

	mockRedis.On("Pipeline").Return(mockPipe)
	mockPipe.On("ZAdd", ctx, KeyTicksProcessed, []redis.Z{{Score: 101, Member: uint64(101)}}).Return()
	mockPipe.On("HIncrBy", ctx, "tick:101", "processed", int64(2)).Return()
	mockPipe.On("HIncrBy", ctx, "tick:101", "skipped", int64(1)).Return()
	mockPipe.On("HSet", ctx, "tick:101", []interface{}{"total", uint64(13)}).Return()
	mockPipe.On("Exec", ctx).Return([]redis.Cmder{}, nil)

	batch, err := store.updateStatusInRedis(ctx, highestTick)

	assert.NoError(t, err)
	assert.NotNil(t, batch)
	assert.Contains(t, batch, uint64(101))
	assert.Contains(t, batch, uint64(99))
	mockRedis.AssertExpectations(t)
	mockPipe.AssertExpectations(t)
}

func TestReadUpdatedStatusBack(t *testing.T) {
	mockRedis := &MockRedisClient{}
	mockPipe := &MockPipeliner{}
	store := &Store{
		redisClient: mockRedis,
		accumulator: NewAccumulator(),
	}

	ctx := context.Background()
	highestTick := uint64(100)
	updateBatch := map[uint64]*TickStatus{
		101: {Processed: 2, Skipped: 1, Total: 3},
		99:  {Processed: 1, Skipped: 0, Total: 1},
	}

	mockRedis.On("Pipeline").Return(mockPipe)
	mockPipe.On("HMGet", ctx, "tick:101", []string{"total", "processed", "skipped"}).Return([]interface{}{"3", "2", "1"}, nil)

	cmd101 := redis.NewSliceCmd(ctx)
	cmd101.SetVal([]interface{}{"3", "2", "1"})
	mockPipe.On("Exec", ctx).Return([]redis.Cmder{cmd101}, nil)

	tickNumbers, results, err := store.readUpdatedStatusBack(ctx, updateBatch, highestTick)

	assert.NoError(t, err)
	assert.Equal(t, []uint64{101}, tickNumbers)
	assert.Len(t, results, 1)
	mockRedis.AssertExpectations(t)
	mockPipe.AssertExpectations(t)
}

func TestSetHighestTick(t *testing.T) {
	mockRedis := &MockRedisClient{}
	store := &Store{
		redisClient: mockRedis,
	}

	ctx := context.Background()
	highestTick := uint64(100)
	updatedTickNumbers := []uint64{101, 102}

	cmd101 := redis.NewSliceCmd(ctx)
	cmd101.SetVal([]interface{}{"10", "10", "0"}) // completed
	cmd102 := redis.NewSliceCmd(ctx)
	cmd102.SetVal([]interface{}{"10", "5", "0"}) // not completed

	updateResults := []redis.Cmder{cmd101, cmd102}

	mockRedis.On("HSet", ctx, "tick:highest", []interface{}{"tickNumber", uint64(101), "count", uint64(10)}).Return(1, nil)

	newHighest, err := store.setHighestTick(ctx, updatedTickNumbers, updateResults, highestTick)

	assert.NoError(t, err)
	assert.Equal(t, uint64(101), newHighest)
	mockRedis.AssertExpectations(t)
}

func TestCleanUp(t *testing.T) {
	mockRedis := &MockRedisClient{}
	mockPipe := &MockPipeliner{}
	store := &Store{
		redisClient: mockRedis,
	}

	ctx := context.Background()
	upTo := uint64(105)

	mockRedis.On("ZRange", ctx, redis.ZRangeArgs{
		Key:     KeyTicksProcessed,
		Start:   "-inf",
		Stop:    "(105",
		ByScore: true,
	}).Return([]string{"101", "102"}, nil)

	mockRedis.On("Pipeline").Return(mockPipe)
	mockPipe.On("Del", ctx, []string{"tick:101"}).Return()
	mockPipe.On("Del", ctx, []string{"tick:102"}).Return()
	mockPipe.On("ZRemRangeByScore", ctx, KeyTicksProcessed, "-inf", "(105").Return()
	mockPipe.On("Exec", ctx).Return([]redis.Cmder{}, nil)

	err := store.cleanUp(ctx, upTo)

	assert.NoError(t, err)
	mockRedis.AssertExpectations(t)
	mockPipe.AssertExpectations(t)
}

func TestUpdateTickHeight(t *testing.T) {
	mockRedis := &MockRedisClient{}
	mockPipe := &MockPipeliner{}
	store := &Store{
		redisClient: mockRedis,
		accumulator: NewAccumulator(),
	}

	ctx := context.Background()
	currentHighest := uint64(100)
	newTick := uint64(101)

	// Prepare data in accumulator
	store.accumulator.AddProcessed(newTick, 0, false)
	store.accumulator.AddProcessed(newTick, 1, true) // Total 2, Processed 2, Skipped 0

	// 1. Fetch highest tick
	mockRedis.On("HGetUint64", ctx, KeyHighestTick, "tickNumber").Return(currentHighest, nil)

	// 2. updateStatusInRedis
	mockRedis.On("Pipeline").Return(mockPipe)
	mockPipe.On("ZAdd", ctx, KeyTicksProcessed, []redis.Z{{Score: float64(newTick), Member: newTick}}).Return()
	mockPipe.On("HIncrBy", ctx, "tick:101", "processed", int64(2)).Return()
	mockPipe.On("HIncrBy", ctx, "tick:101", "skipped", int64(0)).Return()
	mockPipe.On("HSet", ctx, "tick:101", []interface{}{"total", uint64(2)}).Return()
	mockPipe.On("Exec", ctx).Return([]redis.Cmder{}, nil).Once() // First Exec is for update

	// 3. readUpdatedStatusBack
	// Pipeline called again for readback
	mockPipe.On("HMGet", ctx, "tick:101", []string{"total", "processed", "skipped"}).Return([]interface{}{"2", "2", "0"}, nil)

	cmd101 := redis.NewSliceCmd(ctx)
	cmd101.SetVal([]interface{}{"2", "2", "0"})
	mockPipe.On("Exec", ctx).Return([]redis.Cmder{cmd101}, nil).Once() // Second Exec is for readback

	// 4. setHighestTick
	mockRedis.On("HSet", ctx, "tick:highest", []interface{}{"tickNumber", uint64(101), "count", uint64(2)}).Return(1, nil)

	// 5. cleanUp
	mockRedis.On("ZRange", ctx, redis.ZRangeArgs{
		Key:     KeyTicksProcessed,
		Start:   "-inf",
		Stop:    "(101",
		ByScore: true,
	}).Return([]string{"99"}, nil)

	mockPipe.On("Del", ctx, []string{"tick:99"}).Return()
	mockPipe.On("ZRemRangeByScore", ctx, KeyTicksProcessed, "-inf", "(101").Return()
	mockPipe.On("Exec", ctx).Return([]redis.Cmder{}, nil).Once() // Third Exec is for cleanup

	err := store.UpdateTickHeight(ctx)

	assert.NoError(t, err)
	mockRedis.AssertExpectations(t)
	mockPipe.AssertExpectations(t)
}
