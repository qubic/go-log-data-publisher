package tickstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Bose/minisentinel"
	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"

	"github.com/qubic/log-events-consumer/redis"
	goredis "github.com/redis/go-redis/v9"
)

// tests the single steps in the tick store

func TestIntegrationUpdateStatusInRedis(t *testing.T) {
	ctx, store, _, m, cleanup := setupIntegration(t)
	defer cleanup()

	highestTick := uint64(100)
	tickNum := uint64(101)

	store.accumulator.AddProcessed(tickNum, 0, false)
	store.accumulator.AddProcessed(tickNum, 1, true) // Total 2, Processed 2

	batch, err := store.updateStatusInRedis(ctx, highestTick)
	require.NoError(t, err)
	require.Contains(t, batch, tickNum)

	// Verify in miniredis
	key := fmt.Sprintf(KeyTickWithNumber, tickNum)
	require.True(t, m.Exists(key))
	require.Equal(t, "2", m.HGet(key, "processed"))
	require.Equal(t, "2", m.HGet(key, "total"))

	// Verify ZSet
	score, err := m.ZScore(KeyTicksProcessed, "101")
	require.NoError(t, err)
	require.Equal(t, 101.0, score)
}

func TestIntegrationReadUpdatedStatusBack(t *testing.T) {
	ctx, store, _, m, cleanup := setupIntegration(t)
	defer cleanup()

	tickNum := uint64(101)
	m.HSet(tickKey(tickNum), "total", "10", "processed", "7", "skipped", "3")

	updateBatch := map[uint64]*TickStatus{
		tickNum: {Processed: 1, Skipped: 1, Total: 2}, // Content doesn't strictly matter for readback, just the key
	}

	tickNumbers, results, err := store.readUpdatedStatusBack(ctx, updateBatch, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{tickNum}, tickNumbers)
	require.Len(t, results, 1)

	// Check if the command result is correct
	vals := results[0].(*goredis.SliceCmd).Val()
	require.Equal(t, "10", vals[0])
	require.Equal(t, "7", vals[1])
	require.Equal(t, "3", vals[2])
}

func TestIntegrationSetHighestTick(t *testing.T) {
	ctx, store, redisClient, _, cleanup := setupIntegration(t)
	defer cleanup()

	tickNum := uint64(101)
	cmd := goredis.NewSliceCmd(ctx)
	cmd.SetVal([]interface{}{"10", "10", "0"}) // Completed

	newHighest, err := store.setHighestTick(ctx, []uint64{tickNum}, []goredis.Cmder{cmd}, 100)
	require.NoError(t, err)
	require.Equal(t, tickNum, newHighest)

	// Verify in redis
	storedHighest, err := redisClient.HGetUint64(ctx, KeyHighestTick, "tickNumber")
	require.NoError(t, err)
	require.Equal(t, tickNum, storedHighest)
}

func TestIntegrationSetHighestTick_NoProcessedLogs(t *testing.T) {
	ctx, store, redisClient, _, cleanup := setupIntegration(t)
	defer cleanup()

	// Setup a completed tick with 0 processed logs but 10 skipped logs
	tickNum := uint64(101)
	cmd := goredis.NewSliceCmd(ctx)
	cmd.SetVal([]interface{}{"10", "0", "10"}) // Completed (total 10, processed 0, skipped 10)

	// Set initial highest tick to 100
	_, err := redisClient.HSet(ctx, KeyHighestTick, "tickNumber", uint64(100), "count", uint64(5))
	require.NoError(t, err)

	newHighest, err := store.setHighestTick(ctx, []uint64{tickNum}, []goredis.Cmder{cmd}, 100)
	require.NoError(t, err)
	// Should return 0 as new highest because processed logs = 0
	require.Equal(t, uint64(0), newHighest)

	// Verify in redis that it's still 100
	storedHighest, err := redisClient.HGetUint64(ctx, KeyHighestTick, "tickNumber")
	require.NoError(t, err)
	require.Equal(t, uint64(100), storedHighest)
}

func TestIntegrationCleanUp(t *testing.T) {
	ctx, store, _, m, cleanup := setupIntegration(t)
	defer cleanup()

	// Setup some data to be cleaned up
	_, _ = m.ZAdd(KeyTicksProcessed, 90.0, "90")
	_, _ = m.ZAdd(KeyTicksProcessed, 100.0, "100")
	_, _ = m.ZAdd(KeyTicksProcessed, 110.0, "110")

	m.HSet(fmt.Sprintf(KeyTickWithNumber, 90), "foo", "bar")
	m.HSet(fmt.Sprintf(KeyTickWithNumber, 100), "foo", "bar")
	m.HSet(fmt.Sprintf(KeyTickWithNumber, 110), "foo", "bar")

	err := store.cleanUp(ctx, 105)
	require.NoError(t, err)

	// 90 and 100 should be gone
	require.False(t, m.Exists(fmt.Sprintf(KeyTickWithNumber, 90)))
	require.False(t, m.Exists(fmt.Sprintf(KeyTickWithNumber, 100)))
	require.True(t, m.Exists(fmt.Sprintf(KeyTickWithNumber, 110)))

	// ZSet should be updated
	members, err := m.ZMembers(KeyTicksProcessed)
	require.NoError(t, err)
	require.Equal(t, []string{"110"}, members)
}

// setup code

func setupIntegration(t *testing.T) (context.Context, *Store, *redis.Client, *miniredis.Miniredis, func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	m := miniredis.NewMiniRedis()
	err := m.StartAddr("127.0.0.1:0")
	require.NoError(t, err)

	s := minisentinel.NewSentinel(m, minisentinel.WithReplica(m))
	err = s.StartAddr("127.0.0.1:0")
	require.NoError(t, err)

	failoverOpt := &goredis.FailoverOptions{
		MasterName:    s.MasterInfo().Name,
		SentinelAddrs: []string{s.Addr()},
	}
	redisClient := redis.CreateClient(failoverOpt)

	store := NewStore(redisClient)

	cleanup := func() {
		redisClient.Close()
		s.Close()
		m.Close()
		cancel()
	}

	return ctx, store, redisClient, m, cleanup
}
