package tickstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Bose/minisentinel"
	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"

	"github.com/qubic/log-events-consumer/domain"
	"github.com/qubic/log-events-consumer/redis"
	goredis "github.com/redis/go-redis/v9"
)

func TestStoreIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Setup miniredis and minisentinel
	m := miniredis.NewMiniRedis()
	err := m.StartAddr("127.0.0.1:0")
	require.NoError(t, err)
	defer m.Close()

	s := minisentinel.NewSentinel(m, minisentinel.WithReplica(m))
	err = s.StartAddr("127.0.0.1:0")
	require.NoError(t, err)
	defer s.Close()

	// 2. Setup Redis client
	failoverOpt := &goredis.FailoverOptions{
		MasterName:    s.MasterInfo().Name,
		SentinelAddrs: []string{s.Addr()},
	}
	redisClient := redis.CreateClient(failoverOpt)
	defer redisClient.Close()

	// 3. Setup Store
	store := NewStore(redisClient)

	// 4. Scenario: Process logs for tick 1000
	// 1000 is higher than current (0)
	tickNum := uint64(1000)
	store.AddProcessed(domain.LogEvent{TickNumber: tickNum, Index: 1, LastLogForTick: false})
	store.AddSkipped(domain.LogEvent{TickNumber: tickNum, Index: 2, LastLogForTick: false})  // Skipped
	store.AddProcessed(domain.LogEvent{TickNumber: tickNum, Index: 3, LastLogForTick: true}) // Total 3

	// Update tick height
	err = store.UpdateTickHeight(ctx)
	require.NoError(t, err)

	// Verify highest tick in redis
	highest, err := redisClient.HGetUint64(ctx, KeyHighestTick, "tickNumber")
	require.NoError(t, err)
	require.Equal(t, tickNum, highest)

	// Verify tick status in redis
	key := fmt.Sprintf(KeyTickWithNumber, tickNum)
	total, err := redisClient.HGetUint64(ctx, key, "total")
	require.NoError(t, err)
	require.Equal(t, uint64(3), total)

	processed, err := redisClient.HGetUint64(ctx, key, "processed")
	require.NoError(t, err)
	require.Equal(t, uint64(2), processed)

	skipped, err := redisClient.HGetUint64(ctx, key, "skipped")
	require.NoError(t, err)
	require.Equal(t, uint64(1), skipped)

	// 5. Scenario: Move to tick 1001 and ensure cleanup of 1000
	nextTickNum := uint64(1001)
	store.AddSkipped(domain.LogEvent{TickNumber: nextTickNum, Index: 1, LastLogForTick: false})  // Skipped
	store.AddSkipped(domain.LogEvent{TickNumber: nextTickNum, Index: 2, LastLogForTick: false})  // Skipped
	store.AddProcessed(domain.LogEvent{TickNumber: nextTickNum, Index: 3, LastLogForTick: true}) // Total 1

	err = store.UpdateTickHeight(ctx)
	require.NoError(t, err)

	// Verify highest tick is now 1001
	highest, err = redisClient.HGetUint64(ctx, KeyHighestTick, "tickNumber")
	require.NoError(t, err)
	require.Equal(t, nextTickNum, highest)

	// Verify tick 1000 is cleaned up (obsolete)
	exists := m.Exists(key)
	require.False(t, exists, "Tick 1000 should be cleaned up")

	// Verify tick 1001 exists
	nextKey := fmt.Sprintf(KeyTickWithNumber, nextTickNum)
	exists = m.Exists(nextKey)
	require.True(t, exists, "Tick 1001 should exist")
}
