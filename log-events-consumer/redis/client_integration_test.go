package redis

import (
	"context"
	"testing"
	"time"

	"github.com/Bose/minisentinel"
	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"

	"github.com/redis/go-redis/v9"
)

func TestClientPing(t *testing.T) {

	m := miniredis.NewMiniRedis()
	err := m.StartAddr("127.0.0.1:0")
	require.NoError(t, err)
	defer m.Close()

	s := minisentinel.NewSentinel(m, minisentinel.WithReplica(m))
	err = s.StartAddr("127.0.0.1:0")
	require.NoError(t, err)
	defer s.Close()

	// Setup: Create a Redis client with failover configuration
	// Adjust these settings to match your Redis setup
	failoverOpt := &redis.FailoverOptions{
		MasterName:    s.MasterInfo().Name,
		SentinelAddrs: []string{s.Addr()},
		DB:            0,
	}

	client := CreateClient(failoverOpt)
	defer client.Close()

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test: Call the Ping method
	result, err := client.Ping(ctx)

	// Assert: Check that Ping was successful
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	if result != "PONG" {
		t.Errorf("Expected PONG, got %s", result)
	}
}
