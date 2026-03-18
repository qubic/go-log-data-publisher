//go:build integration

package useless

import (
	"context"
	"testing"
	"time"

	"github.com/qubic/log-events-consumer/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedis_ping(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	red := redis.CreateClient(&goredis.FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: []string{"localhost:26379"},
	})

	pong, err := red.Ping(ctx)
	require.NoError(t, err)
	assert.Equal(t, "PONG", pong)
}

func TestRedis_HGET_givenNoValue_thenEmptyString(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	red := redis.CreateClient(&goredis.FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: []string{"localhost:26379"},
	})

	result, err := red.HGet(ctx, "foo", "bar")
	assert.NoError(t, err)
	assert.Empty(t, result)
}
