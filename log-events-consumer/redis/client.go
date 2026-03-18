package redis

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	Rdb *redis.Client
}

func CreateClient(failoverOpt *redis.FailoverOptions) *Client {
	// sends read requests to slave nodes
	client := redis.NewFailoverClient(failoverOpt)
	return &Client{Rdb: client}
}

func (c *Client) Ping(ctx context.Context) (string, error) {
	return c.Rdb.Ping(ctx).Result()
}

func (c *Client) Pipeline() redis.Pipeliner {
	return c.Rdb.Pipeline()
}

func (c *Client) HGetUint64(ctx context.Context, key, field string) (uint64, error) {
	// err == redis.Nil means that the field does not exist
	result, err := c.Rdb.HGet(ctx, key, field).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, fmt.Errorf("redis hget: %w", err)
	}
	return strconv.ParseUint(result, 10, 64)
}

func (c *Client) HSet(ctx context.Context, key string, values ...interface{}) (int64, error) {
	return c.Rdb.HSet(ctx, key, values).Result()
}

func (c *Client) ZRange(ctx context.Context, z redis.ZRangeArgs) ([]string, error) {
	return c.Rdb.ZRangeArgs(ctx, z).Result()
	/*
		redis.ZRangeArgs{
				Key:     key,
				Start:   "-inf",
				Stop:    fmt.Sprintf("(%d", highestTick),
				ByScore: true,
			}
	*/

}

func (c *Client) Close() {
	err := c.Rdb.Close()
	if err != nil {
		log.Printf("[ERROR] closing redis connection: %v", err)
	}
}
