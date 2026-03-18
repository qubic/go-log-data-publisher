package consume

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/Bose/minisentinel"
	"github.com/alicebob/miniredis/v2"
	"github.com/qubic/log-events-consumer/elastic"
	"github.com/qubic/log-events-consumer/metrics"
	"github.com/qubic/log-events-consumer/redis"
	"github.com/qubic/log-events-consumer/testutils"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConsumeBatch_Integration(t *testing.T) {
	kafkaDir := "testdata/kafka"
	elasticDir := "testdata/elastic"

	files, err := testutils.ReadDir(kafkaDir)
	require.NoError(t, err)

	port := rand.Uint32()%60000 + 5000
	// setup embedded redis and sentinel
	mr := setupRedis(int(port))
	defer mr.Close()

	ms := setupSentinel(mr, int(port+1))
	defer ms.Close()

	redisClient := setupRedisClient(ms)
	defer redisClient.Close()

	for i, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != ".json" {
			continue
		}

		t.Run(file.Name(), func(t *testing.T) {
			inputPath := filepath.Join(kafkaDir, file.Name())
			expectedPath := filepath.Join(elasticDir, file.Name())

			inputData, err := testutils.ReadTestFile(inputPath)
			require.NoError(t, err)

			expectedData, err := testutils.ReadTestFile(expectedPath)
			require.NoError(t, err)

			// Mock Kafka Client
			mockKafka := &mockKafkaClient{
				pollRecordsFunc: func(ctx context.Context, maxPollRecords int) kgo.Fetches {
					return kgo.Fetches{{
						Topics: []kgo.FetchTopic{
							{
								Topic:      "test-topic",
								Partitions: []kgo.FetchPartition{{Records: []*kgo.Record{{Value: inputData}}}},
							},
						},
					}}
				},
				commitUncommittedOffsetsFunc: func(ctx context.Context) error {
					return nil
				},
			}

			// Mock Elastic Client
			var capturedDocs []*elastic.EsDocument
			mockElastic := &mockElasticClient{
				bulkIndexFunc: func(ctx context.Context, data []*elastic.EsDocument) error {
					capturedDocs = data
					return nil
				},
			}

			m := metrics.NewMetrics(fmt.Sprintf("test_integration_%d", i))
			consumer := NewConsumer(mockKafka, mockElastic, redisClient, m, map[uint64][]int16{0: {0, 1, 2, 3, 4, 5, 6, 8, 11, 12, 13, 255}})

			count, err := consumer.consumeBatch(context.Background())
			require.NoError(t, err)

			// We expect all files in testdata to be supported unless they are specifically designed to be filtered.

			// Unmarshal the expected elastic JSON to get epoch and logId (elastic id).
			var expectedObj map[string]any
			err = json.Unmarshal(expectedData, &expectedObj)
			require.NoError(t, err)

			require.Equal(t, 1, count, "Expected 1 document to be processed for %s", file.Name())
			require.Len(t, capturedDocs, 1)

			// Verify ID
			epoch := uint32(expectedObj["epoch"].(float64))
			logId := uint64(expectedObj["logId"].(float64))

			expectedID := fmt.Sprintf("%d-%d", int(epoch), int(logId))
			require.Equal(t, expectedID, capturedDocs[0].Id)

			// Verify JSON payload
			require.JSONEq(t, string(expectedData), string(capturedDocs[0].Payload))
		})
	}
}

func TestConsumeBatch_Filtered_Integration(t *testing.T) {
	filteredDir := "testdata/filtered"

	files, err := testutils.ReadDir(filteredDir)
	require.NoError(t, err)

	port := rand.Uint32()%60000 + 5000
	// setup embedded redis and sentinel
	mr := setupRedis(int(port))
	defer mr.Close()

	ms := setupSentinel(mr, int(port+1))
	defer ms.Close()

	redisClient := setupRedisClient(ms)
	defer redisClient.Close()

	for i, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != ".json" {
			continue
		}

		t.Run(file.Name(), func(t *testing.T) {
			inputPath := filepath.Join(filteredDir, file.Name())

			inputData, err := testutils.ReadTestFile(inputPath)
			require.NoError(t, err)

			// Mock Kafka Client
			mockKafka := &mockKafkaClient{
				pollRecordsFunc: func(ctx context.Context, maxPollRecords int) kgo.Fetches {
					return kgo.Fetches{{
						Topics: []kgo.FetchTopic{
							{
								Topic:      "test-topic",
								Partitions: []kgo.FetchPartition{{Records: []*kgo.Record{{Value: inputData}}}},
							},
						},
					}}
				},
				commitUncommittedOffsetsFunc: func(ctx context.Context) error {
					return nil
				},
			}

			// Mock Elastic Client
			var capturedDocs []*elastic.EsDocument
			mockElastic := &mockElasticClient{
				bulkIndexFunc: func(ctx context.Context, data []*elastic.EsDocument) error {
					capturedDocs = data
					return nil
				},
			}

			m := metrics.NewMetrics(fmt.Sprintf("test_filtered_integration_%d", i))
			consumer := NewConsumer(mockKafka, mockElastic, redisClient, m, map[uint64][]int16{0: {0, 1, 2, 3, 8, 13}})

			count, err := consumer.consumeBatch(context.Background())
			require.NoError(t, err)

			require.Equal(t, 0, count, "Expected 0 documents to be processed for %s", file.Name())
			require.Empty(t, capturedDocs, "Expected no documents to be indexed for %s", file.Name())
		})
	}
}

func setupRedis(port int) *miniredis.Miniredis {
	mr := miniredis.NewMiniRedis()
	err := mr.StartAddr(fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Setup redis: %v", err)
	}
	return mr
}

func setupSentinel(m *miniredis.Miniredis, port int) *minisentinel.Sentinel {
	ms := minisentinel.NewSentinel(m, minisentinel.WithReplica(m))
	err := ms.StartAddr(fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Setup sentinel: %v", err)
	}
	return ms
}

func setupRedisClient(ms *minisentinel.Sentinel) *redis.Client {
	// Setup: Create a Redis client with failover configuration
	// Adjust these settings to match your Redis setup
	failoverOpt := &goredis.FailoverOptions{
		MasterName:    ms.MasterInfo().Name,
		SentinelAddrs: []string{ms.Addr()},
		DB:            0,
	}
	return redis.CreateClient(failoverOpt)
}
