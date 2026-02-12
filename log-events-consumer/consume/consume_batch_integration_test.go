package consume

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/qubic/log-events-consumer/elastic"
	"github.com/qubic/log-events-consumer/metrics"
	"github.com/qubic/log-events-consumer/testutils"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConsumeBatch_Integration(t *testing.T) {
	kafkaDir := "testdata/kafka"
	elasticDir := "testdata/elastic"

	files, err := testutils.ReadDir(kafkaDir)
	require.NoError(t, err)

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
			consumer := NewConsumer(mockKafka, mockElastic, m)

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
			consumer := NewConsumer(mockKafka, mockElastic, m)

			count, err := consumer.consumeBatch(context.Background())
			require.NoError(t, err)

			require.Equal(t, 0, count, "Expected 0 documents to be processed for %s", file.Name())
			require.Empty(t, capturedDocs, "Expected no documents to be indexed for %s", file.Name())
		})
	}
}
