package consume

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/qubic/log-events-consumer/domain"
	"github.com/qubic/log-events-consumer/elastic"
	"github.com/qubic/log-events-consumer/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
)

type mockKafkaClient struct {
	pollRecordsFunc              func(ctx context.Context, maxPollRecords int) kgo.Fetches
	commitUncommittedOffsetsFunc func(ctx context.Context) error
	allowRebalanceCalled         bool
}

func (m *mockKafkaClient) PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches {
	return m.pollRecordsFunc(ctx, maxPollRecords)
}

func (m *mockKafkaClient) CommitUncommittedOffsets(ctx context.Context) error {
	if m.commitUncommittedOffsetsFunc != nil {
		return m.commitUncommittedOffsetsFunc(ctx)
	}
	return nil
}

func (m *mockKafkaClient) AllowRebalance() {
	m.allowRebalanceCalled = true
}

type mockElasticClient struct {
	bulkIndexFunc func(ctx context.Context, data []*elastic.EsDocument) error
}

func (m *mockElasticClient) BulkIndex(ctx context.Context, data []*elastic.EsDocument) error {
	if m.bulkIndexFunc != nil {
		return m.bulkIndexFunc(ctx, data)
	}
	return nil
}

func TestConsumeBatch_Success(t *testing.T) {
	logEvent := domain.LogEvent{
		Ok:        true,
		Epoch:     100,
		Tick:      1000,
		Id:        12345,
		Hash:      "test-hash",
		Type:      1,
		TypeName:  "TestEvent",
		Timestamp: "2024-01-01T00:00:00Z",
		TxHash:    "tx-hash",
		BodySize:  100,
		Body:      "test-body",
	}

	logEventJSON, _ := json.Marshal(logEvent)

	mockKafka := &mockKafkaClient{
		pollRecordsFunc: func(ctx context.Context, maxPollRecords int) kgo.Fetches {
			record := &kgo.Record{
				Value: logEventJSON,
			}
			return kgo.Fetches{
				{
					Topics: []kgo.FetchTopic{
						{
							Topic: "test-topic",
							Partitions: []kgo.FetchPartition{
								{
									Partition: 0,
									Records:   []*kgo.Record{record},
								},
							},
						},
					},
				},
			}
		},
	}

	var indexedDocs []*elastic.EsDocument
	mockElastic := &mockElasticClient{
		bulkIndexFunc: func(ctx context.Context, data []*elastic.EsDocument) error {
			indexedDocs = data
			return nil
		},
	}

	m := metrics.NewMetrics("test_success")
	consumer := NewConsumer(mockKafka, mockElastic, m)

	count, err := consumer.consumeBatch(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected count 1, got: %d", count)
	}

	if !mockKafka.allowRebalanceCalled {
		t.Error("Expected AllowRebalance to be called")
	}

	if len(indexedDocs) != 1 {
		t.Fatalf("Expected 1 document to be indexed, got: %d", len(indexedDocs))
	}

	if consumer.currentTick != 1000 {
		t.Errorf("Expected currentTick to be 1000, got: %d", consumer.currentTick)
	}

	if consumer.currentEpoch != 100 {
		t.Errorf("Expected currentEpoch to be 100, got: %d", consumer.currentEpoch)
	}
}

func TestConsumeBatch_EmptyBatch(t *testing.T) {
	mockKafka := &mockKafkaClient{
		pollRecordsFunc: func(ctx context.Context, maxPollRecords int) kgo.Fetches {
			return kgo.Fetches{
				{
					Topics: []kgo.FetchTopic{},
				},
			}
		},
	}

	mockElastic := &mockElasticClient{}
	m := metrics.NewMetrics("test_empty")
	consumer := NewConsumer(mockKafka, mockElastic, m)

	count, err := consumer.consumeBatch(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if count != 0 {
		t.Errorf("Expected count 0, got: %d", count)
	}
}

func TestConsumeBatch_InvalidJSON(t *testing.T) {
	mockKafka := &mockKafkaClient{
		pollRecordsFunc: func(ctx context.Context, maxPollRecords int) kgo.Fetches {
			record := &kgo.Record{
				Value: []byte("invalid json"),
			}
			return kgo.Fetches{
				{
					Topics: []kgo.FetchTopic{
						{
							Topic: "test-topic",
							Partitions: []kgo.FetchPartition{
								{
									Partition: 0,
									Records:   []*kgo.Record{record},
								},
							},
						},
					},
				},
			}
		},
	}

	mockElastic := &mockElasticClient{}
	m := metrics.NewMetrics("test_invalid")
	consumer := NewConsumer(mockKafka, mockElastic, m)

	_, err := consumer.consumeBatch(context.Background())

	if err == nil {
		t.Fatal("Expected error for invalid JSON, got nil")
	}
}

func TestConsumeBatch_ElasticError(t *testing.T) {
	logEvent := domain.LogEvent{
		Ok:   true,
		Tick: 1000,
		Id:   12345,
		Hash: "test-hash",
	}

	logEventJSON, _ := json.Marshal(logEvent)

	mockKafka := &mockKafkaClient{
		pollRecordsFunc: func(ctx context.Context, maxPollRecords int) kgo.Fetches {
			record := &kgo.Record{
				Value: logEventJSON,
			}
			return kgo.Fetches{
				{
					Topics: []kgo.FetchTopic{
						{
							Topic: "test-topic",
							Partitions: []kgo.FetchPartition{
								{
									Partition: 0,
									Records:   []*kgo.Record{record},
								},
							},
						},
					},
				},
			}
		},
	}

	expectedErr := errors.New("elastic error")
	mockElastic := &mockElasticClient{
		bulkIndexFunc: func(ctx context.Context, data []*elastic.EsDocument) error {
			return expectedErr
		},
	}

	m := metrics.NewMetrics("test_elastic_err")
	consumer := NewConsumer(mockKafka, mockElastic, m)

	_, err := consumer.consumeBatch(context.Background())

	if err == nil {
		t.Fatal("Expected error from Elasticsearch, got nil")
	}
}

func TestConsumeBatch_CommitError(t *testing.T) {
	logEvent := domain.LogEvent{
		Ok:   true,
		Tick: 1000,
		Id:   12345,
		Hash: "test-hash",
	}

	logEventJSON, _ := json.Marshal(logEvent)

	expectedErr := errors.New("commit error")
	mockKafka := &mockKafkaClient{
		pollRecordsFunc: func(ctx context.Context, maxPollRecords int) kgo.Fetches {
			record := &kgo.Record{
				Value: logEventJSON,
			}
			return kgo.Fetches{
				{
					Topics: []kgo.FetchTopic{
						{
							Topic: "test-topic",
							Partitions: []kgo.FetchPartition{
								{
									Partition: 0,
									Records:   []*kgo.Record{record},
								},
							},
						},
					},
				},
			}
		},
		commitUncommittedOffsetsFunc: func(ctx context.Context) error {
			return expectedErr
		},
	}

	mockElastic := &mockElasticClient{}
	m := metrics.NewMetrics("test_commit_err")
	consumer := NewConsumer(mockKafka, mockElastic, m)

	_, err := consumer.consumeBatch(context.Background())

	if err == nil {
		t.Fatal("Expected commit error, got nil")
	}
}

func TestConsumeBatch_MultipleRecords(t *testing.T) {
	logEvent1 := domain.LogEvent{
		Epoch: 100,
		Tick:  1000,
		Id:    1,
		Hash:  "hash1",
	}
	logEvent2 := domain.LogEvent{
		Epoch: 100,
		Tick:  1001,
		Id:    2,
		Hash:  "hash2",
	}

	logEvent1JSON, _ := json.Marshal(logEvent1)
	logEvent2JSON, _ := json.Marshal(logEvent2)

	mockKafka := &mockKafkaClient{
		pollRecordsFunc: func(ctx context.Context, maxPollRecords int) kgo.Fetches {
			return kgo.Fetches{
				{
					Topics: []kgo.FetchTopic{
						{
							Topic: "test-topic",
							Partitions: []kgo.FetchPartition{
								{
									Partition: 0,
									Records: []*kgo.Record{
										{Value: logEvent1JSON},
										{Value: logEvent2JSON},
									},
								},
							},
						},
					},
				},
			}
		},
	}

	var indexedDocs []*elastic.EsDocument
	mockElastic := &mockElasticClient{
		bulkIndexFunc: func(ctx context.Context, data []*elastic.EsDocument) error {
			indexedDocs = data
			return nil
		},
	}

	m := metrics.NewMetrics("test_multiple")
	consumer := NewConsumer(mockKafka, mockElastic, m)

	count, err := consumer.consumeBatch(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if count != 2 {
		t.Errorf("Expected count 2, got: %d", count)
	}

	if len(indexedDocs) != 2 {
		t.Fatalf("Expected 2 documents to be indexed, got: %d", len(indexedDocs))
	}

	if consumer.currentTick != 1001 {
		t.Errorf("Expected currentTick to be 1001 (highest), got: %d", consumer.currentTick)
	}
}

func TestConsume_ContextCancellation(t *testing.T) {
	mockKafka := &mockKafkaClient{
		pollRecordsFunc: func(ctx context.Context, maxPollRecords int) kgo.Fetches {
			return kgo.Fetches{{Topics: []kgo.FetchTopic{}}}
		},
	}

	mockElastic := &mockElasticClient{}
	m := metrics.NewMetrics("test_context_cancel")
	consumer := NewConsumer(mockKafka, mockElastic, m)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := consumer.Consume(ctx)

	if err != nil {
		t.Errorf("Expected no error on context cancellation, got: %v", err)
	}
}

func TestUnmarshallLogEvent_Success(t *testing.T) {
	logEvent := domain.LogEvent{
		Ok:        true,
		Epoch:     100,
		Tick:      1000,
		Id:        12345,
		Hash:      "test-hash",
		Type:      1,
		TypeName:  "TestEvent",
		Timestamp: "2024-01-01T00:00:00Z",
		TxHash:    "tx-hash",
		BodySize:  100,
		Body:      "test-body",
	}

	logEventJSON, _ := json.Marshal(logEvent)

	record := &kgo.Record{
		Value: logEventJSON,
	}

	var result domain.LogEvent
	err := unmarshallLogEvent(record, &result)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if result.Id != logEvent.Id {
		t.Errorf("Expected Id %d, got: %d", logEvent.Id, result.Id)
	}

	if result.Hash != logEvent.Hash {
		t.Errorf("Expected Hash %s, got: %s", logEvent.Hash, result.Hash)
	}
}

func TestUnmarshallLogEvent_InvalidJSON(t *testing.T) {
	record := &kgo.Record{
		Value: []byte("invalid json"),
	}

	var result domain.LogEvent
	err := unmarshallLogEvent(record, &result)

	if err == nil {
		t.Fatal("Expected error for invalid JSON, got nil")
	}
}
