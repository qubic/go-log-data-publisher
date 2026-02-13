package consume

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	// Create JSON as it would come from Kafka (numbers are float64 after unmarshaling)
	logEventJSON := []byte(`{
		"epoch": 100,
		"tickNumber": 1000,
		"index": 1,
		"type": 0,
		"emittingContractIndex": 0,
		"logId": 12345,
		"logDigest": "test-digest",
		"transactionHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafxib",
		"timestamp": 1704067200,
		"bodySize": 100,
		"body": {
			"source": "SOURCE_ADDRESS",
			"destination": "DEST_ADDRESS",
			"amount": 1000
		}
	}`)

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
	consumer := NewConsumer(mockKafka, mockElastic, m, map[uint64][]int16{0: {0, 1, 2, 3, 8, 13}})

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

	// Verify the document ID (epoch-logId format)
	expectedID := "100-12345"
	if indexedDocs[0].Id != expectedID {
		t.Errorf("Expected document ID %s, got: %s", expectedID, indexedDocs[0].Id)
	}

	// Verify the JSON contains converted fields (not the original body map)
	var elasticDoc map[string]any
	err = json.Unmarshal(indexedDocs[0].Payload, &elasticDoc)
	if err != nil {
		t.Fatalf("Failed to unmarshal indexed document: %v", err)
	}

	// Check that body fields were flattened to top-level
	if elasticDoc["source"] != "SOURCE_ADDRESS" {
		t.Errorf("Expected source=SOURCEADDRESS, got: %v", elasticDoc["source"])
	}
	if elasticDoc["destination"] != "DEST_ADDRESS" {
		t.Errorf("Expected destination=DESTADDRESS, got: %v", elasticDoc["destination"])
	}
	if elasticDoc["amount"] != float64(1000) {
		t.Errorf("Expected amount=1000, got: %v", elasticDoc["amount"])
	}

	// Verify the original body map is not in the elastic document
	if _, exists := elasticDoc["body"]; exists {
		t.Error("Expected 'body' field to not exist in elastic document")
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
	consumer := NewConsumer(mockKafka, mockElastic, m, map[uint64][]int16{0: {0, 1, 2, 3, 8, 13}})

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
	consumer := NewConsumer(mockKafka, mockElastic, m, map[uint64][]int16{0: {0, 1, 2, 3, 8, 13}})

	_, err := consumer.consumeBatch(context.Background())

	if err == nil {
		t.Fatal("Expected error for invalid JSON, got nil")
	}
}

func TestConsumeBatch_ConversionError(t *testing.T) {
	// Test with invalid body field (unknown key)
	logEvent := domain.LogEvent{
		Epoch:           100,
		TickNumber:      1000,
		LogId:           12345,
		LogDigest:       "test-digest",
		TransactionHash: "tx-hash",
		Timestamp:       1704067200,
		Body: map[string]any{
			"unknownField": "invalid",
		},
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

	mockElastic := &mockElasticClient{}
	m := metrics.NewMetrics("test_conversion_error")
	consumer := NewConsumer(mockKafka, mockElastic, m, map[uint64][]int16{0: {0, 1, 2, 3, 8, 13}})

	_, err := consumer.consumeBatch(context.Background())

	if err == nil {
		t.Fatal("Expected conversion error for unknown body field, got nil")
	}
}

func TestConsumeBatch_ElasticError(t *testing.T) {
	logEvent := domain.LogEvent{
		Epoch:           100,
		TickNumber:      1000,
		LogId:           12345,
		LogDigest:       "test-digest",
		TransactionHash: "tx-hash",
		Timestamp:       1704067200,
		Body:            map[string]any{},
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
	consumer := NewConsumer(mockKafka, mockElastic, m, map[uint64][]int16{0: {0, 1, 2, 3, 8, 13}})

	_, err := consumer.consumeBatch(context.Background())

	if err == nil {
		t.Fatal("Expected error from Elasticsearch, got nil")
	}
}

func TestConsumeBatch_CommitError(t *testing.T) {
	logEvent := domain.LogEvent{
		Epoch:           100,
		TickNumber:      1000,
		LogId:           12345,
		LogDigest:       "test-digest",
		TransactionHash: "tx-hash",
		Timestamp:       1704067200,
		Body:            map[string]any{},
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
	consumer := NewConsumer(mockKafka, mockElastic, m, map[uint64][]int16{0: {0, 1, 2, 3, 8, 13}})

	_, err := consumer.consumeBatch(context.Background())

	if err == nil {
		t.Fatal("Expected commit error, got nil")
	}
}

func TestConsumeBatch_MultipleRecords(t *testing.T) {
	logEvent1 := domain.LogEvent{
		Epoch:           100,
		TickNumber:      1000,
		Type:            0,
		LogId:           1,
		LogDigest:       "digest1",
		TransactionHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafxib",
		Timestamp:       1704067200,
		Body: map[string]any{
			"source":      "TEST",
			"destination": "TEST",
			"amount":      float64(100),
		},
	}
	logEvent2 := domain.LogEvent{
		Epoch:           100,
		TickNumber:      1001,
		Type:            0,
		LogId:           2,
		LogDigest:       "digest2",
		TransactionHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafxib",
		Timestamp:       1704067201,
		Body: map[string]any{
			"source":      "TEST",
			"destination": "TEST",
			"amount":      float64(100),
		},
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
	consumer := NewConsumer(mockKafka, mockElastic, m, map[uint64][]int16{0: {0, 1, 2, 3, 8, 13}})

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
	consumer := NewConsumer(mockKafka, mockElastic, m, map[uint64][]int16{0: {0, 1, 2, 3, 8, 13}})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := consumer.Consume(ctx)

	if err != nil {
		t.Errorf("Expected no error on context cancellation, got: %v", err)
	}
}

func TestUnmarshallLogEvent_Success(t *testing.T) {
	logEvent := domain.LogEvent{
		Epoch:                 100,
		TickNumber:            1000,
		Index:                 1,
		Type:                  1,
		EmittingContractIndex: 5,
		LogId:                 12345,
		LogDigest:             "test-digest",
		TransactionHash:       "tx-hash",
		Timestamp:             1704067200,
		BodySize:              100,
		Body: map[string]any{
			"source": "SOURCE_ADDRESS",
		},
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

	if result.LogId != logEvent.LogId {
		t.Errorf("Expected LogId %d, got: %d", logEvent.LogId, result.LogId)
	}

	if result.LogDigest != logEvent.LogDigest {
		t.Errorf("Expected LogDigest %s, got: %s", logEvent.LogDigest, result.LogDigest)
	}

	if result.TickNumber != logEvent.TickNumber {
		t.Errorf("Expected TickNumber %d, got: %d", logEvent.TickNumber, result.TickNumber)
	}

	if result.TransactionHash != logEvent.TransactionHash {
		t.Errorf("Expected TransactionHash %s, got: %s", logEvent.TransactionHash, result.TransactionHash)
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

func TestConsumeBatch_filterIfLogIsNotSupported(t *testing.T) {
	tests := []struct {
		name         string
		eventType    int16
		shouldFilter bool
	}{
		{
			name:         "Type 0 is supported",
			eventType:    0,
			shouldFilter: false,
		},
		{
			name:         "Type 1 is supported",
			eventType:    1,
			shouldFilter: false,
		},
		{
			name:         "Type 2 is supported",
			eventType:    2,
			shouldFilter: false,
		},
		{
			name:         "Type 3 is supported",
			eventType:    3,
			shouldFilter: false,
		},
		{
			name:         "Type 8 is supported",
			eventType:    8,
			shouldFilter: false,
		},
		{
			name:         "Type 13 is supported",
			eventType:    13,
			shouldFilter: false,
		},
		{
			name:         "Type 4 is NOT supported",
			eventType:    4,
			shouldFilter: true,
		},
		{
			name:         "Type 100 is NOT supported",
			eventType:    100,
			shouldFilter: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logEvent := domain.LogEvent{
				Epoch:           100,
				TickNumber:      1000,
				Type:            tt.eventType,
				LogId:           12345,
				LogDigest:       "test-digest",
				TransactionHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafxib",
				Timestamp:       1704067200,
				Body: map[string]any{
					"source":                 "SOURCE",
					"destination":            "DESTINATION",
					"assetName":              "ASSET_NAME",
					"assetIssuer":            "ASSET_ISSUER",
					"amount":                 float64(100),
					"numberOfShares":         float64(101),
					"managingContractIndex":  float64(102),
					"numberOfDecimalPlaces":  float64(103),
					"unitOfMeasurement":      "0000000",
					"contractIndexBurnedFor": float64(104),
					"contractIndex":          float64(105),
					"deductedAmount":         float64(106),
					"remainingAmount":        float64(107),
				},
				Index: 1,
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

			m := metrics.NewMetrics("test_supported_filter_" + tt.name)
			consumer := NewConsumer(mockKafka, mockElastic, m, map[uint64][]int16{0: {0, 1, 2, 3, 8, 13}})

			count, err := consumer.consumeBatch(context.Background())
			if err != nil {
				t.Fatalf("Expected no error, got: %v", err)
			}

			if tt.shouldFilter {
				if count != 0 {
					t.Errorf("Expected count 0 (filtered), got: %d", count)
				}
				if len(indexedDocs) != 0 {
					t.Errorf("Expected 0 documents indexed, got: %d", len(indexedDocs))
				}
			} else {
				if count != 1 {
					t.Errorf("Expected count 1 (not filtered), got: %d", count)
				}
				if len(indexedDocs) != 1 {
					t.Errorf("Expected 1 document indexed, got: %d", len(indexedDocs))
				}
			}
		})
	}
}

func TestConsumeBatch_FilterEmptyTransfers(t *testing.T) {
	// Create events that should be filtered (Type=0, ContractIndex=0, Amount=0)
	logEvent := domain.LogEvent{
		Epoch:                 100,
		TickNumber:            1000,
		Type:                  0,
		EmittingContractIndex: 0,
		LogId:                 12345,
		LogDigest:             "test-digest",
		TransactionHash:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafxib",
		Timestamp:             1704067200,
		Body: map[string]any{
			"source":      "SOURCE",
			"destination": "DESTINATION",
			"amount":      float64(0),
		},
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

	m := metrics.NewMetrics("test_filter")
	consumer := NewConsumer(mockKafka, mockElastic, m, map[uint64][]int16{0: {0, 1, 2, 3, 8, 13}})

	count, err := consumer.consumeBatch(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Event should be filtered, so count should be 0
	if count != 0 {
		t.Errorf("Expected count 0 (filtered), got: %d", count)
	}

	if len(indexedDocs) != 0 {
		t.Errorf("Expected 0 documents indexed (filtered), got: %d", len(indexedDocs))
	}
}

func TestConsumeBatch_FilterEdgeCases(t *testing.T) {
	tests := []struct {
		name                  string
		typ                   int16
		emittingContractIndex uint64
		amount                uint64
		shouldFilter          bool
	}{
		{
			name:                  "Amount non-zero, should not filter",
			typ:                   0,
			emittingContractIndex: 0,
			amount:                100,
			shouldFilter:          false,
		},
		{
			name:                  "All zero, should filter",
			typ:                   0,
			emittingContractIndex: 0,
			amount:                0,
			shouldFilter:          true,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := map[string]any{
				"source":      "SOURCE",
				"destination": "DESTINATION",
			}
			body["amount"] = float64(tt.amount)

			logEvent := domain.LogEvent{
				Epoch:                 100,
				TickNumber:            1000,
				EmittingContractIndex: tt.emittingContractIndex,
				Type:                  tt.typ,
				LogId:                 12345,
				LogDigest:             "test-digest",
				TransactionHash:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafxib",
				Timestamp:             1704067200,
				Body:                  body,
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

			m := metrics.NewMetrics(fmt.Sprintf("test_filter_edge_%d", i))
			consumer := NewConsumer(mockKafka, mockElastic, m, map[uint64][]int16{0: {0, 1, 2, 3, 8, 13}})

			count, err := consumer.consumeBatch(context.Background())

			if err != nil {
				t.Fatalf("Expected no error, got: %v", err)
			}

			if tt.shouldFilter {
				if count != 0 {
					t.Errorf("Expected count 0 (filtered), got: %d", count)
				}
				if len(indexedDocs) != 0 {
					t.Errorf("Expected 0 documents (filtered), got: %d", len(indexedDocs))
				}
			} else {
				if count != 1 {
					t.Errorf("Expected count 1 (not filtered), got: %d", count)
				}
				if len(indexedDocs) != 1 {
					t.Errorf("Expected 1 document (not filtered), got: %d", len(indexedDocs))
				}
			}
		})
	}
}

func TestConsumeBatch_IDUniqueness(t *testing.T) {
	// Test that IDs are unique even with potential collision patterns
	const validTxHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafxib"
	logEvent1 := domain.LogEvent{
		Epoch:           1,
		TickNumber:      1000,
		Type:            0,
		LogId:           23,
		LogDigest:       "digest1",
		TransactionHash: validTxHash,
		Timestamp:       1704067200,
		Body: map[string]any{
			"amount":      float64(100),
			"source":      "SOURCE",
			"destination": "DESTINATION",
		},
	}
	logEvent2 := domain.LogEvent{
		Epoch:           12,
		TickNumber:      1001,
		Type:            0,
		LogId:           3,
		LogDigest:       "digest2",
		TransactionHash: validTxHash,
		Timestamp:       1704067201,
		Body: map[string]any{
			"amount":      float64(100),
			"source":      "SOURCE",
			"destination": "DESTINATION",
		},
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

	m := metrics.NewMetrics("test_id_uniqueness")
	consumer := NewConsumer(mockKafka, mockElastic, m, map[uint64][]int16{0: {0, 1, 2, 3, 8, 13}})

	count, err := consumer.consumeBatch(context.Background())

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if count != 2 {
		t.Errorf("Expected count 2, got: %d", count)
	}

	if len(indexedDocs) != 2 {
		t.Fatalf("Expected 2 documents, got: %d", len(indexedDocs))
	}

	// Verify IDs are unique with separator
	expectedID1 := "1-23"
	expectedID2 := "12-3"

	if indexedDocs[0].Id != expectedID1 {
		t.Errorf("Expected first document ID %s, got: %s", expectedID1, indexedDocs[0].Id)
	}

	if indexedDocs[1].Id != expectedID2 {
		t.Errorf("Expected second document ID %s, got: %s", expectedID2, indexedDocs[1].Id)
	}

	// Verify they are different (no collision)
	if indexedDocs[0].Id == indexedDocs[1].Id {
		t.Errorf("Document IDs should be unique, but both are: %s", indexedDocs[0].Id)
	}
}
