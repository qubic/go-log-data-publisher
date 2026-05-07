package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/qubic/bob-events-bridge/internal/bob"
	"github.com/qubic/bob-events-bridge/internal/config"
	"github.com/qubic/bob-events-bridge/internal/kafka"
	"github.com/qubic/bob-events-bridge/internal/metrics"
	"github.com/qubic/bob-events-bridge/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"go.uber.org/zap"
)

// mockPublisher implements kafka.Publisher for testing
type mockPublisher struct {
	published []*kafka.EventMessage
	failErr   error
}

func (m *mockPublisher) PublishEvent(_ context.Context, msg *kafka.EventMessage) error {
	if m.failErr != nil {
		return m.failErr
	}
	m.published = append(m.published, msg)
	return nil
}

func (m *mockPublisher) PublishEvents(_ context.Context, msgs []*kafka.EventMessage) error {
	if m.failErr != nil {
		return m.failErr
	}
	m.published = append(m.published, msgs...)
	return nil
}

func (m *mockPublisher) Close() error { return nil }

// newTestProcessor creates a processor with real storage and mock publisher for testing
func newTestProcessor(t *testing.T, publisher kafka.Publisher) *Processor {
	t.Helper()
	tempDir := t.TempDir()
	logger := zap.NewNop()
	mgr, err := storage.NewManager(tempDir, 0, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })

	reg := prometheus.NewRegistry()
	m := metrics.NewBridgeMetrics(reg, "test")

	cfg := &config.Config{}
	return NewProcessor(cfg, mgr, logger, publisher, m)
}

func makeQuTransferBody(from, to string, amount int64) json.RawMessage {
	b, _ := json.Marshal(map[string]any{"from": from, "to": to, "amount": amount})
	return b
}

func TestHandleTickStreamResult_LastLogForTick_MultipleEvents(t *testing.T) {
	pub := &mockPublisher{}
	p := newTestProcessor(t, pub)
	p.currentEpoch = 1

	result := &bob.TickStreamResult{
		Epoch: 1,
		Tick:  100,
		Logs: []bob.LogPayload{
			{OK: true, Epoch: 1, Tick: 100, Type: 0, LogID: 1, LogDigest: "d1", BodySize: 10, Timestamp: 1000, TxHash: "tx1",
				Body: makeQuTransferBody("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", 100)},
			{OK: true, Epoch: 1, Tick: 100, Type: 0, LogID: 2, LogDigest: "d2", BodySize: 10, Timestamp: 1001, TxHash: "tx2",
				Body: makeQuTransferBody("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC", 200)},
			{OK: true, Epoch: 1, Tick: 100, Type: 0, LogID: 3, LogDigest: "d3", BodySize: 10, Timestamp: 1002, TxHash: "tx3",
				Body: makeQuTransferBody("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD", 300)},
		},
	}

	err := p.handleTickStreamResult(context.Background(), result)
	require.NoError(t, err)
	require.NotNil(t, p.pendingBatch)

	// Only last proto event should have LastLogForTick = true
	require.Len(t, p.pendingBatch.protoEvents, 3)
	assert.False(t, p.pendingBatch.protoEvents[0].LastLogForTick)
	assert.False(t, p.pendingBatch.protoEvents[1].LastLogForTick)
	assert.True(t, p.pendingBatch.protoEvents[2].LastLogForTick)

	// Only last kafka msg should have LastLogForTick = true
	require.Len(t, p.pendingBatch.kafkaMsgs, 3)
	assert.False(t, p.pendingBatch.kafkaMsgs[0].LastLogForTick)
	assert.False(t, p.pendingBatch.kafkaMsgs[1].LastLogForTick)
	assert.True(t, p.pendingBatch.kafkaMsgs[2].LastLogForTick)
}

func TestHandleTickStreamResult_LastLogForTick_SingleEvent(t *testing.T) {
	pub := &mockPublisher{}
	p := newTestProcessor(t, pub)
	p.currentEpoch = 1

	result := &bob.TickStreamResult{
		Epoch: 1,
		Tick:  100,
		Logs: []bob.LogPayload{
			{OK: true, Epoch: 1, Tick: 100, Type: 0, LogID: 1, LogDigest: "d1", BodySize: 10, Timestamp: 1000, TxHash: "tx1",
				Body: makeQuTransferBody("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", 100)},
		},
	}

	err := p.handleTickStreamResult(context.Background(), result)
	require.NoError(t, err)
	require.NotNil(t, p.pendingBatch)

	require.Len(t, p.pendingBatch.protoEvents, 1)
	assert.True(t, p.pendingBatch.protoEvents[0].LastLogForTick)

	require.Len(t, p.pendingBatch.kafkaMsgs, 1)
	assert.True(t, p.pendingBatch.kafkaMsgs[0].LastLogForTick)
}

func TestHandleTickStreamResult_LastLogForTick_MixedOKAndNonOK(t *testing.T) {
	pub := &mockPublisher{}
	p := newTestProcessor(t, pub)
	p.currentEpoch = 1

	result := &bob.TickStreamResult{
		Epoch: 1,
		Tick:  100,
		Logs: []bob.LogPayload{
			{OK: true, Epoch: 1, Tick: 100, Type: 0, LogID: 1, LogDigest: "d1", BodySize: 10, Timestamp: 1000, TxHash: "tx1",
				Body: makeQuTransferBody("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", 100)},
			{OK: false, Epoch: 1, Tick: 100, Type: 0, LogID: 2}, // non-OK, skipped
			{OK: true, Epoch: 1, Tick: 100, Type: 0, LogID: 3, LogDigest: "d3", BodySize: 10, Timestamp: 1002, TxHash: "tx3",
				Body: makeQuTransferBody("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC", 200)},
			{OK: false, Epoch: 1, Tick: 100, Type: 0, LogID: 4}, // non-OK, skipped — last in tick but not added
		},
	}

	err := p.handleTickStreamResult(context.Background(), result)
	require.NoError(t, err)
	require.NotNil(t, p.pendingBatch)

	// Only 2 valid events should be in batch
	require.Len(t, p.pendingBatch.protoEvents, 2)
	assert.False(t, p.pendingBatch.protoEvents[0].LastLogForTick)
	assert.True(t, p.pendingBatch.protoEvents[1].LastLogForTick, "Last valid event should have LastLogForTick=true")

	require.Len(t, p.pendingBatch.kafkaMsgs, 2)
	assert.False(t, p.pendingBatch.kafkaMsgs[0].LastLogForTick)
	assert.True(t, p.pendingBatch.kafkaMsgs[1].LastLogForTick)
}

func TestHandleTickStreamResult_LastLogForTick_NoPublisher(t *testing.T) {
	// Test without Kafka publisher (nil)
	p := newTestProcessor(t, nil)
	p.currentEpoch = 1

	result := &bob.TickStreamResult{
		Epoch: 1,
		Tick:  100,
		Logs: []bob.LogPayload{
			{OK: true, Epoch: 1, Tick: 100, Type: 0, LogID: 1, LogDigest: "d1", BodySize: 10, Timestamp: 1000, TxHash: "tx1",
				Body: makeQuTransferBody("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", 100)},
			{OK: true, Epoch: 1, Tick: 100, Type: 0, LogID: 2, LogDigest: "d2", BodySize: 10, Timestamp: 1001, TxHash: "tx2",
				Body: makeQuTransferBody("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC", 200)},
		},
	}

	err := p.handleTickStreamResult(context.Background(), result)
	require.NoError(t, err)
	require.NotNil(t, p.pendingBatch)

	// Proto events should still have flag set
	require.Len(t, p.pendingBatch.protoEvents, 2)
	assert.False(t, p.pendingBatch.protoEvents[0].LastLogForTick)
	assert.True(t, p.pendingBatch.protoEvents[1].LastLogForTick)

	// No kafka messages when publisher is nil
	assert.Empty(t, p.pendingBatch.kafkaMsgs)
}

func TestIsNonRetriableKafkaError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "non-retriable kerr error",
			err:      kerr.TopicAuthorizationFailed,
			expected: true,
		},
		{
			name:     "wrapped non-retriable kerr error",
			err:      fmt.Errorf("test error: %w", kerr.TopicAuthorizationFailed),
			expected: true,
		},
		{
			name:     "retriable kerr error",
			err:      kerr.NotEnoughReplicas,
			expected: false,
		},
		{
			name:     "wrapped retriable kerr error",
			err:      fmt.Errorf("test error: %w", kerr.NotEnoughReplicas),
			expected: false,
		},
		{
			name:     "non-kafka error",
			err:      fmt.Errorf("some other error"),
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isNonRetriableKafkaError(tt.err))
		})
	}
}

func TestFlushBatch_NonRetriableKafkaError(t *testing.T) {
	pub := &mockPublisher{failErr: kerr.TopicAuthorizationFailed}
	p := newTestProcessor(t, pub)
	p.currentEpoch = 1

	result := &bob.TickStreamResult{
		Epoch: 1,
		Tick:  100,
		Logs: []bob.LogPayload{
			{OK: true, Epoch: 1, Tick: 100, Type: 0, LogID: 1, LogDigest: "d1", BodySize: 10, Timestamp: 1000, TxHash: "tx1",
				Body: makeQuTransferBody("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", 100)},
		},
	}

	err := p.handleTickStreamResult(context.Background(), result)
	require.NoError(t, err)
	require.NotNil(t, p.pendingBatch)

	err = p.flushBatch(context.Background())
	require.Error(t, err)

	// The kerr.Error should be unwrappable
	var kafkaErr *kerr.Error
	require.True(t, errors.As(err, &kafkaErr))
	assert.False(t, kafkaErr.Retriable)

	// State should NOT be updated
	assert.Equal(t, uint32(0), p.lastTick)
}

func TestFlushBatch_RetriableKafkaError(t *testing.T) {
	pub := &mockPublisher{failErr: kerr.NotEnoughReplicas}
	p := newTestProcessor(t, pub)
	p.currentEpoch = 1

	result := &bob.TickStreamResult{
		Epoch: 1,
		Tick:  100,
		Logs: []bob.LogPayload{
			{OK: true, Epoch: 1, Tick: 100, Type: 0, LogID: 1, LogDigest: "d1", BodySize: 10, Timestamp: 1000, TxHash: "tx1",
				Body: makeQuTransferBody("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", 100)},
		},
	}

	err := p.handleTickStreamResult(context.Background(), result)
	require.NoError(t, err)

	err = p.flushBatch(context.Background())
	require.Error(t, err)

	// Still a kerr.Error, but retriable
	var kafkaErr *kerr.Error
	require.True(t, errors.As(err, &kafkaErr))
	assert.True(t, kafkaErr.Retriable)

	// State should NOT be updated
	assert.Equal(t, uint32(0), p.lastTick)
}
