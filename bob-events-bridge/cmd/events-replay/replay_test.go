package main

import (
	"context"
	"errors"
	"testing"

	eventsbridge "github.com/qubic/bob-events-bridge/api/events-bridge/v1"
	"github.com/qubic/bob-events-bridge/internal/bob"
	"github.com/qubic/bob-events-bridge/internal/kafka"
	"github.com/qubic/bob-events-bridge/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"
)

// makeQuTransferEvent builds an Event in stored (bob) body format.
func makeQuTransferEvent(t *testing.T, tick uint32, logID uint64, indexInTick uint32, last bool) *eventsbridge.Event {
	t.Helper()
	body, err := structpb.NewStruct(map[string]any{
		"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"amount": float64(logID * 10),
	})
	require.NoError(t, err)
	return &eventsbridge.Event{
		LogId:          logID,
		Tick:           tick,
		Epoch:          1,
		EventType:      bob.LogTypeQuTransfer,
		TxHash:         "TXHASH",
		Timestamp:      1718461800,
		Body:           body,
		IndexInTick:    indexInTick,
		LogDigest:      "digest",
		LastLogForTick: last,
	}
}

func openTestDB(t *testing.T) *storage.EpochDB {
	t.Helper()
	db, err := storage.NewEpochDB(t.TempDir(), 1)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestRunReplay_FullEpoch(t *testing.T) {
	db := openTestDB(t)

	// Three ticks, each with 2 events.
	require.NoError(t, db.StoreEvent(makeQuTransferEvent(t, 100, 1, 0, false)))
	require.NoError(t, db.StoreEvent(makeQuTransferEvent(t, 100, 2, 1, true)))
	require.NoError(t, db.StoreEvent(makeQuTransferEvent(t, 200, 3, 0, false)))
	require.NoError(t, db.StoreEvent(makeQuTransferEvent(t, 200, 4, 1, true)))
	require.NoError(t, db.StoreEvent(makeQuTransferEvent(t, 300, 5, 0, true)))

	pub := kafka.NewMockPublisher()

	n, err := runReplay(context.Background(), db, pub, 0, 0, zap.NewNop())
	require.NoError(t, err)
	assert.Equal(t, uint64(5), n)

	msgs := pub.Messages()
	require.Len(t, msgs, 5)

	// Order preserved (ascending tick, ascending key within tick).
	assert.Equal(t, []uint32{100, 100, 200, 200, 300}, []uint32{
		msgs[0].TickNumber, msgs[1].TickNumber, msgs[2].TickNumber, msgs[3].TickNumber, msgs[4].TickNumber,
	})

	// Body must be transformed to kafka format.
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", msgs[0].Body["source"])
	assert.NotContains(t, msgs[0].Body, "from")

	// BodySize=0 on replay
	for _, m := range msgs {
		assert.Equal(t, uint32(0), m.BodySize)
	}
}

func TestRunReplay_TickRange(t *testing.T) {
	db := openTestDB(t)

	require.NoError(t, db.StoreEvent(makeQuTransferEvent(t, 100, 1, 0, true)))
	require.NoError(t, db.StoreEvent(makeQuTransferEvent(t, 200, 2, 0, true)))
	require.NoError(t, db.StoreEvent(makeQuTransferEvent(t, 300, 3, 0, true)))
	require.NoError(t, db.StoreEvent(makeQuTransferEvent(t, 400, 4, 0, true)))

	pub := kafka.NewMockPublisher()

	n, err := runReplay(context.Background(), db, pub, 200, 300, zap.NewNop())
	require.NoError(t, err)
	assert.Equal(t, uint64(2), n)

	msgs := pub.Messages()
	require.Len(t, msgs, 2)
	assert.Equal(t, uint32(200), msgs[0].TickNumber)
	assert.Equal(t, uint32(300), msgs[1].TickNumber)
}

func TestRunReplay_EmptyDB(t *testing.T) {
	db := openTestDB(t)
	pub := kafka.NewMockPublisher()

	n, err := runReplay(context.Background(), db, pub, 0, 0, zap.NewNop())
	require.NoError(t, err)
	assert.Equal(t, uint64(0), n)
	assert.Empty(t, pub.Messages())
}

func TestRunReplay_PublisherErrorPropagates(t *testing.T) {
	db := openTestDB(t)
	require.NoError(t, db.StoreEvent(makeQuTransferEvent(t, 100, 1, 0, false)))
	require.NoError(t, db.StoreEvent(makeQuTransferEvent(t, 200, 2, 0, true)))

	pub := kafka.NewMockPublisher()
	pub.SetFailNext(errors.New("kafka boom"))

	_, err := runReplay(context.Background(), db, pub, 0, 0, zap.NewNop())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to publish batch")
}

func TestRunReplay_ContextCancellationStopsIteration(t *testing.T) {
	db := openTestDB(t)
	for i := uint32(1); i <= 10; i++ {
		require.NoError(t, db.StoreEvent(makeQuTransferEvent(t, i*10, uint64(i), 0, true)))
	}

	pub := kafka.NewMockPublisher()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before the run

	_, err := runReplay(ctx, db, pub, 0, 0, zap.NewNop())
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, pub.Messages(), "no batch should flush after immediate cancellation")
}

func TestRunReplay_PerTickBatching(t *testing.T) {
	// Verify per-tick flush by tracking batch sizes — events on the same tick
	// flush together; tick boundary causes a flush before the new tick events.
	db := openTestDB(t)

	require.NoError(t, db.StoreEvent(makeQuTransferEvent(t, 100, 1, 0, false)))
	require.NoError(t, db.StoreEvent(makeQuTransferEvent(t, 100, 2, 1, false)))
	require.NoError(t, db.StoreEvent(makeQuTransferEvent(t, 100, 3, 2, true)))
	require.NoError(t, db.StoreEvent(makeQuTransferEvent(t, 200, 4, 0, true)))

	pub := &batchTrackingPublisher{}
	_, err := runReplay(context.Background(), db, pub, 0, 0, zap.NewNop())
	require.NoError(t, err)

	// Two flushes: one per tick.
	assert.Equal(t, []int{3, 1}, pub.batchSizes)
}

// batchTrackingPublisher records the size of each PublishEvents call.
type batchTrackingPublisher struct {
	batchSizes []int
}

func (p *batchTrackingPublisher) PublishEvent(_ context.Context, _ *kafka.EventMessage) error {
	return errors.New("not used")
}

func (p *batchTrackingPublisher) PublishEvents(_ context.Context, msgs []*kafka.EventMessage) error {
	p.batchSizes = append(p.batchSizes, len(msgs))
	return nil
}

func (p *batchTrackingPublisher) Close() error { return nil }
