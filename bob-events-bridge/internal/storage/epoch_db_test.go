package storage

import (
	"errors"
	"testing"

	eventsbridge "github.com/qubic/bob-events-bridge/api/events-bridge/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func storeRange(t *testing.T, db *EpochDB, ticks []uint32, eventsPerTick int) {
	t.Helper()
	for _, tick := range ticks {
		for i := 0; i < eventsPerTick; i++ {
			ev := &eventsbridge.Event{
				Epoch:     db.Epoch(),
				Tick:      tick,
				LogId:     uint64(tick)*100 + uint64(i),
				EventType: 0,
			}
			require.NoError(t, db.StoreEvent(ev))
		}
	}
}

func collectTicks(events []*eventsbridge.Event) []uint32 {
	out := make([]uint32, len(events))
	for i, e := range events {
		out[i] = e.Tick
	}
	return out
}

func TestIterateEventsInRange_Unbounded(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewEpochDB(tempDir, 1)
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	storeRange(t, db, []uint32{100, 200, 300}, 2)

	var got []*eventsbridge.Event
	err = db.IterateEventsInRange(0, 0, func(e *eventsbridge.Event) error {
		got = append(got, e)
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, got, 6)
	// Order is by tick-padded key, so ascending tick.
	assert.Equal(t, []uint32{100, 100, 200, 200, 300, 300}, collectTicks(got))
}

func TestIterateEventsInRange_BoundedStart(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewEpochDB(tempDir, 1)
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	storeRange(t, db, []uint32{100, 200, 300}, 1)

	var got []*eventsbridge.Event
	err = db.IterateEventsInRange(200, 0, func(e *eventsbridge.Event) error {
		got = append(got, e)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []uint32{200, 300}, collectTicks(got))
}

func TestIterateEventsInRange_BoundedEnd(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewEpochDB(tempDir, 1)
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	storeRange(t, db, []uint32{100, 200, 300}, 1)

	var got []*eventsbridge.Event
	err = db.IterateEventsInRange(0, 200, func(e *eventsbridge.Event) error {
		got = append(got, e)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []uint32{100, 200}, collectTicks(got), "tickEnd is inclusive")
}

func TestIterateEventsInRange_BothBounds(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewEpochDB(tempDir, 1)
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	storeRange(t, db, []uint32{100, 150, 200, 250, 300}, 1)

	var got []*eventsbridge.Event
	err = db.IterateEventsInRange(150, 250, func(e *eventsbridge.Event) error {
		got = append(got, e)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []uint32{150, 200, 250}, collectTicks(got))
}

func TestIterateEventsInRange_ExactSingleTick(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewEpochDB(tempDir, 1)
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	storeRange(t, db, []uint32{100, 200, 300}, 3)

	var got []*eventsbridge.Event
	err = db.IterateEventsInRange(200, 200, func(e *eventsbridge.Event) error {
		got = append(got, e)
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, got, 3)
	for _, e := range got {
		assert.Equal(t, uint32(200), e.Tick)
	}
}

func TestIterateEventsInRange_EmptyRange(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewEpochDB(tempDir, 1)
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	storeRange(t, db, []uint32{100, 200}, 1)

	var got []*eventsbridge.Event
	err = db.IterateEventsInRange(500, 600, func(e *eventsbridge.Event) error {
		got = append(got, e)
		return nil
	})
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestIterateEventsInRange_EmptyDB(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewEpochDB(tempDir, 1)
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	called := false
	err = db.IterateEventsInRange(0, 0, func(_ *eventsbridge.Event) error {
		called = true
		return nil
	})
	require.NoError(t, err)
	assert.False(t, called)
}

func TestIterateEventsInRange_CallbackErrorStopsIteration(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewEpochDB(tempDir, 1)
	require.NoError(t, err)
	defer db.Close() //nolint:errcheck

	storeRange(t, db, []uint32{100, 200, 300}, 1)

	stopErr := errors.New("stop here")
	count := 0
	err = db.IterateEventsInRange(0, 0, func(_ *eventsbridge.Event) error {
		count++
		if count == 2 {
			return stopErr
		}
		return nil
	})
	require.ErrorIs(t, err, stopErr)
	assert.Equal(t, 2, count, "iteration must stop on callback error")
}
