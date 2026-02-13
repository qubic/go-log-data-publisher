package storage

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	eventsbridge "github.com/qubic/bob-events-bridge/api/events-bridge/v1"
	"go.uber.org/zap"
)

// createTestEvent creates a test event with given parameters
func createTestEvent(epoch, tick uint32, logID uint64) *eventsbridge.Event {
	return &eventsbridge.Event{
		Epoch:     epoch,
		Tick:      tick,
		LogId:     logID,
		EventType: 0, // qu_transfer
		TxHash:    fmt.Sprintf("hash_%d_%d", tick, logID),
		LogDigest: fmt.Sprintf("digest_%d_%d", tick, logID),
		Timestamp: 1704067200,
		Body:      nil,
	}
}

// createTestManager creates a manager with the given temp directory and no-op logger
func createTestManager(t *testing.T, basePath string) *Manager {
	t.Helper()
	logger := zap.NewNop()
	manager, err := NewManager(basePath, logger)
	require.NoError(t, err, "Failed to create manager")
	return manager
}

func TestEpochMigration(t *testing.T) {
	// Create temp directory
	tempDir := t.TempDir()

	// Create manager
	manager := createTestManager(t, tempDir)
	defer manager.Close() //nolint:errcheck

	// Store events in epoch 1 (ticks 100-105)
	for i := uint32(0); i < 6; i++ {
		event := createTestEvent(1, 100+i, uint64(i+1))
		err := manager.StoreEvent(event)
		require.NoError(t, err, "Failed to store event in epoch 1")
	}

	// Store events in epoch 2 (ticks 200-205)
	for i := uint32(0); i < 6; i++ {
		event := createTestEvent(2, 200+i, uint64(i+10))
		err := manager.StoreEvent(event)
		require.NoError(t, err, "Failed to store event in epoch 2")
	}

	// Assert: Folder for epoch 1 exists
	epoch1Path := filepath.Join(tempDir, "epochs", "1")
	require.DirExists(t, epoch1Path, "Epoch 1 folder should exist")

	// Assert: Folder for epoch 2 exists
	epoch2Path := filepath.Join(tempDir, "epochs", "2")
	require.DirExists(t, epoch2Path, "Epoch 2 folder should exist")

	// Assert: GetAvailableEpochs returns [1, 2]
	epochs := manager.GetAvailableEpochs()
	require.Len(t, epochs, 2, "Expected 2 epochs")
	require.Equal(t, uint32(1), epochs[0])
	require.Equal(t, uint32(2), epochs[1])

	// Assert: Events in epoch 1 have correct epoch field
	_, events, err := manager.GetEventsForTick(100)
	require.NoError(t, err, "Failed to get events for tick 100")
	require.NotEmpty(t, events, "Expected events for tick 100")
	for _, event := range events {
		require.Equal(t, uint32(1), event.Epoch, "Expected event epoch 1")
	}
}

func TestCrossEpochRetrieval(t *testing.T) {
	// Create temp directory
	tempDir := t.TempDir()

	// Create manager
	manager := createTestManager(t, tempDir)
	defer manager.Close() //nolint:errcheck

	// Store events: epoch 1 tick 100, epoch 1 tick 101
	err := manager.StoreEvent(createTestEvent(1, 100, 1))
	require.NoError(t, err, "Failed to store event")
	err = manager.StoreEvent(createTestEvent(1, 101, 2))
	require.NoError(t, err, "Failed to store event")

	// Store events: epoch 2 tick 200, epoch 2 tick 201
	err = manager.StoreEvent(createTestEvent(2, 200, 3))
	require.NoError(t, err, "Failed to store event")
	err = manager.StoreEvent(createTestEvent(2, 201, 4))
	require.NoError(t, err, "Failed to store event")

	// Test: GetEventsForTick(100) - expect epoch=1, events returned
	epoch, events, err := manager.GetEventsForTick(100)
	require.NoError(t, err, "Failed to get events for tick 100")
	require.Equal(t, uint32(1), epoch, "Expected epoch 1 for tick 100")
	require.Len(t, events, 1, "Expected 1 event for tick 100")

	// Test: GetEventsForTick(200) - expect epoch=2, events returned
	epoch, events, err = manager.GetEventsForTick(200)
	require.NoError(t, err, "Failed to get events for tick 200")
	require.Equal(t, uint32(2), epoch, "Expected epoch 2 for tick 200")
	require.Len(t, events, 1, "Expected 1 event for tick 200")

	// Test: GetEventsForTick(999) - expect epoch=0, no events (tick not found)
	epoch, events, err = manager.GetEventsForTick(999)
	require.NoError(t, err, "Failed to get events for tick 999")
	require.Equal(t, uint32(0), epoch, "Expected epoch 0 for tick 999 (not found)")
	require.Empty(t, events, "Expected 0 events for tick 999")
}

func TestManagerDiscovery(t *testing.T) {
	// Create temp directory
	tempDir := t.TempDir()

	// Create first manager
	manager1 := createTestManager(t, tempDir)

	// Store events in epochs 1 and 2
	err := manager1.StoreEvent(createTestEvent(1, 100, 1))
	require.NoError(t, err, "Failed to store event")
	err = manager1.StoreEvent(createTestEvent(1, 101, 2))
	require.NoError(t, err, "Failed to store event")
	err = manager1.StoreEvent(createTestEvent(2, 200, 3))
	require.NoError(t, err, "Failed to store event")

	// Close first manager (simulates restart)
	err = manager1.Close()
	require.NoError(t, err, "Failed to close manager")

	// Create new manager with same base path (simulates restart)
	manager2 := createTestManager(t, tempDir)
	defer manager2.Close() //nolint:errcheck

	// Assert: Both epochs are discovered
	epochs := manager2.GetAvailableEpochs()
	require.Len(t, epochs, 2, "Expected 2 epochs discovered")
	require.Equal(t, uint32(1), epochs[0])
	require.Equal(t, uint32(2), epochs[1])

	// Assert: Events can be retrieved from epoch 1
	epoch, events, err := manager2.GetEventsForTick(100)
	require.NoError(t, err, "Failed to get events for tick 100")
	require.Equal(t, uint32(1), epoch)
	require.Len(t, events, 1)

	// Assert: Events can be retrieved from epoch 2
	epoch, events, err = manager2.GetEventsForTick(200)
	require.NoError(t, err, "Failed to get events for tick 200")
	require.Equal(t, uint32(2), epoch)
	require.Len(t, events, 1)
}

func TestGetOrCreateEpochDB(t *testing.T) {
	// Create temp directory
	tempDir := t.TempDir()

	// Create manager
	manager := createTestManager(t, tempDir)
	defer manager.Close() //nolint:errcheck

	// Call GetOrCreateEpochDB(1) - creates new
	db1, err := manager.GetOrCreateEpochDB(1)
	require.NoError(t, err, "Failed to get/create epoch db 1")
	require.NotNil(t, db1, "Expected non-nil db for epoch 1")

	// Call GetOrCreateEpochDB(1) again - returns same instance
	db1Again, err := manager.GetOrCreateEpochDB(1)
	require.NoError(t, err, "Failed to get epoch db 1 again")
	require.Same(t, db1, db1Again, "Expected same db instance for epoch 1")

	// Call GetOrCreateEpochDB(2) - creates new for epoch 2
	db2, err := manager.GetOrCreateEpochDB(2)
	require.NoError(t, err, "Failed to get/create epoch db 2")
	require.NotNil(t, db2, "Expected non-nil db for epoch 2")

	// Assert: db1 and db2 are different instances
	require.NotSame(t, db1, db2, "Expected different db instances for epoch 1 and 2")

	// Verify epochs match
	require.Equal(t, uint32(1), db1.Epoch())
	require.Equal(t, uint32(2), db2.Epoch())
}

func TestStateUpdatedOnStore(t *testing.T) {
	// Create temp directory
	tempDir := t.TempDir()

	// Create manager
	manager := createTestManager(t, tempDir)
	defer manager.Close() //nolint:errcheck

	// Store event in epoch 1, tick 100, logID 1
	err := manager.StoreEvent(createTestEvent(1, 100, 1))
	require.NoError(t, err, "Failed to store event")

	// Verify state after first event
	state, err := manager.LoadState()
	require.NoError(t, err, "Failed to load state")
	require.Equal(t, uint32(1), state.CurrentEpoch)
	require.Equal(t, uint32(100), state.LastTick)
	require.Equal(t, int64(1), state.LastLogID)

	// Store event in epoch 1, tick 105, logID 5
	err = manager.StoreEvent(createTestEvent(1, 105, 5))
	require.NoError(t, err, "Failed to store event")

	// Verify state after second event
	state, err = manager.LoadState()
	require.NoError(t, err, "Failed to load state")
	require.Equal(t, uint32(1), state.CurrentEpoch)
	require.Equal(t, uint32(105), state.LastTick)
	require.Equal(t, int64(5), state.LastLogID)

	// Store event in epoch 2, tick 200, logID 10
	err = manager.StoreEvent(createTestEvent(2, 200, 10))
	require.NoError(t, err, "Failed to store event")

	// Verify final state
	state, err = manager.LoadState()
	require.NoError(t, err, "Failed to load state")
	require.Equal(t, uint32(2), state.CurrentEpoch)
	require.Equal(t, uint32(200), state.LastTick)
	require.Equal(t, int64(10), state.LastLogID)
}

func TestGetStatusMultipleEpochs(t *testing.T) {
	// Create temp directory
	tempDir := t.TempDir()

	// Create manager
	manager := createTestManager(t, tempDir)
	defer manager.Close() //nolint:errcheck

	// Store events in epoch 1 (ticks 100-102, 3 events)
	for i := uint32(0); i < 3; i++ {
		err := manager.StoreEvent(createTestEvent(1, 100+i, uint64(i+1)))
		require.NoError(t, err, "Failed to store event")
	}

	// Store events in epoch 2 (ticks 200-204, 5 events)
	for i := uint32(0); i < 5; i++ {
		err := manager.StoreEvent(createTestEvent(2, 200+i, uint64(i+10)))
		require.NoError(t, err, "Failed to store event")
	}

	// Get status
	status, err := manager.GetStatus()
	require.NoError(t, err, "Failed to get status")

	// Assert: Response contains both epochs
	require.Len(t, status.Epochs, 2, "Expected 2 epochs in status")

	// Find epoch 1 info
	var epoch1Info, epoch2Info *eventsbridge.EpochInfo
	for _, info := range status.Epochs {
		switch info.Epoch {
		case 1:
			epoch1Info = info
		case 2:
			epoch2Info = info
		}
	}

	// Assert: Epoch 1 has correct info
	require.NotNil(t, epoch1Info, "Epoch 1 info not found in status")
	require.Equal(t, uint64(3), epoch1Info.TotalEvents)
	require.Len(t, epoch1Info.Intervals, 1)
	require.Equal(t, uint32(100), epoch1Info.Intervals[0].FirstTick)
	require.Equal(t, uint32(102), epoch1Info.Intervals[0].LastTick)

	// Assert: Epoch 2 has correct info
	require.NotNil(t, epoch2Info, "Epoch 2 info not found in status")
	require.Equal(t, uint64(5), epoch2Info.TotalEvents)
	require.Len(t, epoch2Info.Intervals, 1)
	require.Equal(t, uint32(200), epoch2Info.Intervals[0].FirstTick)
	require.Equal(t, uint32(204), epoch2Info.Intervals[0].LastTick)

	// Assert: Status has correct current state
	require.Equal(t, uint32(2), status.CurrentEpoch)
	require.Equal(t, uint32(204), status.LastProcessedTick)
}

func TestHasEvent(t *testing.T) {
	// Create temp directory
	tempDir := t.TempDir()

	// Create manager
	manager := createTestManager(t, tempDir)
	defer manager.Close() //nolint:errcheck

	// Store an event
	err := manager.StoreEvent(createTestEvent(1, 100, 1))
	require.NoError(t, err, "Failed to store event")

	// Test: HasEvent returns true for existing event
	exists, err := manager.HasEvent(1, 100, 1)
	require.NoError(t, err, "HasEvent failed")
	require.True(t, exists, "Expected HasEvent to return true for existing event")

	// Test: HasEvent returns false for non-existing event
	exists, err = manager.HasEvent(1, 100, 999)
	require.NoError(t, err, "HasEvent failed")
	require.False(t, exists, "Expected HasEvent to return false for non-existing event")

	// Test: HasEvent returns false for non-existing epoch
	exists, err = manager.HasEvent(99, 100, 1)
	require.NoError(t, err, "HasEvent failed")
	require.False(t, exists, "Expected HasEvent to return false for non-existing epoch")
}

func TestMultipleEventsPerTick(t *testing.T) {
	// Create temp directory
	tempDir := t.TempDir()

	// Create manager
	manager := createTestManager(t, tempDir)
	defer manager.Close() //nolint:errcheck

	// Store multiple events for the same tick
	tick := uint32(100)
	for i := uint64(1); i <= 5; i++ {
		event := createTestEvent(1, tick, i)
		err := manager.StoreEvent(event)
		require.NoError(t, err, "Failed to store event %d", i)
	}

	// Get events for the tick
	epoch, events, err := manager.GetEventsForTick(tick)
	require.NoError(t, err, "Failed to get events for tick")

	// Assert: All 5 events returned
	require.Equal(t, uint32(1), epoch)
	require.Len(t, events, 5)

	// Verify each event
	logIDs := make(map[uint64]bool)
	for _, event := range events {
		logIDs[event.LogId] = true
		require.Equal(t, tick, event.Tick)
		require.Equal(t, uint32(1), event.Epoch)
		require.Equal(t, fmt.Sprintf("digest_%d_%d", tick, event.LogId), event.LogDigest)
	}

	// Verify all log IDs are present
	for i := uint64(1); i <= 5; i++ {
		require.True(t, logIDs[i], "Missing event with logID %d", i)
	}
}

func TestCountEventsForTick(t *testing.T) {
	tempDir := t.TempDir()
	manager := createTestManager(t, tempDir)
	defer manager.Close() //nolint:errcheck

	// Store 3 events for tick 100 in epoch 1
	for i := uint64(1); i <= 3; i++ {
		err := manager.StoreEvent(createTestEvent(1, 100, i))
		require.NoError(t, err)
	}

	// Store 1 event for tick 101 in epoch 1
	err := manager.StoreEvent(createTestEvent(1, 101, 10))
	require.NoError(t, err)

	// Count events for tick 100 — expect 3
	count, err := manager.CountEventsForTick(1, 100)
	require.NoError(t, err)
	require.Equal(t, uint32(3), count)

	// Count events for tick 101 — expect 1
	count, err = manager.CountEventsForTick(1, 101)
	require.NoError(t, err)
	require.Equal(t, uint32(1), count)

	// Count events for nonexistent tick — expect 0
	count, err = manager.CountEventsForTick(1, 999)
	require.NoError(t, err)
	require.Equal(t, uint32(0), count)

	// Count events for nonexistent epoch — expect 0
	count, err = manager.CountEventsForTick(99, 100)
	require.NoError(t, err)
	require.Equal(t, uint32(0), count)
}

func TestBasePath(t *testing.T) {
	// Create temp directory
	tempDir := t.TempDir()

	// Create manager
	manager := createTestManager(t, tempDir)
	defer manager.Close() //nolint:errcheck

	// Verify base path
	require.Equal(t, tempDir, manager.BasePath())

	// Verify storage path
	expected := filepath.Join(tempDir, "component")
	require.Equal(t, expected, manager.GetStoragePath("component"))
}
