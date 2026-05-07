package storage

import (
	"errors"
	"path/filepath"
	"testing"

	eventsbridge "github.com/qubic/bob-events-bridge/api/events-bridge/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// preCreateEpochDirs opens (and immediately closes) the given epoch DBs so
// their directories exist on disk for a subsequent manager to discover.
func preCreateEpochDirs(t *testing.T, basePath string, epochs ...uint32) {
	t.Helper()
	for _, e := range epochs {
		db, err := NewEpochDB(basePath, e)
		require.NoError(t, err)
		require.NoError(t, db.Close())
	}
}

func TestKeepEpochs_DiscoveryCapsAtN(t *testing.T) {
	tempDir := t.TempDir()
	preCreateEpochDirs(t, tempDir, 1, 2, 3, 4, 5)

	mgr := createTestManagerWithKeep(t, tempDir, 3)
	defer mgr.Close() //nolint:errcheck

	// Top 3 epochs by number stay loaded.
	assert.Equal(t, []uint32{3, 4, 5}, mgr.GetAvailableEpochs())

	// Skipped epoch directories must still exist on disk.
	assert.DirExists(t, filepath.Join(tempDir, "epochs", "1"))
	assert.DirExists(t, filepath.Join(tempDir, "epochs", "2"))
}

func TestKeepEpochs_DiscoveryUnlimitedWhenZero(t *testing.T) {
	tempDir := t.TempDir()
	preCreateEpochDirs(t, tempDir, 1, 2, 3, 4, 5)

	mgr := createTestManager(t, tempDir) // keepEpochs=0
	defer mgr.Close()                    //nolint:errcheck

	assert.Equal(t, []uint32{1, 2, 3, 4, 5}, mgr.GetAvailableEpochs())
}

func TestKeepEpochs_EvictionOnEpochTransition(t *testing.T) {
	tempDir := t.TempDir()

	mgr := createTestManagerWithKeep(t, tempDir, 3)
	defer mgr.Close() //nolint:errcheck

	// Fill epochs 1, 2, 3 — at the cap.
	for epoch := uint32(1); epoch <= 3; epoch++ {
		require.NoError(t, mgr.StoreEvent(createTestEvent(epoch, 100, uint64(epoch))))
	}
	assert.Equal(t, []uint32{1, 2, 3}, mgr.GetAvailableEpochs())

	// New epoch arrives — oldest (epoch 1) must be evicted.
	require.NoError(t, mgr.StoreEvent(createTestEvent(4, 100, 4)))

	assert.Equal(t, []uint32{2, 3, 4}, mgr.GetAvailableEpochs())

	// On-disk directory for evicted epoch must be preserved for archival.
	assert.DirExists(t, filepath.Join(tempDir, "epochs", "1"),
		"evicted epoch directory must be preserved on disk")
}

func TestKeepEpochs_StateStoreEntriesPersistAfterEviction(t *testing.T) {
	tempDir := t.TempDir()

	mgr := createTestManagerWithKeep(t, tempDir, 2)
	defer mgr.Close() //nolint:errcheck

	require.NoError(t, mgr.StoreEvent(createTestEvent(1, 100, 1)))
	require.NoError(t, mgr.StoreEvent(createTestEvent(2, 100, 2)))
	require.NoError(t, mgr.StoreEvent(createTestEvent(3, 100, 3))) // evicts 1

	require.NotContains(t, mgr.GetAvailableEpochs(), uint32(1))

	// State-store entries for the evicted epoch are intentionally left intact;
	// they're inert because GetStatus iterates m.epochDBs only.
	minTick, maxTick, exists, err := mgr.state.GetEpochTickRange(1)
	require.NoError(t, err)
	assert.True(t, exists, "state-store keys should persist after eviction (inert)")
	assert.Equal(t, uint32(100), minTick)
	assert.Equal(t, uint32(100), maxTick)

	count, err := mgr.state.GetEpochEventCount(1)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count)
}

func TestKeepEpochs_GetStatusOnlyShowsLoadedEpochs(t *testing.T) {
	tempDir := t.TempDir()

	mgr := createTestManagerWithKeep(t, tempDir, 2)
	defer mgr.Close() //nolint:errcheck

	require.NoError(t, mgr.StoreEvent(createTestEvent(1, 100, 1)))
	require.NoError(t, mgr.StoreEvent(createTestEvent(2, 100, 2)))
	require.NoError(t, mgr.StoreEvent(createTestEvent(3, 100, 3))) // evicts 1

	resp, err := mgr.GetStatus()
	require.NoError(t, err)

	gotEpochs := make([]uint32, 0, len(resp.Epochs))
	for _, info := range resp.Epochs {
		gotEpochs = append(gotEpochs, info.Epoch)
	}
	assert.ElementsMatch(t, []uint32{2, 3}, gotEpochs,
		"GetStatus must not surface evicted epochs even though state keys remain")
}

func TestKeepEpochs_StoreEventReturnsErrorForEvictedEpoch(t *testing.T) {
	tempDir := t.TempDir()

	mgr := createTestManagerWithKeep(t, tempDir, 2)
	defer mgr.Close() //nolint:errcheck

	require.NoError(t, mgr.StoreEvent(createTestEvent(1, 100, 1)))
	require.NoError(t, mgr.StoreEvent(createTestEvent(2, 100, 2)))
	require.NoError(t, mgr.StoreEvent(createTestEvent(3, 100, 3))) // evicts 1

	// Attempt to write to the evicted epoch — must fail with ErrEpochOutOfScope.
	err := mgr.StoreEvent(createTestEvent(1, 200, 99))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrEpochOutOfScope),
		"expected ErrEpochOutOfScope, got: %v", err)
}

func TestKeepEpochs_StartupSkippedEpochIsOutOfScope(t *testing.T) {
	tempDir := t.TempDir()
	preCreateEpochDirs(t, tempDir, 1, 2, 3, 4, 5)

	mgr := createTestManagerWithKeep(t, tempDir, 3)
	defer mgr.Close() //nolint:errcheck

	require.Equal(t, []uint32{3, 4, 5}, mgr.GetAvailableEpochs())

	// Threshold from discovery should reject writes to epoch 2 even though it
	// was never opened in this process.
	err := mgr.StoreEvent(createTestEvent(2, 100, 1))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrEpochOutOfScope),
		"expected ErrEpochOutOfScope for startup-skipped epoch, got: %v", err)

	// Boundary epoch (= threshold) is also out of scope.
	err = mgr.StoreEvent(createTestEvent(1, 100, 1))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrEpochOutOfScope))
}

func TestKeepEpochs_ZeroDisablesEvictionAndGuard(t *testing.T) {
	tempDir := t.TempDir()

	mgr := createTestManager(t, tempDir) // keepEpochs=0
	defer mgr.Close()                    //nolint:errcheck

	for epoch := uint32(1); epoch <= 10; epoch++ {
		require.NoError(t, mgr.StoreEvent(createTestEvent(epoch, 100, uint64(epoch))))
	}

	// All 10 stay loaded; no eviction.
	available := mgr.GetAvailableEpochs()
	assert.Len(t, available, 10)
}

func TestKeepEpochs_StoreEventsBatchAlsoGuarded(t *testing.T) {
	tempDir := t.TempDir()

	mgr := createTestManagerWithKeep(t, tempDir, 2)
	defer mgr.Close() //nolint:errcheck

	require.NoError(t, mgr.StoreEvent(createTestEvent(1, 100, 1)))
	require.NoError(t, mgr.StoreEvent(createTestEvent(2, 100, 2)))
	require.NoError(t, mgr.StoreEvent(createTestEvent(3, 100, 3))) // evicts 1

	// Batch path must enforce the same guard.
	batch := []*eventsbridge.Event{
		createTestEvent(1, 200, 10),
		createTestEvent(1, 200, 11),
	}
	err := mgr.StoreEvents(batch)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrEpochOutOfScope))
}
