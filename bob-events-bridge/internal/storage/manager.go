package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	eventsbridge "github.com/qubic/bob-events-bridge/api/events-bridge/v1"
	"go.uber.org/zap"
)

// ErrEpochOutOfScope is returned when a write is attempted for an epoch that
// has been unloaded by retention (either skipped at startup discovery or
// evicted during a runtime epoch transition). Out-of-scope is permanent for
// the lifetime of the process; callers should treat this as a fatal condition
// since it indicates either operator error (restored archive in live data dir,
// misconfigured KeepEpochs) or unexpected ingest order.
var ErrEpochOutOfScope = errors.New("storage: epoch is out of scope (below retention threshold)")

// Manager manages multiple epoch databases
type Manager struct {
	basePath   string
	keepEpochs uint16
	logger     *zap.Logger
	state      *StateStore
	epochDBs   map[uint32]*EpochDB
	// highestEvictedEpoch is the largest epoch number that has been unloaded
	// by retention. Writes to epoch <= this value are rejected with
	// ErrEpochOutOfScope. Process-lifetime only; rebuilt at startup from
	// directories we found on disk but did not load.
	highestEvictedEpoch uint32
	mu                  sync.RWMutex
}

// NewManager creates a new storage manager. keepEpochs caps the number of
// recent epoch DBs kept loaded; 0 means unlimited (no eviction).
func NewManager(basePath string, keepEpochs uint16, logger *zap.Logger) (*Manager, error) {
	// Ensure base path exists
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base path: %w", err)
	}

	// Open state store
	state, err := NewStateStore(basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open state store: %w", err)
	}

	m := &Manager{
		basePath:   basePath,
		keepEpochs: keepEpochs,
		logger:     logger,
		state:      state,
		epochDBs:   make(map[uint32]*EpochDB),
	}

	// Discover and open existing epoch databases
	if err := m.discoverEpochDBs(); err != nil {
		_ = state.Close()
		return nil, fmt.Errorf("failed to discover epoch dbs: %w", err)
	}

	return m, nil
}

// discoverEpochDBs scans the epochs subfolder for existing epoch databases.
// If keepEpochs > 0, only the top-N epoch directories (by number) are opened;
// the rest are left on disk for external archival and the highestEvictedEpoch
// threshold is set to the largest skipped epoch so future writes for those
// epochs are rejected with ErrEpochOutOfScope.
func (m *Manager) discoverEpochDBs() error {
	epochsPath := filepath.Join(m.basePath, "epochs")

	// Check if epochs directory exists
	if _, err := os.Stat(epochsPath); os.IsNotExist(err) {
		// No epochs directory yet, nothing to discover
		return nil
	}

	entries, err := os.ReadDir(epochsPath)
	if err != nil {
		return fmt.Errorf("failed to read epochs path: %w", err)
	}

	// Collect all valid epoch numbers from disk.
	found := make([]uint32, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		epoch, err := strconv.ParseUint(entry.Name(), 10, 32)
		if err != nil {
			m.logger.Warn("Invalid epoch directory name", zap.String("name", entry.Name()))
			continue
		}
		found = append(found, uint32(epoch))
	}

	// Sort descending so we can take the top N.
	sort.Slice(found, func(i, j int) bool { return found[i] > found[j] })

	toOpen := found
	var skipped []uint32
	if m.keepEpochs > 0 && len(found) > int(m.keepEpochs) {
		toOpen = found[:m.keepEpochs]
		skipped = found[m.keepEpochs:]
	}

	for _, epoch := range toOpen {
		db, err := NewEpochDB(m.basePath, epoch)
		if err != nil {
			m.logger.Error("Failed to open epoch db",
				zap.Uint32("epoch", epoch),
				zap.Error(err))
			continue
		}
		m.epochDBs[epoch] = db
		m.logger.Info("Opened epoch database", zap.Uint32("epoch", epoch))
	}

	// Set the out-of-scope threshold from the largest skipped epoch (if any).
	// Skipped is sorted descending, so the first element is the max.
	if len(skipped) > 0 {
		m.highestEvictedEpoch = skipped[0]
		for _, epoch := range skipped {
			m.logger.Info("Epoch on disk but not loaded (retention)",
				zap.Uint32("epoch", epoch))
		}
	}

	return nil
}

// Close closes all databases
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error

	for epoch, db := range m.epochDBs {
		if err := db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close epoch %d: %w", epoch, err))
		}
	}

	if err := m.state.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close state store: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing databases: %v", errs)
	}

	return nil
}

// LoadState loads the persisted state
func (m *Manager) LoadState() (*State, error) {
	return m.state.LoadState()
}

// GetOrCreateEpochDB gets or creates an epoch database.
// Returns ErrEpochOutOfScope if the epoch has been unloaded by retention.
func (m *Manager) GetOrCreateEpochDB(epoch uint32) (*EpochDB, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if db, exists := m.epochDBs[epoch]; exists {
		return db, nil
	}

	if m.keepEpochs > 0 && epoch <= m.highestEvictedEpoch {
		return nil, fmt.Errorf("%w: epoch=%d threshold=%d", ErrEpochOutOfScope, epoch, m.highestEvictedEpoch)
	}

	db, err := NewEpochDB(m.basePath, epoch)
	if err != nil {
		return nil, err
	}

	m.epochDBs[epoch] = db
	m.logger.Info("Created new epoch database", zap.Uint32("epoch", epoch))

	m.evictOldestIfOverLimit()

	return db, nil
}

// evictOldestIfOverLimit unloads the oldest epoch DBs until len(epochDBs) <= keepEpochs.
// Eviction = Close() + delete from map. The on-disk directory is left intact for
// external archival. Caller must hold m.mu.
func (m *Manager) evictOldestIfOverLimit() {
	if m.keepEpochs == 0 {
		return
	}

	for len(m.epochDBs) > int(m.keepEpochs) {
		// Find the smallest epoch number currently loaded.
		var oldest uint32
		first := true
		for e := range m.epochDBs {
			if first || e < oldest {
				oldest = e
				first = false
			}
		}

		db := m.epochDBs[oldest]
		if err := db.Close(); err != nil {
			m.logger.Error("Failed to close evicted epoch db",
				zap.Uint32("epoch", oldest),
				zap.Error(err))
		}
		delete(m.epochDBs, oldest)

		if oldest > m.highestEvictedEpoch {
			m.highestEvictedEpoch = oldest
		}

		m.logger.Info("Epoch unloaded (retention)",
			zap.Uint32("epoch", oldest),
			zap.Uint32("threshold", m.highestEvictedEpoch))
	}
}

// GetEpochDB gets an epoch database if it exists
func (m *Manager) GetEpochDB(epoch uint32) *EpochDB {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.epochDBs[epoch]
}

// CountEventsForTick counts the number of events stored for a given tick in the specified epoch
func (m *Manager) CountEventsForTick(epoch uint32, tick uint32) (uint32, error) {
	db := m.GetEpochDB(epoch)
	if db == nil {
		return 0, nil
	}
	return db.CountEventsForTick(tick)
}

// HasEvent checks if an event exists in the specified epoch
func (m *Manager) HasEvent(epoch uint32, tick uint32, logID uint64) (bool, error) {
	db := m.GetEpochDB(epoch)
	if db == nil {
		return false, nil
	}
	return db.HasEvent(tick, logID)
}

// StoreEvent stores an event and updates state atomically
func (m *Manager) StoreEvent(event *eventsbridge.Event) error {
	db, err := m.GetOrCreateEpochDB(event.Epoch)
	if err != nil {
		return fmt.Errorf("failed to get epoch db: %w", err)
	}

	// Store the event
	if err := db.StoreEvent(event); err != nil {
		return fmt.Errorf("failed to store event: %w", err)
	}

	// Update state atomically
	if err := m.state.UpdateState(event.Epoch, int64(event.LogId), event.Tick); err != nil {
		return fmt.Errorf("failed to update state: %w", err)
	}

	// Update epoch tick range
	if err := m.state.UpdateEpochTickRange(event.Epoch, event.Tick); err != nil {
		m.logger.Warn("Failed to update epoch tick range", zap.Error(err))
	}

	// Increment event count
	if err := m.state.IncrementEpochEventCount(event.Epoch); err != nil {
		m.logger.Warn("Failed to increment event count", zap.Error(err))
	}

	return nil
}

// StoreEvents stores a batch of events (assumed to be same epoch) and updates state
func (m *Manager) StoreEvents(events []*eventsbridge.Event) error {
	if len(events) == 0 {
		return nil
	}

	// All events in a batch are for the same epoch
	epoch := events[0].Epoch

	db, err := m.GetOrCreateEpochDB(epoch)
	if err != nil {
		return fmt.Errorf("failed to get epoch db: %w", err)
	}

	// Batch store all events
	if err := db.StoreEvents(events); err != nil {
		return fmt.Errorf("failed to store events: %w", err)
	}

	// Update state with last event's info
	last := events[len(events)-1]
	if err := m.state.UpdateState(epoch, int64(last.LogId), last.Tick); err != nil {
		return fmt.Errorf("failed to update state: %w", err)
	}

	// Update epoch tick range
	if err := m.state.UpdateEpochTickRange(epoch, last.Tick); err != nil {
		m.logger.Warn("Failed to update epoch tick range", zap.Error(err))
	}

	// Increment event count by batch size
	if err := m.state.IncrementEpochEventCountBy(epoch, uint64(len(events))); err != nil {
		m.logger.Warn("Failed to increment event count", zap.Error(err))
	}

	return nil
}

// GetEventsForTick finds the appropriate epoch and returns events for a tick
func (m *Manager) GetEventsForTick(tick uint32) (uint32, []*eventsbridge.Event, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Find the epoch containing this tick
	for epoch, db := range m.epochDBs {
		events, err := db.GetEventsForTick(tick)
		if err != nil {
			m.logger.Warn("Error getting events from epoch",
				zap.Uint32("epoch", epoch),
				zap.Error(err))
			continue
		}

		if len(events) > 0 {
			return epoch, events, nil
		}
	}

	// Try to determine the epoch from state tick ranges
	for epoch := range m.epochDBs {
		minTick, maxTick, exists, err := m.state.GetEpochTickRange(epoch)
		if err != nil || !exists {
			continue
		}
		if tick >= minTick && tick <= maxTick {
			events, err := m.epochDBs[epoch].GetEventsForTick(tick)
			if err != nil {
				return 0, nil, err
			}
			return epoch, events, nil
		}
	}

	// Tick not found in any epoch
	return 0, nil, nil
}

// GetStatus returns status information for all epochs
func (m *Manager) GetStatus() (*eventsbridge.GetStatusResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, err := m.state.LoadState()
	if err != nil {
		return nil, fmt.Errorf("failed to load state: %w", err)
	}

	response := &eventsbridge.GetStatusResponse{
		CurrentEpoch:       state.CurrentEpoch,
		LastProcessedTick:  state.LastTick,
		LastProcessedLogId: state.LastLogID,
	}

	// Get sorted epoch list
	epochs := make([]uint32, 0, len(m.epochDBs))
	for epoch := range m.epochDBs {
		epochs = append(epochs, epoch)
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })

	// Build epoch info from cached state (O(1) per epoch)
	for _, epoch := range epochs {
		minTick, maxTick, exists, err := m.state.GetEpochTickRange(epoch)
		if err != nil {
			m.logger.Warn("Failed to get epoch tick range",
				zap.Uint32("epoch", epoch),
				zap.Error(err))
			continue
		}
		if !exists {
			continue
		}

		totalEvents, err := m.state.GetEpochEventCount(epoch)
		if err != nil {
			m.logger.Warn("Failed to get epoch event count",
				zap.Uint32("epoch", epoch),
				zap.Error(err))
			continue
		}

		epochInfo := &eventsbridge.EpochInfo{
			Epoch: epoch,
			Intervals: []*eventsbridge.TickInterval{{
				FirstTick: minTick,
				LastTick:  maxTick,
			}},
			TotalEvents: totalEvents,
		}

		response.Epochs = append(response.Epochs, epochInfo)
	}

	return response, nil
}

// GetAvailableEpochs returns a list of available epochs
func (m *Manager) GetAvailableEpochs() []uint32 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	epochs := make([]uint32, 0, len(m.epochDBs))
	for epoch := range m.epochDBs {
		epochs = append(epochs, epoch)
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	return epochs
}

// BasePath returns the base storage path
func (m *Manager) BasePath() string {
	return m.basePath
}

// GetStoragePath returns the full path for a given component
func (m *Manager) GetStoragePath(component string) string {
	return filepath.Join(m.basePath, component)
}
