package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	eventsbridge "github.com/qubic/bob-events-bridge/api/events-bridge/v1"
	"go.uber.org/zap"
)

// Manager manages multiple epoch databases
type Manager struct {
	basePath   string
	logger     *zap.Logger
	state      *StateStore
	epochDBs   map[uint32]*EpochDB
	mu         sync.RWMutex
}

// NewManager creates a new storage manager
func NewManager(basePath string, logger *zap.Logger) (*Manager, error) {
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
		basePath: basePath,
		logger:   logger,
		state:    state,
		epochDBs: make(map[uint32]*EpochDB),
	}

	// Discover and open existing epoch databases
	if err := m.discoverEpochDBs(); err != nil {
		state.Close()
		return nil, fmt.Errorf("failed to discover epoch dbs: %w", err)
	}

	return m, nil
}

// discoverEpochDBs scans the epochs subfolder for existing epoch databases
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

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		name := entry.Name()
		epoch, err := strconv.ParseUint(name, 10, 32)
		if err != nil {
			m.logger.Warn("Invalid epoch directory name", zap.String("name", name))
			continue
		}

		db, err := NewEpochDB(m.basePath, uint32(epoch))
		if err != nil {
			m.logger.Error("Failed to open epoch db",
				zap.Uint64("epoch", epoch),
				zap.Error(err))
			continue
		}

		m.epochDBs[uint32(epoch)] = db
		m.logger.Info("Opened epoch database", zap.Uint64("epoch", epoch))
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

// GetOrCreateEpochDB gets or creates an epoch database
func (m *Manager) GetOrCreateEpochDB(epoch uint32) (*EpochDB, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if db, exists := m.epochDBs[epoch]; exists {
		return db, nil
	}

	db, err := NewEpochDB(m.basePath, epoch)
	if err != nil {
		return nil, err
	}

	m.epochDBs[epoch] = db
	m.logger.Info("Created new epoch database", zap.Uint32("epoch", epoch))

	return db, nil
}

// GetEpochDB gets an epoch database if it exists
func (m *Manager) GetEpochDB(epoch uint32) *EpochDB {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.epochDBs[epoch]
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

	// Build epoch info
	for _, epoch := range epochs {
		db := m.epochDBs[epoch]

		intervals, totalEvents, err := db.GetTickIntervals()
		if err != nil {
			m.logger.Warn("Failed to get tick intervals",
				zap.Uint32("epoch", epoch),
				zap.Error(err))
			continue
		}

		epochInfo := &eventsbridge.EpochInfo{
			Epoch:       epoch,
			Intervals:   intervals,
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
