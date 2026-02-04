package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"
)

const (
	keyCurrentEpoch   = "state:current_epoch"
	keyLastLogID      = "state:last_log_id"
	keyLastTick       = "state:last_tick"
	keyEpochMinTick   = "state:epoch:%d:min_tick"
	keyEpochMaxTick   = "state:epoch:%d:max_tick"
	keyEpochEventCount = "state:epoch:%d:event_count"
)

// StateStore manages crash recovery state using PebbleDB
type StateStore struct {
	db *pebble.DB
}

// NewStateStore opens or creates a state store at the given path
func NewStateStore(basePath string) (*StateStore, error) {
	path := fmt.Sprintf("%s/state", basePath)

	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to open state db: %w", err)
	}

	return &StateStore{db: db}, nil
}

// Close closes the state store
func (s *StateStore) Close() error {
	return s.db.Close()
}

// State represents the current processing state
type State struct {
	CurrentEpoch  uint32
	LastLogID     int64
	LastTick      uint32
	HasState      bool
}

// LoadState loads the complete state from the database
func (s *StateStore) LoadState() (*State, error) {
	state := &State{}

	// Get current epoch
	epoch, hasEpoch, err := s.getUint32(keyCurrentEpoch)
	if err != nil {
		return nil, err
	}
	state.CurrentEpoch = epoch
	state.HasState = hasEpoch

	// Get last log ID
	logID, hasLogID, err := s.getInt64(keyLastLogID)
	if err != nil {
		return nil, err
	}
	if hasLogID {
		state.LastLogID = logID
		state.HasState = true
	}

	// Get last tick
	tick, hasTick, err := s.getUint32(keyLastTick)
	if err != nil {
		return nil, err
	}
	if hasTick {
		state.LastTick = tick
	}

	return state, nil
}

// UpdateState atomically updates the processing state
func (s *StateStore) UpdateState(epoch uint32, logID int64, tick uint32) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	// Set current epoch
	epochVal := make([]byte, 4)
	binary.LittleEndian.PutUint32(epochVal, epoch)
	if err := batch.Set([]byte(keyCurrentEpoch), epochVal, nil); err != nil {
		return fmt.Errorf("failed to set epoch: %w", err)
	}

	// Set last log ID
	logVal := make([]byte, 8)
	binary.LittleEndian.PutUint64(logVal, uint64(logID))
	if err := batch.Set([]byte(keyLastLogID), logVal, nil); err != nil {
		return fmt.Errorf("failed to set log id: %w", err)
	}

	// Set last tick
	tickVal := make([]byte, 4)
	binary.LittleEndian.PutUint32(tickVal, tick)
	if err := batch.Set([]byte(keyLastTick), tickVal, nil); err != nil {
		return fmt.Errorf("failed to set tick: %w", err)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit state: %w", err)
	}

	return nil
}

// UpdateEpochTickRange updates the min/max tick for an epoch
func (s *StateStore) UpdateEpochTickRange(epoch, tick uint32) error {
	minKey := fmt.Sprintf(keyEpochMinTick, epoch)
	maxKey := fmt.Sprintf(keyEpochMaxTick, epoch)

	batch := s.db.NewBatch()
	defer batch.Close()

	// Check and update min tick
	currentMin, hasMin, err := s.getUint32(minKey)
	if err != nil {
		return err
	}
	if !hasMin || tick < currentMin {
		val := make([]byte, 4)
		binary.LittleEndian.PutUint32(val, tick)
		if err := batch.Set([]byte(minKey), val, nil); err != nil {
			return err
		}
	}

	// Always update max tick
	maxVal := make([]byte, 4)
	binary.LittleEndian.PutUint32(maxVal, tick)
	if err := batch.Set([]byte(maxKey), maxVal, nil); err != nil {
		return err
	}

	return batch.Commit(pebble.Sync)
}

// GetEpochTickRange returns the min and max tick for an epoch from state
func (s *StateStore) GetEpochTickRange(epoch uint32) (minTick, maxTick uint32, exists bool, err error) {
	minKey := fmt.Sprintf(keyEpochMinTick, epoch)
	maxKey := fmt.Sprintf(keyEpochMaxTick, epoch)

	minTick, hasMin, err := s.getUint32(minKey)
	if err != nil {
		return 0, 0, false, err
	}
	if !hasMin {
		return 0, 0, false, nil
	}

	maxTick, hasMax, err := s.getUint32(maxKey)
	if err != nil {
		return 0, 0, false, err
	}
	if !hasMax {
		return 0, 0, false, nil
	}

	return minTick, maxTick, true, nil
}

// IncrementEpochEventCount increments the event count for an epoch
func (s *StateStore) IncrementEpochEventCount(epoch uint32) error {
	key := fmt.Sprintf(keyEpochEventCount, epoch)

	current, _, err := s.getUint64(key)
	if err != nil {
		return err
	}

	val := make([]byte, 8)
	binary.LittleEndian.PutUint64(val, current+1)

	return s.db.Set([]byte(key), val, pebble.Sync)
}

// IncrementEpochEventCountBy increments the event count for an epoch by count
func (s *StateStore) IncrementEpochEventCountBy(epoch uint32, count uint64) error {
	key := fmt.Sprintf(keyEpochEventCount, epoch)

	current, _, err := s.getUint64(key)
	if err != nil {
		return err
	}

	val := make([]byte, 8)
	binary.LittleEndian.PutUint64(val, current+count)

	return s.db.Set([]byte(key), val, pebble.Sync)
}

// GetEpochEventCount returns the event count for an epoch
func (s *StateStore) GetEpochEventCount(epoch uint32) (uint64, error) {
	key := fmt.Sprintf(keyEpochEventCount, epoch)
	count, _, err := s.getUint64(key)
	return count, err
}

// Helper methods

func (s *StateStore) getUint32(key string) (uint32, bool, error) {
	val, closer, err := s.db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("failed to get %s: %w", key, err)
	}
	defer closer.Close()

	if len(val) < 4 {
		return 0, false, fmt.Errorf("invalid value length for %s: %d", key, len(val))
	}

	return binary.LittleEndian.Uint32(val), true, nil
}

func (s *StateStore) getInt64(key string) (int64, bool, error) {
	val, closer, err := s.db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("failed to get %s: %w", key, err)
	}
	defer closer.Close()

	if len(val) < 8 {
		return 0, false, fmt.Errorf("invalid value length for %s: %d", key, len(val))
	}

	return int64(binary.LittleEndian.Uint64(val)), true, nil
}

func (s *StateStore) getUint64(key string) (uint64, bool, error) {
	val, closer, err := s.db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("failed to get %s: %w", key, err)
	}
	defer closer.Close()

	if len(val) < 8 {
		return 0, false, fmt.Errorf("invalid value length for %s: %d", key, len(val))
	}

	return binary.LittleEndian.Uint64(val), true, nil
}
