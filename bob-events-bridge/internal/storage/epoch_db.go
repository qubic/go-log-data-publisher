package storage

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	eventsbridge "github.com/qubic/bob-events-bridge/api/events-bridge/v1"
	"google.golang.org/protobuf/proto"
)

// EpochDB wraps a PebbleDB instance for a single epoch
type EpochDB struct {
	db    *pebble.DB
	epoch uint32
	path  string
}

// NewEpochDB opens or creates a PebbleDB for the given epoch
func NewEpochDB(basePath string, epoch uint32) (*EpochDB, error) {
	path := fmt.Sprintf("%s/epochs/%d", basePath, epoch)

	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to open epoch db: %w", err)
	}

	return &EpochDB{
		db:    db,
		epoch: epoch,
		path:  path,
	}, nil
}

// Close closes the epoch database
func (e *EpochDB) Close() error {
	return e.db.Close()
}

// Epoch returns the epoch number
func (e *EpochDB) Epoch() uint32 {
	return e.epoch
}

// Path returns the database path
func (e *EpochDB) Path() string {
	return e.path
}

// FormatKey creates a key in the format <tick_padded_10>:<event_id_padded_20>
func FormatKey(tick uint32, logID uint64) []byte {
	return []byte(fmt.Sprintf("%010d:%020d", tick, logID))
}

// ParseKey extracts tick and logID from a key
func ParseKey(key []byte) (tick uint32, logID uint64, err error) {
	_, err = fmt.Sscanf(string(key), "%d:%d", &tick, &logID)
	return
}

// HasEvent checks if an event exists in the database
func (e *EpochDB) HasEvent(tick uint32, logID uint64) (bool, error) {
	key := FormatKey(tick, logID)
	_, closer, err := e.db.Get(key)
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	return true, nil
}

// StoreEvent stores an event in the database
func (e *EpochDB) StoreEvent(event *eventsbridge.Event) error {
	key := FormatKey(event.Tick, event.LogId)

	data, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	if err := e.db.Set(key, data, pebble.Sync); err != nil {
		return fmt.Errorf("failed to store event: %w", err)
	}

	return nil
}

// StoreEvents stores multiple events in a single batch write with one fsync
func (e *EpochDB) StoreEvents(events []*eventsbridge.Event) error {
	if len(events) == 0 {
		return nil
	}

	batch := e.db.NewBatch()
	defer batch.Close()

	for _, event := range events {
		key := FormatKey(event.Tick, event.LogId)
		data, err := proto.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}
		if err := batch.Set(key, data, nil); err != nil {
			return fmt.Errorf("failed to set event in batch: %w", err)
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	return nil
}

// CountEventsForTick counts the number of events stored for a given tick using prefix scan
func (e *EpochDB) CountEventsForTick(tick uint32) (uint32, error) {
	prefix := []byte(fmt.Sprintf("%010d:", tick))

	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: []byte(fmt.Sprintf("%010d;", tick)), // ';' is after ':' in ASCII
	})
	if err != nil {
		return 0, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	var count uint32
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	return count, nil
}

// GetEventsForTick retrieves all events for a given tick using prefix scan
func (e *EpochDB) GetEventsForTick(tick uint32) ([]*eventsbridge.Event, error) {
	prefix := []byte(fmt.Sprintf("%010d:", tick))

	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: []byte(fmt.Sprintf("%010d;", tick)), // ';' is after ':' in ASCII
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	var events []*eventsbridge.Event

	for iter.First(); iter.Valid(); iter.Next() {
		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("failed to read value: %w", err)
		}

		var event eventsbridge.Event
		if err := proto.Unmarshal(value, &event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal event: %w", err)
		}

		events = append(events, &event)
	}

	return events, nil
}

// GetTickRange returns the min and max ticks stored in this epoch DB
func (e *EpochDB) GetTickRange() (minTick, maxTick uint32, count uint64, err error) {
	iter, err := e.db.NewIter(nil)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	if !iter.First() {
		// Empty database
		return 0, 0, 0, nil
	}

	// Get first tick
	minTick, _, err = ParseKey(iter.Key())
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to parse first key: %w", err)
	}

	// Count and find last tick
	count = 1
	lastTick := minTick

	for iter.Next() {
		count++
		tick, _, parseErr := ParseKey(iter.Key())
		if parseErr == nil {
			lastTick = tick
		}
	}

	maxTick = lastTick
	return minTick, maxTick, count, nil
}

// GetTickIntervals returns a single interval spanning from min to max tick stored in this epoch
func (e *EpochDB) GetTickIntervals() ([]*eventsbridge.TickInterval, uint64, error) {
	iter, err := e.db.NewIter(nil)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	var minTick, maxTick uint32
	var totalEvents uint64
	var initialized bool

	for iter.First(); iter.Valid(); iter.Next() {
		totalEvents++

		tick, _, parseErr := ParseKey(iter.Key())
		if parseErr != nil {
			continue
		}

		if !initialized {
			minTick = tick
			maxTick = tick
			initialized = true
		} else {
			if tick < minTick {
				minTick = tick
			}
			if tick > maxTick {
				maxTick = tick
			}
		}
	}

	if !initialized {
		return nil, 0, nil // Empty database
	}

	interval := &eventsbridge.TickInterval{
		FirstTick: minTick,
		LastTick:  maxTick,
	}

	return []*eventsbridge.TickInterval{interval}, totalEvents, nil
}
