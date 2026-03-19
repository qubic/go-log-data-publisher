package tickstore

import (
	"sync"
)

type TickStatus struct {
	Processed int64
	Skipped   int64
	Total     int64
}

type Accumulator struct {
	mu   sync.Mutex
	data map[uint64]*TickStatus
}

func NewAccumulator() *Accumulator {
	return &Accumulator{data: make(map[uint64]*TickStatus)}
}

func (a *Accumulator) AddProcessed(tickNumber, index uint64, isLastLog bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	tickStatus := a.getTick(tickNumber)
	tickStatus.Processed++

	if isLastLog {
		tickStatus.Total = int64(index)
	}
}

func (a *Accumulator) AddSkipped(tickNumber, index uint64, isLastLog bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	tickStatus := a.getTick(tickNumber)
	tickStatus.Skipped++

	if isLastLog {
		tickStatus.Total = int64(index)
	}
}

// Drain returns a snapshot of current state and resets the internal map.
func (a *Accumulator) Drain() map[uint64]*TickStatus {
	a.mu.Lock()
	defer a.mu.Unlock()

	snapshot := a.data
	a.data = make(map[uint64]*TickStatus)
	return snapshot
}

func (a *Accumulator) getTick(tickNumber uint64) *TickStatus {
	tick, ok := a.data[tickNumber]
	if !ok {
		tick = &TickStatus{}
		a.data[tickNumber] = tick
	}
	return tick
}
