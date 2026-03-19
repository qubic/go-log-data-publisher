package tickstore

import (
	"sync"
)

type TickStatus struct {
	Processed uint64
	Skipped   uint64
	Total     uint64
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

	setTotalIfLastLog(tickStatus, index, isLastLog)
}

func (a *Accumulator) AddSkipped(tickNumber, index uint64, isLastLog bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	tickStatus := a.getTick(tickNumber)
	tickStatus.Skipped++

	setTotalIfLastLog(tickStatus, index, isLastLog)
}

func setTotalIfLastLog(tickStatus *TickStatus, index uint64, isLastLog bool) {
	if isLastLog {
		tickStatus.Total = index + 1
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
