package consume

import (
	"sync"

	"github.com/qubic/log-events-consumer/domain"
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

func (a *Accumulator) AddProcessed(log domain.LogEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()

	tick := a.getTick(log.TickNumber)
	tick.Processed++

	checkLast(log, tick)
}

func (a *Accumulator) AddSkipped(log domain.LogEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()

	tick := a.getTick(log.TickNumber)
	tick.Skipped++

	checkLast(log, tick)
}

// Drain returns a snapshot of current state and resets the internal map.
func (a *Accumulator) Drain() map[uint64]*TickStatus {
	a.mu.Lock()
	defer a.mu.Unlock()

	snapshot := a.data
	a.data = make(map[uint64]*TickStatus)
	return snapshot
}

func checkLast(log domain.LogEvent, tick *TickStatus) {
	if log.LastLogForTick {
		tick.Total = int64(log.Index)
	}
}

func (a *Accumulator) getTick(tickNumber uint64) *TickStatus {
	tick, ok := a.data[tickNumber]
	if !ok {
		tick = &TickStatus{}
		a.data[tickNumber] = tick
	}
	return tick
}
