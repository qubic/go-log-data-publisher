package tickstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccumulator(t *testing.T) {
	acc := NewAccumulator()
	// 1. Add processed log
	acc.AddProcessed(100, 0, false)
	// 2. Add skipped log
	acc.AddSkipped(100, 1, false)
	// 3. Set total for tick 100
	acc.AddProcessed(100, 2, true)
	// 4. Add for another tick
	acc.AddProcessed(101, 0, false)

	snapshot := acc.Drain()
	require.Len(t, snapshot, 2)

	// 5. Ensure Drain cleared the data and doesn't influence snapshot
	acc.AddProcessed(102, 0, false)
	require.Len(t, acc.Drain(), 1)
	require.Len(t, acc.Drain(), 0)

	tick100 := snapshot[100]
	if tick100.Processed != 2 || tick100.Skipped != 1 || tick100.Total != 3 {
		t.Errorf("Tick 100: Expected {Processed: 2, Skipped: 1, Total: 3}, got %+v", tick100)
	}

	tick101 := snapshot[101]
	if tick101.Processed != 1 || tick101.Skipped != 0 || tick101.Total != 0 {
		t.Errorf("Tick 101: Expected {Processed: 1, Skipped: 0, Total: 0}, got %+v", tick101)
	}
}
