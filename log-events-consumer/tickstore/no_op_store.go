package tickstore

import (
	"context"

	"github.com/qubic/log-events-consumer/domain"
)

// NoOpStore is used for disabling the tick store in case the consumer should run without redis.
type NoOpStore struct {
}

func (n *NoOpStore) AddProcessed(_ domain.LogEvent) {
}

func (n *NoOpStore) AddSkipped(_ domain.LogEvent) {
}

func (n *NoOpStore) UpdateTickHeight(_ context.Context) error {
	return nil
}
