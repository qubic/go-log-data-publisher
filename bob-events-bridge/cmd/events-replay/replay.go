package main

import (
	"context"
	"fmt"

	eventsbridge "github.com/qubic/bob-events-bridge/api/events-bridge/v1"
	"github.com/qubic/bob-events-bridge/internal/kafka"
	"github.com/qubic/bob-events-bridge/internal/storage"
	"go.uber.org/zap"
)

const replayProgressTickInterval = 100

// runReplay iterates events in [tickStart, tickEnd] from db, builds Kafka messages,
// and publishes them in per-tick batches (mirrors processor.flushBatch behavior).
// Returns the total number of events published.
func runReplay(
	ctx context.Context,
	db *storage.EpochDB,
	publisher kafka.Publisher,
	tickStart, tickEnd uint32,
	logger *zap.Logger,
) (uint64, error) {
	logger.Info("Replay starting",
		zap.Uint32("epoch", db.Epoch()),
		zap.Uint32("tickStart", tickStart),
		zap.Uint32("tickEnd", tickEnd))

	var (
		published    uint64
		ticksFlushed uint64
		batch        []*kafka.EventMessage
		batchTick    uint32
	)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := publisher.PublishEvents(ctx, batch); err != nil {
			return fmt.Errorf("failed to publish batch for tick %d: %w", batchTick, err)
		}
		published += uint64(len(batch))
		ticksFlushed++
		if ticksFlushed%replayProgressTickInterval == 0 {
			logger.Info("Replay progress",
				zap.Uint64("ticksFlushed", ticksFlushed),
				zap.Uint64("eventsPublished", published),
				zap.Uint32("currentTick", batchTick))
		}
		batch = batch[:0]
		return nil
	}

	err := db.IterateEventsInRange(tickStart, tickEnd, func(event *eventsbridge.Event) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		if len(batch) > 0 && event.Tick != batchTick {
			if err := flush(); err != nil {
				return err
			}
		}

		msg, err := kafka.BuildEventMessageFromStored(event)
		if err != nil {
			return fmt.Errorf("failed to build kafka message for tick %d log %d: %w",
				event.Tick, event.LogId, err)
		}

		batch = append(batch, msg)
		batchTick = event.Tick
		return nil
	})
	if err != nil {
		return published, fmt.Errorf("replay iteration failed: %w", err)
	}

	if err := flush(); err != nil {
		return published, err
	}

	logger.Info("Replay complete",
		zap.Uint32("epoch", db.Epoch()),
		zap.Uint64("ticksFlushed", ticksFlushed),
		zap.Uint64("eventsPublished", published))

	return published, nil
}
