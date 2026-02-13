package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	eventsbridge "github.com/qubic/bob-events-bridge/api/events-bridge/v1"
	"github.com/qubic/bob-events-bridge/internal/config"
	"github.com/qubic/bob-events-bridge/internal/grpc"
	"github.com/qubic/bob-events-bridge/internal/kafka"
	"github.com/qubic/bob-events-bridge/internal/processor"
	"github.com/qubic/bob-events-bridge/internal/storage"
)

// startProcessor starts the processor in a goroutine and returns a channel that signals when it stops
func startProcessor(ctx context.Context, proc *processor.Processor) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		_ = proc.Start(ctx)
		close(done)
	}()
	return done
}

// stopProcessor stops the processor and waits for it to fully stop
func stopProcessor(cancel context.CancelFunc, proc *processor.Processor, done <-chan struct{}) {
	cancel()
	proc.Stop()
	// Wait for processor goroutine to exit with timeout
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
}

// TestE2E_SingleEventFlow tests the complete flow: WebSocket → Storage → gRPC query
func TestE2E_SingleEventFlow(t *testing.T) {
	// 1. Create mock bob server
	mockBob := NewMockBobServer(145, 22000000)
	defer mockBob.Close()

	// 2. Create storage
	tempDir := t.TempDir()
	storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err, "Failed to create storage manager")

	// 3. Create processor
	cfg := CreateTestConfig(mockBob, tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop(), nil)

	// 4. Start processor
	ctx, cancel := context.WithCancel(context.Background())
	done := startProcessor(ctx, proc)

	// Ensure clean shutdown
	defer func() {
		stopProcessor(cancel, proc, done)
		_ = storageMgr.Close()
	}()

	// 5. Wait for subscription
	_, err = mockBob.WaitForSubscription(5 * time.Second)
	require.NoError(t, err, "Timeout waiting for subscription")

	// 6. Send event
	payload := CreateLogPayload(145, 22000001, 1, 0, map[string]any{
		"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"amount": 1000,
	})
	err = mockBob.SendLogMessage(payload, 0, 0, false)
	require.NoError(t, err, "Failed to send log message")

	mockBob.SendCatchUpComplete(0, 1, 1)

	// 7. Wait for storage
	WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
		exists, _ := storageMgr.HasEvent(145, 22000001, 1)
		return exists
	}, "event should be stored")

	// 8. Query via gRPC service
	service := grpc.NewEventsBridgeService(storageMgr, zap.NewNop())
	resp, err := service.GetEventsForTick(ctx, &eventsbridge.GetEventsForTickRequest{Tick: 22000001})
	require.NoError(t, err, "Failed to get events for tick")
	require.Len(t, resp.Events, 1, "Expected 1 event")
	require.Equal(t, uint64(1), resp.Events[0].LogId)
	require.Equal(t, uint32(22000001), resp.Events[0].Tick)
	require.Equal(t, uint32(145), resp.Events[0].Epoch)
	require.Equal(t, uint32(0), resp.Events[0].IndexInTick)
	require.Equal(t, "test-digest", resp.Events[0].LogDigest)
}

// TestE2E_MultipleEventsPerTick tests multiple events for the same tick
func TestE2E_MultipleEventsPerTick(t *testing.T) {
	mockBob := NewMockBobServer(145, 22000000)
	defer mockBob.Close()

	tempDir := t.TempDir()
	storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	cfg := CreateTestConfig(mockBob, tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := startProcessor(ctx, proc)

	defer func() {
		stopProcessor(cancel, proc, done)
		_ = storageMgr.Close()
	}()

	_, err = mockBob.WaitForSubscription(5 * time.Second)
	require.NoError(t, err)

	// Send 5 events for the same tick
	tick := uint32(22000001)
	for i := uint64(1); i <= 5; i++ {
		payload := CreateLogPayload(145, tick, i, 0, map[string]any{
			"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			"amount": i,
		})
		err = mockBob.SendLogMessage(payload, 0, 0, false)
		require.NoError(t, err)
	}
	mockBob.SendCatchUpComplete(0, 5, 5)

	// Wait for all events
	WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
		_, events, _ := storageMgr.GetEventsForTick(tick)
		return len(events) >= 5
	}, "all 5 events should be stored")

	// Verify via gRPC
	service := grpc.NewEventsBridgeService(storageMgr, zap.NewNop())
	resp, err := service.GetEventsForTick(ctx, &eventsbridge.GetEventsForTickRequest{Tick: tick})
	require.NoError(t, err)
	require.Len(t, resp.Events, 5)

	// Verify all log IDs are present and IndexInTick values are 0-4
	logIDs := make(map[uint64]bool)
	indexValues := make(map[uint32]bool)
	for _, event := range resp.Events {
		logIDs[event.LogId] = true
		indexValues[event.IndexInTick] = true
		require.Equal(t, "test-digest", event.LogDigest)
	}
	for i := uint64(1); i <= 5; i++ {
		require.True(t, logIDs[i], "Missing event with logID %d", i)
	}
	for i := uint32(0); i < 5; i++ {
		require.True(t, indexValues[i], "Missing IndexInTick value %d", i)
	}
}

// TestE2E_EpochTransition tests events across epoch boundaries
func TestE2E_EpochTransition(t *testing.T) {
	mockBob := NewMockBobServer(145, 22000000)
	defer mockBob.Close()

	tempDir := t.TempDir()
	storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	cfg := CreateTestConfig(mockBob, tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := startProcessor(ctx, proc)

	defer func() {
		stopProcessor(cancel, proc, done)
		_ = storageMgr.Close()
	}()

	_, err = mockBob.WaitForSubscription(5 * time.Second)
	require.NoError(t, err)

	// Send events in epoch 145
	payload1 := CreateLogPayload(145, 22000001, 1, 0, nil)
	err = mockBob.SendLogMessage(payload1, 0, 0, false)
	require.NoError(t, err)

	// Send events in epoch 146 (epoch transition)
	payload2 := CreateLogPayload(146, 22500001, 2, 0, nil)
	err = mockBob.SendLogMessage(payload2, 0, 0, false)
	require.NoError(t, err)

	mockBob.SendCatchUpComplete(0, 2, 2)

	// Wait for both events
	WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
		exists1, _ := storageMgr.HasEvent(145, 22000001, 1)
		exists2, _ := storageMgr.HasEvent(146, 22500001, 2)
		return exists1 && exists2
	}, "both epoch events should be stored")

	// Verify events are in correct epochs
	service := grpc.NewEventsBridgeService(storageMgr, zap.NewNop())

	resp1, err := service.GetEventsForTick(ctx, &eventsbridge.GetEventsForTickRequest{Tick: 22000001})
	require.NoError(t, err)
	require.Len(t, resp1.Events, 1)
	require.Equal(t, uint32(145), resp1.Epoch)

	resp2, err := service.GetEventsForTick(ctx, &eventsbridge.GetEventsForTickRequest{Tick: 22500001})
	require.NoError(t, err)
	require.Len(t, resp2.Events, 1)
	require.Equal(t, uint32(146), resp2.Epoch)

	// Verify both epochs are available
	epochs := storageMgr.GetAvailableEpochs()
	require.Len(t, epochs, 2)
	require.Contains(t, epochs, uint32(145))
	require.Contains(t, epochs, uint32(146))
}

// TestE2E_Deduplication tests that duplicate events are not stored twice
func TestE2E_Deduplication(t *testing.T) {
	mockBob := NewMockBobServer(145, 22000000)
	defer mockBob.Close()

	tempDir := t.TempDir()
	storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	cfg := CreateTestConfig(mockBob, tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := startProcessor(ctx, proc)

	defer func() {
		stopProcessor(cancel, proc, done)
		_ = storageMgr.Close()
	}()

	_, err = mockBob.WaitForSubscription(5 * time.Second)
	require.NoError(t, err)

	// Send the same event twice
	payload := CreateLogPayload(145, 22000001, 1, 0, map[string]any{
		"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"amount": 500,
	})
	err = mockBob.SendLogMessage(payload, 0, 0, false)
	require.NoError(t, err)

	// Flush the batch so the first event is stored
	mockBob.SendCatchUpComplete(0, 1, 1)

	// Wait for first event
	WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
		exists, _ := storageMgr.HasEvent(145, 22000001, 1)
		return exists
	}, "first event should be stored")

	// Send duplicate
	err = mockBob.SendLogMessage(payload, 0, 0, false)
	require.NoError(t, err)
	mockBob.SendCatchUpComplete(0, 1, 2)

	// Give some time for the duplicate to be processed
	time.Sleep(100 * time.Millisecond)

	// Verify only one event is stored
	_, events, err := storageMgr.GetEventsForTick(22000001)
	require.NoError(t, err)
	require.Len(t, events, 1, "Duplicate event should not be stored")
}

// TestE2E_StatePersistence tests that state (lastTick/lastLogID/currentEpoch) is persisted
func TestE2E_StatePersistence(t *testing.T) {
	mockBob := NewMockBobServer(145, 22000000)
	defer mockBob.Close()

	tempDir := t.TempDir()
	storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	cfg := CreateTestConfig(mockBob, tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := startProcessor(ctx, proc)

	_, err = mockBob.WaitForSubscription(5 * time.Second)
	require.NoError(t, err)

	// Send events
	payload := CreateLogPayload(145, 22000100, 42, 0, nil)
	err = mockBob.SendLogMessage(payload, 0, 0, false)
	require.NoError(t, err)
	mockBob.SendCatchUpComplete(0, 42, 1)

	// Wait for storage
	WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
		exists, _ := storageMgr.HasEvent(145, 22000100, 42)
		return exists
	}, "event should be stored")

	// Stop processor
	stopProcessor(cancel, proc, done)

	// Verify state was persisted
	state, err := storageMgr.LoadState()
	require.NoError(t, err)
	require.True(t, state.HasState)
	require.Equal(t, uint32(145), state.CurrentEpoch)
	require.Equal(t, uint32(22000100), state.LastTick)
	require.Equal(t, int64(42), state.LastLogID)

	_ = storageMgr.Close()
}

// TestE2E_CrashRecovery tests that processor resumes from saved state
func TestE2E_CrashRecovery(t *testing.T) {
	tempDir := t.TempDir()

	// Phase 1: Process some events and stop
	func() {
		mockBob := NewMockBobServer(145, 22000000)
		defer mockBob.Close()

		storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
		require.NoError(t, err)

		cfg := CreateTestConfig(mockBob, tempDir)
		subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
		proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop(), nil)

		ctx, cancel := context.WithCancel(context.Background())
		done := startProcessor(ctx, proc)

		_, err = mockBob.WaitForSubscription(5 * time.Second)
		require.NoError(t, err)

		// Send event
		payload := CreateLogPayload(145, 22000050, 10, 0, nil)
		err = mockBob.SendLogMessage(payload, 0, 0, false)
		require.NoError(t, err)
		mockBob.SendCatchUpComplete(0, 10, 1)

		WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
			exists, _ := storageMgr.HasEvent(145, 22000050, 10)
			return exists
		}, "event should be stored")

		stopProcessor(cancel, proc, done)
		_ = storageMgr.Close()
	}()

	// Phase 2: Restart with new mock server and verify resumption
	mockBob2 := NewMockBobServer(145, 22000100)
	defer mockBob2.Close()

	storageMgr2, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	cfg2 := CreateTestConfig(mockBob2, tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc2 := processor.NewProcessor(cfg2, subs, storageMgr2, zap.NewNop(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := startProcessor(ctx, proc2)

	defer func() {
		stopProcessor(cancel, proc2, done)
		_ = storageMgr2.Close()
	}()

	// Wait for subscription and check it has lastTick set
	subReq, err := mockBob2.WaitForSubscription(5 * time.Second)
	require.NoError(t, err)
	require.NotNil(t, subReq.LastTick, "Subscription should include lastTick for resumption")
	require.Equal(t, uint32(22000050), *subReq.LastTick)

	// Verify original event is still accessible
	_, events, err := storageMgr2.GetEventsForTick(22000050)
	require.NoError(t, err)
	require.Len(t, events, 1)
}

// TestE2E_GetStatus tests the GetStatus endpoint
func TestE2E_GetStatus(t *testing.T) {
	mockBob := NewMockBobServer(145, 22000000)
	defer mockBob.Close()

	tempDir := t.TempDir()
	storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	cfg := CreateTestConfig(mockBob, tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := startProcessor(ctx, proc)

	defer func() {
		stopProcessor(cancel, proc, done)
		_ = storageMgr.Close()
	}()

	_, err = mockBob.WaitForSubscription(5 * time.Second)
	require.NoError(t, err)

	// Send events across tick range
	for tick := uint32(22000001); tick <= 22000005; tick++ {
		payload := CreateLogPayload(145, tick, uint64(tick-22000000), 0, nil)
		err = mockBob.SendLogMessage(payload, 0, 0, false)
		require.NoError(t, err)
	}
	mockBob.SendCatchUpComplete(0, 5, 5)

	// Wait for all events
	WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
		exists, _ := storageMgr.HasEvent(145, 22000005, 5)
		return exists
	}, "last event should be stored")

	// Check status
	service := grpc.NewEventsBridgeService(storageMgr, zap.NewNop())
	status, err := service.GetStatus(ctx, nil)
	require.NoError(t, err)

	require.Equal(t, uint32(145), status.CurrentEpoch)
	require.Equal(t, uint32(22000005), status.LastProcessedTick)
	require.Len(t, status.Epochs, 1)

	epochInfo := status.Epochs[0]
	require.Equal(t, uint32(145), epochInfo.Epoch)
	require.Equal(t, uint64(5), epochInfo.TotalEvents)
	require.Len(t, epochInfo.Intervals, 1)
	require.Equal(t, uint32(22000001), epochInfo.Intervals[0].FirstTick)
	require.Equal(t, uint32(22000005), epochInfo.Intervals[0].LastTick)
}

// TestE2E_EventBodyParsing tests that complex JSON bodies are correctly stored and retrieved
func TestE2E_EventBodyParsing(t *testing.T) {
	mockBob := NewMockBobServer(145, 22000000)
	defer mockBob.Close()

	tempDir := t.TempDir()
	storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	cfg := CreateTestConfig(mockBob, tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := startProcessor(ctx, proc)

	defer func() {
		stopProcessor(cancel, proc, done)
		_ = storageMgr.Close()
	}()

	_, err = mockBob.WaitForSubscription(5 * time.Second)
	require.NoError(t, err)

	// Send event with a valid qu_transfer body
	transferBody := map[string]any{
		"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"amount": 1000000,
	}

	payload := CreateLogPayload(145, 22000001, 1, 0, transferBody)
	err = mockBob.SendLogMessage(payload, 0, 0, false)
	require.NoError(t, err)
	mockBob.SendCatchUpComplete(0, 1, 1)

	// Wait for storage
	WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
		exists, _ := storageMgr.HasEvent(145, 22000001, 1)
		return exists
	}, "event should be stored")

	// Query and verify body
	service := grpc.NewEventsBridgeService(storageMgr, zap.NewNop())
	resp, err := service.GetEventsForTick(ctx, &eventsbridge.GetEventsForTickRequest{Tick: 22000001})
	require.NoError(t, err)
	require.Len(t, resp.Events, 1)

	event := resp.Events[0]
	require.NotNil(t, event.GetBody())

	bodyFields := event.GetBody().GetFields()
	require.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", bodyFields["from"].GetStringValue())
	require.Equal(t, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", bodyFields["to"].GetStringValue())
	require.Equal(t, float64(1000000), bodyFields["amount"].GetNumberValue())
	require.Equal(t, "test-digest", event.LogDigest)
}

// TestE2E_NonOKLogsSkipped tests that logs with ok=false are not stored
func TestE2E_NonOKLogsSkipped(t *testing.T) {
	mockBob := NewMockBobServer(145, 22000000)
	defer mockBob.Close()

	tempDir := t.TempDir()
	storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	cfg := CreateTestConfig(mockBob, tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := startProcessor(ctx, proc)

	defer func() {
		stopProcessor(cancel, proc, done)
		_ = storageMgr.Close()
	}()

	_, err = mockBob.WaitForSubscription(5 * time.Second)
	require.NoError(t, err)

	// Send non-OK log (should be skipped)
	nonOKPayload := CreateNonOKLogPayload(145, 22000001, 1)
	err = mockBob.SendLogMessage(nonOKPayload, 0, 0, false)
	require.NoError(t, err)

	// Send OK log
	okPayload := CreateLogPayload(145, 22000002, 2, 0, nil)
	err = mockBob.SendLogMessage(okPayload, 0, 0, false)
	require.NoError(t, err)
	mockBob.SendCatchUpComplete(0, 2, 2)

	// Wait for OK event
	WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
		exists, _ := storageMgr.HasEvent(145, 22000002, 2)
		return exists
	}, "OK event should be stored")

	// Give some time for potential non-OK event to be processed
	time.Sleep(100 * time.Millisecond)

	// Verify non-OK event was NOT stored
	exists, err := storageMgr.HasEvent(145, 22000001, 1)
	require.NoError(t, err)
	require.False(t, exists, "Non-OK event should not be stored")

	// Verify OK event WAS stored
	exists, err = storageMgr.HasEvent(145, 22000002, 2)
	require.NoError(t, err)
	require.True(t, exists, "OK event should be stored")
}

// TestE2E_MultipleLogTypes tests events from different log types
func TestE2E_MultipleLogTypes(t *testing.T) {
	mockBob := NewMockBobServer(145, 22000000)
	defer mockBob.Close()

	tempDir := t.TempDir()
	storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	cfg := CreateTestConfig(mockBob, tempDir)
	// Subscribe to multiple log types
	subs := []config.SubscriptionEntry{
		{SCIndex: 0, LogType: 0}, // qu_transfer
		{SCIndex: 0, LogType: 1}, // asset_issuance
		{SCIndex: 0, LogType: 2}, // asset_ownership_change
	}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := startProcessor(ctx, proc)

	defer func() {
		stopProcessor(cancel, proc, done)
		_ = storageMgr.Close()
	}()

	// Wait for all subscriptions (3 expected)
	for i := 0; i < 3; i++ {
		_, err = mockBob.WaitForSubscription(5 * time.Second)
		require.NoError(t, err)
	}

	// Send events from different log types
	payload0 := CreateLogPayload(145, 22000001, 1, 0, nil) // qu_transfer
	err = mockBob.SendLogMessage(payload0, 0, 0, false)
	require.NoError(t, err)

	payload1 := CreateLogPayload(145, 22000001, 2, 1, nil) // asset_issuance
	err = mockBob.SendLogMessage(payload1, 0, 1, false)
	require.NoError(t, err)

	payload2 := CreateLogPayload(145, 22000001, 3, 2, nil) // asset_ownership_change
	err = mockBob.SendLogMessage(payload2, 0, 2, false)
	require.NoError(t, err)

	mockBob.SendCatchUpComplete(0, 3, 3)

	// Wait for all events
	WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
		_, events, _ := storageMgr.GetEventsForTick(22000001)
		return len(events) >= 3
	}, "all 3 events from different log types should be stored")

	// Verify all events are stored
	_, events, err := storageMgr.GetEventsForTick(22000001)
	require.NoError(t, err)
	require.Len(t, events, 3)

	// Verify event types
	eventTypes := make(map[uint32]bool)
	for _, event := range events {
		eventTypes[event.EventType] = true
	}
	require.True(t, eventTypes[0], "qu_transfer event should be stored")
	require.True(t, eventTypes[1], "asset_issuance event should be stored")
	require.True(t, eventTypes[2], "asset_ownership_change event should be stored")
}

// TestE2E_IndexInTickResetsAcrossTicks tests that IndexInTick resets when tick changes
func TestE2E_IndexInTickResetsAcrossTicks(t *testing.T) {
	mockBob := NewMockBobServer(145, 22000000)
	defer mockBob.Close()

	tempDir := t.TempDir()
	storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	cfg := CreateTestConfig(mockBob, tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := startProcessor(ctx, proc)

	defer func() {
		stopProcessor(cancel, proc, done)
		_ = storageMgr.Close()
	}()

	_, err = mockBob.WaitForSubscription(5 * time.Second)
	require.NoError(t, err)

	// Send 2 events for tick A
	tickA := uint32(22000001)
	for i := uint64(1); i <= 2; i++ {
		payload := CreateLogPayload(145, tickA, i, 0, map[string]any{
			"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			"amount": i,
		})
		err = mockBob.SendLogMessage(payload, 0, 0, false)
		require.NoError(t, err)
	}

	// Send 3 events for tick B
	tickB := uint32(22000002)
	for i := uint64(3); i <= 5; i++ {
		payload := CreateLogPayload(145, tickB, i, 0, map[string]any{
			"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			"amount": i,
		})
		err = mockBob.SendLogMessage(payload, 0, 0, false)
		require.NoError(t, err)
	}

	mockBob.SendCatchUpComplete(0, 5, 5)

	// Wait for all events
	WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
		_, eventsA, _ := storageMgr.GetEventsForTick(tickA)
		_, eventsB, _ := storageMgr.GetEventsForTick(tickB)
		return len(eventsA) >= 2 && len(eventsB) >= 3
	}, "all events should be stored")

	// Verify tick A indices are 0,1
	service := grpc.NewEventsBridgeService(storageMgr, zap.NewNop())
	respA, err := service.GetEventsForTick(ctx, &eventsbridge.GetEventsForTickRequest{Tick: tickA})
	require.NoError(t, err)
	require.Len(t, respA.Events, 2)
	indicesA := make(map[uint32]bool)
	for _, event := range respA.Events {
		indicesA[event.IndexInTick] = true
	}
	require.True(t, indicesA[0], "Expected IndexInTick 0 for tick A")
	require.True(t, indicesA[1], "Expected IndexInTick 1 for tick A")

	// Verify tick B indices are 0,1,2 (reset from tick A)
	respB, err := service.GetEventsForTick(ctx, &eventsbridge.GetEventsForTickRequest{Tick: tickB})
	require.NoError(t, err)
	require.Len(t, respB.Events, 3)
	indicesB := make(map[uint32]bool)
	for _, event := range respB.Events {
		indicesB[event.IndexInTick] = true
	}
	require.True(t, indicesB[0], "Expected IndexInTick 0 for tick B")
	require.True(t, indicesB[1], "Expected IndexInTick 1 for tick B")
	require.True(t, indicesB[2], "Expected IndexInTick 2 for tick B")
}

// TestE2E_CrashRecoveryIndexInTick tests that IndexInTick is correctly recovered after crash
func TestE2E_CrashRecoveryIndexInTick(t *testing.T) {
	tempDir := t.TempDir()
	tick := uint32(22000050)

	// Phase 1: Process 2 events and stop
	func() {
		mockBob := NewMockBobServer(145, 22000000)
		defer mockBob.Close()

		storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
		require.NoError(t, err)

		cfg := CreateTestConfig(mockBob, tempDir)
		subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
		proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop(), nil)

		ctx, cancel := context.WithCancel(context.Background())
		done := startProcessor(ctx, proc)

		_, err = mockBob.WaitForSubscription(5 * time.Second)
		require.NoError(t, err)

		// Send 2 events
		for i := uint64(1); i <= 2; i++ {
			payload := CreateLogPayload(145, tick, i, 0, map[string]any{
				"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
				"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
				"amount": i,
			})
			err = mockBob.SendLogMessage(payload, 0, 0, false)
			require.NoError(t, err)
		}
		mockBob.SendCatchUpComplete(0, 2, 2)

		WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
			_, events, _ := storageMgr.GetEventsForTick(tick)
			return len(events) >= 2
		}, "2 events should be stored")

		stopProcessor(cancel, proc, done)
		_ = storageMgr.Close()
	}()

	// Phase 2: Restart, send 2 duplicates + 1 new event, verify new event gets index 2
	mockBob2 := NewMockBobServer(145, 22000000)
	defer mockBob2.Close()

	storageMgr2, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	cfg2 := CreateTestConfig(mockBob2, tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc2 := processor.NewProcessor(cfg2, subs, storageMgr2, zap.NewNop(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := startProcessor(ctx, proc2)

	defer func() {
		stopProcessor(cancel, proc2, done)
		_ = storageMgr2.Close()
	}()

	_, err = mockBob2.WaitForSubscription(5 * time.Second)
	require.NoError(t, err)

	// Resend the 2 duplicate events (should be deduplicated)
	for i := uint64(1); i <= 2; i++ {
		payload := CreateLogPayload(145, tick, i, 0, map[string]any{
			"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			"amount": i,
		})
		err = mockBob2.SendLogMessage(payload, 0, 0, false)
		require.NoError(t, err)
	}

	// Send 1 new event for the same tick
	newPayload := CreateLogPayload(145, tick, 3, 0, map[string]any{
		"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"amount": 3,
	})
	err = mockBob2.SendLogMessage(newPayload, 0, 0, false)
	require.NoError(t, err)
	mockBob2.SendCatchUpComplete(0, 3, 3)

	// Wait for the new event
	WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
		_, events, _ := storageMgr2.GetEventsForTick(tick)
		return len(events) >= 3
	}, "3 events should be stored after recovery")

	// Verify the new event has IndexInTick=2
	service := grpc.NewEventsBridgeService(storageMgr2, zap.NewNop())
	resp, err := service.GetEventsForTick(ctx, &eventsbridge.GetEventsForTickRequest{Tick: tick})
	require.NoError(t, err)
	require.Len(t, resp.Events, 3)

	// Find the event with logID=3 (the new one) and check its index
	for _, event := range resp.Events {
		if event.LogId == 3 {
			require.Equal(t, uint32(2), event.IndexInTick, "New event after crash recovery should have IndexInTick=2")
		}
	}
}

// TestE2E_KafkaPublishing tests that events are published to Kafka with correct field mapping
func TestE2E_KafkaPublishing(t *testing.T) {
	mockBob := NewMockBobServer(145, 22000000)
	defer mockBob.Close()

	tempDir := t.TempDir()
	storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	mockKafka := kafka.NewMockPublisher()
	cfg := CreateTestConfig(mockBob, tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop(), mockKafka)

	ctx, cancel := context.WithCancel(context.Background())
	done := startProcessor(ctx, proc)

	defer func() {
		stopProcessor(cancel, proc, done)
		_ = storageMgr.Close()
	}()

	_, err = mockBob.WaitForSubscription(5 * time.Second)
	require.NoError(t, err)

	// Send a qu_transfer event
	payload := CreateLogPayloadWithTimestamp(145, 22000001, 1, 0, map[string]any{
		"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"amount": 1000,
	}, "2024-06-15 14:30:00")
	err = mockBob.SendLogMessage(payload, 0, 0, false)
	require.NoError(t, err)
	mockBob.SendCatchUpComplete(0, 1, 1)

	// Wait for storage (implies Kafka was published first)
	WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
		exists, _ := storageMgr.HasEvent(145, 22000001, 1)
		return exists
	}, "event should be stored")

	// Verify Kafka message
	msgs := mockKafka.Messages()
	require.Len(t, msgs, 1)

	msg := msgs[0]
	assert.Equal(t, uint64(0), msg.Index)
	assert.Equal(t, uint32(0), msg.EmittingContractIndex)
	assert.Equal(t, uint32(0), msg.Type)
	assert.Equal(t, uint32(22000001), msg.TickNumber)
	assert.Equal(t, uint32(145), msg.Epoch)
	assert.Equal(t, "test-digest", msg.LogDigest)
	assert.Equal(t, uint64(1), msg.LogID)
	assert.Equal(t, int64(1718461800), msg.Timestamp)
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", msg.TransactionHash)

	// Verify body transformation (from→source, to→destination)
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", msg.Body["source"])
	assert.Equal(t, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", msg.Body["destination"])
	assert.Equal(t, int64(1000), msg.Body["amount"])
}

// TestE2E_KafkaBodyTransformations tests body transformations for all event types
func TestE2E_KafkaBodyTransformations(t *testing.T) {
	mockBob := NewMockBobServer(145, 22000000)
	defer mockBob.Close()

	tempDir := t.TempDir()
	storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	mockKafka := kafka.NewMockPublisher()
	cfg := CreateTestConfig(mockBob, tempDir)
	subs := []config.SubscriptionEntry{
		{SCIndex: 0, LogType: 0},
		{SCIndex: 0, LogType: 1},
		{SCIndex: 0, LogType: 2},
		{SCIndex: 0, LogType: 3},
		{SCIndex: 0, LogType: 8},
		{SCIndex: 0, LogType: 13},
	}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop(), mockKafka)

	ctx, cancel := context.WithCancel(context.Background())
	done := startProcessor(ctx, proc)

	defer func() {
		stopProcessor(cancel, proc, done)
		_ = storageMgr.Close()
	}()

	for i := 0; i < len(subs); i++ {
		_, err = mockBob.WaitForSubscription(5 * time.Second)
		require.NoError(t, err)
	}

	tick := uint32(22000001)

	// Type 0: qu_transfer
	p0 := CreateLogPayloadWithTimestamp(145, tick, 1, 0, map[string]any{
		"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"amount": 500,
	}, "2024-06-15 14:30:00")
	require.NoError(t, mockBob.SendLogMessage(p0, 0, 0, false))

	// Type 1: asset_issuance
	p1 := CreateLogPayloadWithTimestamp(145, tick, 2, 1, map[string]any{
		"issuerPublicKey":       "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"numberOfShares":        100000,
		"managingContractIndex": 5,
		"name":                  "QX",
		"numberOfDecimalPlaces": 0,
		"unitOfMeasurement":     "shares",
	}, "2024-06-15 14:30:00")
	require.NoError(t, mockBob.SendLogMessage(p1, 0, 1, false))

	// Type 2: asset_ownership_change
	p2 := CreateLogPayloadWithTimestamp(145, tick, 3, 2, map[string]any{
		"sourcePublicKey":      "SRCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"destinationPublicKey": "DSTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"issuerPublicKey":      "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetName":            "QX",
		"numberOfShares":       200,
	}, "2024-06-15 14:30:00")
	require.NoError(t, mockBob.SendLogMessage(p2, 0, 2, false))

	// Type 3: asset_possession_change
	p3 := CreateLogPayloadWithTimestamp(145, tick, 4, 3, map[string]any{
		"sourcePublicKey":      "SRCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"destinationPublicKey": "DSTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"issuerPublicKey":      "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetName":            "CFB",
		"numberOfShares":       300,
	}, "2024-06-15 14:30:00")
	require.NoError(t, mockBob.SendLogMessage(p3, 0, 3, false))

	// Type 8: burning
	p8 := CreateLogPayloadWithTimestamp(145, tick, 5, 8, map[string]any{
		"publicKey":              "BURNAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"amount":                 999,
		"contractIndexBurnedFor": 7,
	}, "2024-06-15 14:30:00")
	require.NoError(t, mockBob.SendLogMessage(p8, 0, 8, false))

	// Type 13: contract_reserve_deduction
	p13 := CreateLogPayloadWithTimestamp(145, tick, 6, 13, map[string]any{
		"deductedAmount":  5000,
		"remainingAmount": 95000,
		"contractIndex":   3,
	}, "2024-06-15 14:30:00")
	require.NoError(t, mockBob.SendLogMessage(p13, 0, 13, false))

	mockBob.SendCatchUpComplete(0, 6, 6)

	// Wait for all events to be stored
	WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
		return len(mockKafka.Messages()) >= 6
	}, "all 6 kafka messages should be published")

	msgs := mockKafka.Messages()
	require.Len(t, msgs, 6)

	// Verify type 0 body: from→source, to→destination
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", msgs[0].Body["source"])
	assert.Equal(t, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", msgs[0].Body["destination"])

	// Verify type 1 body: issuerPublicKey→assetIssuer, name→assetName
	assert.Equal(t, "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", msgs[1].Body["assetIssuer"])
	assert.Equal(t, "QX", msgs[1].Body["assetName"])

	// Verify type 2 body: sourcePublicKey→source, destinationPublicKey→destination, issuerPublicKey→assetIssuer
	assert.Equal(t, "SRCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", msgs[2].Body["source"])
	assert.Equal(t, "DSTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", msgs[2].Body["destination"])
	assert.Equal(t, "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", msgs[2].Body["assetIssuer"])

	// Verify type 3 body: sourcePublicKey→source, destinationPublicKey→destination, issuerPublicKey→assetIssuer
	assert.Equal(t, "SRCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", msgs[3].Body["source"])
	assert.Equal(t, "DSTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", msgs[3].Body["destination"])
	assert.Equal(t, "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", msgs[3].Body["assetIssuer"])

	// Verify type 8 body: publicKey→source
	assert.Equal(t, "BURNAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", msgs[4].Body["source"])

	// Verify type 13 body: no renames
	assert.NotNil(t, msgs[5].Body["deductedAmount"])
	assert.NotNil(t, msgs[5].Body["remainingAmount"])
	assert.NotNil(t, msgs[5].Body["contractIndex"])
}

// TestE2E_KafkaPublishFailure tests that processor disconnects and retries on Kafka failure
func TestE2E_KafkaPublishFailure(t *testing.T) {
	mockBob := NewMockBobServer(145, 22000000)
	defer mockBob.Close()

	tempDir := t.TempDir()
	storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	mockKafka := kafka.NewMockPublisher()
	// Make the first publish fail
	mockKafka.SetFailNext(fmt.Errorf("kafka broker unavailable"))

	cfg := CreateTestConfig(mockBob, tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop(), mockKafka)

	ctx, cancel := context.WithCancel(context.Background())
	done := startProcessor(ctx, proc)

	defer func() {
		stopProcessor(cancel, proc, done)
		_ = storageMgr.Close()
	}()

	// First connection - Kafka will fail
	_, err = mockBob.WaitForSubscription(5 * time.Second)
	require.NoError(t, err)

	payload := CreateLogPayloadWithTimestamp(145, 22000001, 1, 0, map[string]any{
		"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"amount": 1000,
	}, "2024-06-15 14:30:00")
	err = mockBob.SendLogMessage(payload, 0, 0, false)
	require.NoError(t, err)

	// Send catchUpComplete to trigger batch flush (and thus Kafka failure)
	mockBob.SendCatchUpComplete(0, 1, 1)

	// Wait for the processor to reconnect (it will disconnect after Kafka failure)
	// The mock server accepts new connections, so we wait for a second subscription
	_, err = mockBob.WaitForSubscription(10 * time.Second)
	require.NoError(t, err)

	// Small delay to let the mock server's old writeLoop exit so messages
	// are delivered in order on the new connection
	time.Sleep(200 * time.Millisecond)

	// Resend the event (bob resends on reconnect)
	err = mockBob.SendLogMessage(payload, 0, 0, false)
	require.NoError(t, err)
	// Small delay to ensure event is delivered before catchUpComplete
	time.Sleep(50 * time.Millisecond)
	mockBob.SendCatchUpComplete(0, 1, 1)

	// Now Kafka should succeed and event should be stored
	WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
		exists, _ := storageMgr.HasEvent(145, 22000001, 1)
		return exists
	}, "event should be stored after retry")

	msgs := mockKafka.Messages()
	require.Len(t, msgs, 1, "Event should be published to Kafka on successful retry")
	assert.Equal(t, uint64(1), msgs[0].LogID)
}

// TestE2E_IndexResetAfterDeduplication tests that IndexInTick resets to 0 for a new tick
// even when all events from the previous tick were deduplicated (pendingBatch is nil).
// This was a bug where tickEventIndex was recovered for lastTick but not reset when
// transitioning to a new tick if pendingBatch was nil.
func TestE2E_IndexResetAfterDeduplication(t *testing.T) {
	tempDir := t.TempDir()
	tickOld := uint32(22000050) // Previous tick
	tickNew := uint32(22000051) // New tick

	// Phase 1: Process 3 events for tickOld and stop
	func() {
		mockBob := NewMockBobServer(145, 22000000)
		defer mockBob.Close()

		storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
		require.NoError(t, err)

		mockKafka := kafka.NewMockPublisher()
		cfg := CreateTestConfig(mockBob, tempDir)
		subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
		proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop(), mockKafka)

		ctx, cancel := context.WithCancel(context.Background())
		done := startProcessor(ctx, proc)

		_, err = mockBob.WaitForSubscription(5 * time.Second)
		require.NoError(t, err)

		// Send 3 events for tickOld
		for i := uint64(1); i <= 3; i++ {
			payload := CreateLogPayloadWithTimestamp(145, tickOld, i, 0, map[string]any{
				"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
				"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
				"amount": i,
			}, "2024-06-15 14:30:00")
			err = mockBob.SendLogMessage(payload, 0, 0, false)
			require.NoError(t, err)
		}
		mockBob.SendCatchUpComplete(0, 3, 3)

		WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
			_, events, _ := storageMgr.GetEventsForTick(tickOld)
			return len(events) >= 3
		}, "3 events should be stored for tickOld")

		// Verify indexes are 0, 1, 2
		msgs := mockKafka.Messages()
		require.Len(t, msgs, 3)
		for i, msg := range msgs {
			require.Equal(t, uint64(i), msg.Index, "Phase 1: event %d should have index %d", i, i)
		}

		stopProcessor(cancel, proc, done)
		_ = storageMgr.Close()
	}()

	// Phase 2: Restart, resend tickOld events (will be deduplicated), then send tickNew events.
	// The tickNew events should start with index 0, not index 3.
	mockBob2 := NewMockBobServer(145, 22000000)
	defer mockBob2.Close()

	storageMgr2, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	mockKafka2 := kafka.NewMockPublisher()
	cfg2 := CreateTestConfig(mockBob2, tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc2 := processor.NewProcessor(cfg2, subs, storageMgr2, zap.NewNop(), mockKafka2)

	ctx, cancel := context.WithCancel(context.Background())
	done := startProcessor(ctx, proc2)

	defer func() {
		stopProcessor(cancel, proc2, done)
		_ = storageMgr2.Close()
	}()

	_, err = mockBob2.WaitForSubscription(5 * time.Second)
	require.NoError(t, err)

	// Resend the 3 events for tickOld (these will be deduplicated - already in storage)
	// This simulates bob's catch-up behavior after reconnect
	for i := uint64(1); i <= 3; i++ {
		payload := CreateLogPayloadWithTimestamp(145, tickOld, i, 0, map[string]any{
			"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			"amount": i,
		}, "2024-06-15 14:30:00")
		err = mockBob2.SendLogMessage(payload, 0, 0, false)
		require.NoError(t, err)
	}

	// Now send 2 events for tickNew (these should get index 0, 1 - NOT 3, 4)
	for i := uint64(4); i <= 5; i++ {
		payload := CreateLogPayloadWithTimestamp(145, tickNew, i, 0, map[string]any{
			"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			"amount": i,
		}, "2024-06-15 14:30:01")
		err = mockBob2.SendLogMessage(payload, 0, 0, false)
		require.NoError(t, err)
	}
	mockBob2.SendCatchUpComplete(0, 5, 5)

	// Wait for the new events to be stored
	WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
		_, events, _ := storageMgr2.GetEventsForTick(tickNew)
		return len(events) >= 2
	}, "2 events should be stored for tickNew")

	// Verify that tickNew events have indexes 0 and 1 in Kafka (NOT 3 and 4)
	msgs := mockKafka2.Messages()
	require.Len(t, msgs, 2, "Only 2 new events should be published to Kafka (tickOld events were deduplicated)")

	// The events should have index 0 and 1 for tickNew
	for i, msg := range msgs {
		require.Equal(t, uint64(i), msg.Index, "tickNew event %d should have index %d, not %d", i, i, msg.Index)
		require.Equal(t, tickNew, msg.TickNumber, "Event should be for tickNew")
	}

	// Also verify storage IndexInTick values
	_, eventsNew, err := storageMgr2.GetEventsForTick(tickNew)
	require.NoError(t, err)
	require.Len(t, eventsNew, 2)

	indexValues := make(map[uint32]bool)
	for _, event := range eventsNew {
		indexValues[event.IndexInTick] = true
	}
	require.True(t, indexValues[0], "tickNew should have event with IndexInTick=0")
	require.True(t, indexValues[1], "tickNew should have event with IndexInTick=1")
}

// TestE2E_KafkaDeduplication tests that deduplicated events are NOT published to Kafka
func TestE2E_KafkaDeduplication(t *testing.T) {
	tempDir := t.TempDir()
	tick := uint32(22000001)

	// Phase 1: Process an event and store it
	func() {
		mockBob := NewMockBobServer(145, 22000000)
		defer mockBob.Close()

		storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
		require.NoError(t, err)

		mockKafka := kafka.NewMockPublisher()
		cfg := CreateTestConfig(mockBob, tempDir)
		subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
		proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop(), mockKafka)

		ctx, cancel := context.WithCancel(context.Background())
		done := startProcessor(ctx, proc)

		_, err = mockBob.WaitForSubscription(5 * time.Second)
		require.NoError(t, err)

		payload := CreateLogPayloadWithTimestamp(145, tick, 1, 0, map[string]any{
			"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			"amount": 1000,
		}, "2024-06-15 14:30:00")
		err = mockBob.SendLogMessage(payload, 0, 0, false)
		require.NoError(t, err)
		mockBob.SendCatchUpComplete(0, 1, 1)

		WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
			exists, _ := storageMgr.HasEvent(145, tick, 1)
			return exists
		}, "event should be stored")

		// Verify Kafka got it
		require.Len(t, mockKafka.Messages(), 1)

		stopProcessor(cancel, proc, done)
		_ = storageMgr.Close()
	}()

	// Phase 2: Restart and resend the same event — should be deduplicated
	mockBob2 := NewMockBobServer(145, 22000000)
	defer mockBob2.Close()

	storageMgr2, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	mockKafka2 := kafka.NewMockPublisher()
	cfg2 := CreateTestConfig(mockBob2, tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc2 := processor.NewProcessor(cfg2, subs, storageMgr2, zap.NewNop(), mockKafka2)

	ctx, cancel := context.WithCancel(context.Background())
	done := startProcessor(ctx, proc2)

	defer func() {
		stopProcessor(cancel, proc2, done)
		_ = storageMgr2.Close()
	}()

	_, err = mockBob2.WaitForSubscription(5 * time.Second)
	require.NoError(t, err)

	// Resend the same event (bob resends on reconnect)
	payload := CreateLogPayloadWithTimestamp(145, tick, 1, 0, map[string]any{
		"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"amount": 1000,
	}, "2024-06-15 14:30:00")
	err = mockBob2.SendLogMessage(payload, 0, 0, false)
	require.NoError(t, err)

	// Send a new event to confirm processing continues
	payload2 := CreateLogPayloadWithTimestamp(145, tick, 2, 0, map[string]any{
		"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"amount": 2000,
	}, "2024-06-15 14:30:01")
	err = mockBob2.SendLogMessage(payload2, 0, 0, false)
	require.NoError(t, err)
	mockBob2.SendCatchUpComplete(0, 2, 2)

	// Wait for the new event
	WaitForCondition(t, 5*time.Second, 50*time.Millisecond, func() bool {
		exists, _ := storageMgr2.HasEvent(145, tick, 2)
		return exists
	}, "new event should be stored")

	// Verify only the new event was published to Kafka (duplicate was skipped)
	msgs := mockKafka2.Messages()
	require.Len(t, msgs, 1, "Only non-duplicate event should be published to Kafka")
	assert.Equal(t, uint64(2), msgs[0].LogID, "Only the new event should be published")
}
