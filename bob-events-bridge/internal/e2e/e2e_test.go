package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	eventsbridge "github.com/qubic/bob-events-bridge/api/events-bridge/v1"
	"github.com/qubic/bob-events-bridge/internal/config"
	"github.com/qubic/bob-events-bridge/internal/grpc"
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
	cfg := CreateTestConfig(mockBob.URL(), tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop())

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
}

// TestE2E_MultipleEventsPerTick tests multiple events for the same tick
func TestE2E_MultipleEventsPerTick(t *testing.T) {
	mockBob := NewMockBobServer(145, 22000000)
	defer mockBob.Close()

	tempDir := t.TempDir()
	storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	cfg := CreateTestConfig(mockBob.URL(), tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop())

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

	// Verify all log IDs are present
	logIDs := make(map[uint64]bool)
	for _, event := range resp.Events {
		logIDs[event.LogId] = true
	}
	for i := uint64(1); i <= 5; i++ {
		require.True(t, logIDs[i], "Missing event with logID %d", i)
	}
}

// TestE2E_EpochTransition tests events across epoch boundaries
func TestE2E_EpochTransition(t *testing.T) {
	mockBob := NewMockBobServer(145, 22000000)
	defer mockBob.Close()

	tempDir := t.TempDir()
	storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	cfg := CreateTestConfig(mockBob.URL(), tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop())

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

	cfg := CreateTestConfig(mockBob.URL(), tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop())

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

	cfg := CreateTestConfig(mockBob.URL(), tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop())

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

		cfg := CreateTestConfig(mockBob.URL(), tempDir)
		subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
		proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop())

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

	cfg2 := CreateTestConfig(mockBob2.URL(), tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc2 := processor.NewProcessor(cfg2, subs, storageMgr2, zap.NewNop())

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

	cfg := CreateTestConfig(mockBob.URL(), tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop())

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

	cfg := CreateTestConfig(mockBob.URL(), tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop())

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
}

// TestE2E_NonOKLogsSkipped tests that logs with ok=false are not stored
func TestE2E_NonOKLogsSkipped(t *testing.T) {
	mockBob := NewMockBobServer(145, 22000000)
	defer mockBob.Close()

	tempDir := t.TempDir()
	storageMgr, err := storage.NewManager(tempDir, zap.NewNop())
	require.NoError(t, err)

	cfg := CreateTestConfig(mockBob.URL(), tempDir)
	subs := []config.SubscriptionEntry{{SCIndex: 0, LogType: 0}}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop())

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

	cfg := CreateTestConfig(mockBob.URL(), tempDir)
	// Subscribe to multiple log types
	subs := []config.SubscriptionEntry{
		{SCIndex: 0, LogType: 0}, // qu_transfer
		{SCIndex: 0, LogType: 1}, // asset_issuance
		{SCIndex: 0, LogType: 2}, // asset_ownership_change
	}
	proc := processor.NewProcessor(cfg, subs, storageMgr, zap.NewNop())

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
