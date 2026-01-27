package e2e

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/qubic/bob-events-bridge/internal/bob"
	"github.com/qubic/bob-events-bridge/internal/config"
)

// ErrTimeout is returned when a wait operation times out
var ErrTimeout = errors.New("timeout waiting for condition")

// CreateTestConfig creates a test configuration pointing to the mock server
func CreateTestConfig(wsURL, storagePath string) *config.Config {
	return &config.Config{
		Bob: config.BobConfig{
			WebSocketURL: wsURL,
			StatusURL:    "http://localhost:40420/status",
			LogTypes:     "0 1 2 3",
		},
		Storage: config.StorageConfig{
			BasePath: storagePath,
		},
		Server: config.ServerConfig{
			GRPCAddr: "0.0.0.0:0",
			HTTPAddr: "0.0.0.0:0",
		},
		Debug: true,
	}
}

// CreateLogPayload creates a LogPayload with all required fields
func CreateLogPayload(epoch uint16, tick uint32, logID uint64, eventType uint32, body map[string]any) bob.LogPayload {
	var bodyJSON json.RawMessage
	if body != nil {
		bodyJSON, _ = json.Marshal(body)
	}

	return bob.LogPayload{
		OK:        true,
		Epoch:     epoch,
		Tick:      tick,
		Type:      eventType,
		LogID:     logID,
		LogDigest: "test-digest",
		BodySize:  uint32(len(bodyJSON)),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		TxHash:    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		Body:      bodyJSON,
	}
}

// CreateNonOKLogPayload creates a LogPayload with OK=false
func CreateNonOKLogPayload(epoch uint16, tick uint32, logID uint64) bob.LogPayload {
	return bob.LogPayload{
		OK:    false,
		Epoch: epoch,
		Tick:  tick,
		LogID: logID,
	}
}

// WaitForCondition polls a condition until it returns true or timeout
func WaitForCondition(t *testing.T, timeout, interval time.Duration, condition func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(interval)
	}
	t.Fatalf("Timeout waiting for condition: %s", msg)
}

// WaitForEventCount waits until storage has at least the expected number of events for a tick
func WaitForEventCount(t *testing.T, hasEventFn func() (int, error), expectedCount int, timeout time.Duration) {
	t.Helper()
	WaitForCondition(t, timeout, 50*time.Millisecond, func() bool {
		count, err := hasEventFn()
		return err == nil && count >= expectedCount
	}, "expected event count not reached")
}

// TestSubscriptions returns the default test subscriptions
func TestSubscriptions() []config.SubscriptionEntry {
	return []config.SubscriptionEntry{
		{SCIndex: 0, LogType: 0},
		{SCIndex: 0, LogType: 1},
		{SCIndex: 0, LogType: 2},
		{SCIndex: 0, LogType: 3},
	}
}
