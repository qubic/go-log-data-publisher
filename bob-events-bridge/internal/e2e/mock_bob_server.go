package e2e

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/qubic/bob-events-bridge/internal/bob"
)

// MockBobServer simulates bob's /ws/qubic WebSocket protocol for testing
type MockBobServer struct {
	server              *httptest.Server
	currentEpoch        uint16
	currentVerifiedTick uint32
	initialTick         uint32
	upgrader            websocket.Upgrader

	mu                         sync.Mutex
	conn                       *websocket.Conn
	subscriptionCh             chan bob.TickStreamSubscribeParams // Receives subscribe params from client
	messagesToSend             chan []byte                        // Queue of messages to send to client
	closed                     bool
	preResponseNotifications   []bob.TickStreamResult // notifications to send before subscribe response
}

// NewMockBobServer creates a new mock bob server
func NewMockBobServer(epoch uint16, verifiedTick uint32) *MockBobServer {
	m := &MockBobServer{
		currentEpoch:        epoch,
		currentVerifiedTick: verifiedTick,
		initialTick:         verifiedTick, // Default initialTick to verifiedTick
		subscriptionCh:      make(chan bob.TickStreamSubscribeParams, 10),
		messagesToSend:      make(chan []byte, 100),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws/qubic", m.handleWS)
	mux.HandleFunc("/status", m.handleStatus)
	m.server = httptest.NewServer(mux)
	return m
}

// URL returns the WebSocket URL of the mock server
func (m *MockBobServer) URL() string {
	return "ws" + strings.TrimPrefix(m.server.URL, "http") + "/ws/qubic"
}

// StatusURL returns the HTTP status URL of the mock server
func (m *MockBobServer) StatusURL() string {
	return m.server.URL + "/status"
}

// Close shuts down the mock server
func (m *MockBobServer) Close() {
	m.mu.Lock()
	m.closed = true
	if m.conn != nil {
		_ = m.conn.Close()
	}
	m.mu.Unlock()
	m.server.Close()
}

// handleStatus handles the /status HTTP endpoint
func (m *MockBobServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	status := bob.StatusResponse{
		CurrentProcessingEpoch:   m.currentEpoch,
		CurrentFetchingTick:      m.currentVerifiedTick,
		CurrentFetchingLogTick:   m.currentVerifiedTick,
		CurrentVerifyLoggingTick: m.currentVerifiedTick,
		CurrentIndexingTick:      m.currentVerifiedTick,
		InitialTick:              m.initialTick,
	}
	m.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}

// handleWS handles incoming WebSocket connections
func (m *MockBobServer) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	m.mu.Lock()
	m.conn = conn
	m.mu.Unlock()

	// No welcome message in /ws/qubic — start read loop directly
	go m.readLoop(conn)

	// Write loop - send queued messages
	m.writeLoop(conn)
}

// readLoop reads messages from the client and handles JSON-RPC requests
func (m *MockBobServer) readLoop(conn *websocket.Conn) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}

		// Parse as JSON-RPC request
		var req bob.JsonRpcRequest
		if err := json.Unmarshal(data, &req); err != nil {
			continue
		}

		switch req.Method {
		case "qubic_subscribe":
			m.handleSubscribe(conn, req, data)
		}
	}
}

// handleSubscribe handles a qubic_subscribe JSON-RPC request
func (m *MockBobServer) handleSubscribe(conn *websocket.Conn, req bob.JsonRpcRequest, rawData []byte) {
	// Parse params: ["tickStream", {logFilters, excludeTxs, ...}]
	var rawParams []json.RawMessage
	paramsBytes, _ := json.Marshal(req.Params)
	if err := json.Unmarshal(paramsBytes, &rawParams); err != nil || len(rawParams) < 2 {
		// Send error response
		m.sendJsonRpcError(conn, req.ID, -32602, "invalid params")
		return
	}

	var subscribeParams bob.TickStreamSubscribeParams
	if err := json.Unmarshal(rawParams[1], &subscribeParams); err != nil {
		m.sendJsonRpcError(conn, req.ID, -32602, "invalid subscribe params")
		return
	}

	// Use a deterministic subscription ID for testing
	subID := "test-subscription-id"

	// Send pre-response notifications if configured (simulates bob's catch-up race)
	m.mu.Lock()
	preNotifs := m.preResponseNotifications
	m.preResponseNotifications = nil // consume them
	m.mu.Unlock()

	for _, result := range preNotifs {
		resultJSON, _ := json.Marshal(result)
		notification := bob.JsonRpcNotification{
			JsonRpc: "2.0",
			Method:  "qubic_subscription",
			Params: bob.SubscriptionParams{
				Subscription: subID,
				Result:       resultJSON,
			},
		}
		m.mu.Lock()
		if m.conn != nil {
			_ = m.conn.WriteJSON(notification)
		}
		m.mu.Unlock()
	}

	// Send JSON-RPC response with subscription ID
	resp := bob.JsonRpcResponse{
		JsonRpc: "2.0",
		ID:      req.ID,
	}
	resultBytes, _ := json.Marshal(subID)
	resp.Result = resultBytes

	m.mu.Lock()
	if m.conn != nil {
		_ = m.conn.WriteJSON(resp)
	}
	m.mu.Unlock()

	// Notify test of subscription
	select {
	case m.subscriptionCh <- subscribeParams:
	default:
	}
}

// SetSendNotificationsBeforeResponse queues notifications to be sent before the
// next subscribe response. This simulates bob's behavior of starting tickStream
// catch-up before sending the JSON-RPC response.
func (m *MockBobServer) SetSendNotificationsBeforeResponse(notifications []bob.TickStreamResult) {
	m.mu.Lock()
	m.preResponseNotifications = notifications
	m.mu.Unlock()
}

// sendJsonRpcError sends a JSON-RPC error response
func (m *MockBobServer) sendJsonRpcError(conn *websocket.Conn, id int, code int, message string) {
	resp := bob.JsonRpcResponse{
		JsonRpc: "2.0",
		ID:      id,
		Error: &bob.JsonRpcError{
			Code:    code,
			Message: message,
		},
	}
	m.mu.Lock()
	if m.conn != nil {
		_ = m.conn.WriteJSON(resp)
	}
	m.mu.Unlock()
}

// writeLoop sends queued messages to the client
func (m *MockBobServer) writeLoop(conn *websocket.Conn) {
	for msg := range m.messagesToSend {
		m.mu.Lock()
		closed := m.closed
		currentConn := m.conn
		m.mu.Unlock()

		if closed {
			return
		}

		// If this writeLoop's connection is no longer the active one,
		// re-queue the message for the new connection's writeLoop and exit.
		if currentConn != conn {
			m.messagesToSend <- msg
			return
		}

		if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			return
		}
	}
}

// SendTickStreamResult sends a tickStream notification containing logs for a tick.
// This replaces the old SendLogMessage — all logs for a tick are sent in one message.
func (m *MockBobServer) SendTickStreamResult(result bob.TickStreamResult) error {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return err
	}

	notification := bob.JsonRpcNotification{
		JsonRpc: "2.0",
		Method:  "qubic_subscription",
		Params: bob.SubscriptionParams{
			Subscription: "test-subscription-id",
			Result:       resultJSON,
		},
	}

	data, err := json.Marshal(notification)
	if err != nil {
		return err
	}

	m.messagesToSend <- data
	return nil
}

// SendCatchUpComplete sends a catchUpComplete tickStream notification
func (m *MockBobServer) SendCatchUpComplete() {
	result := bob.TickStreamResult{
		CatchUpComplete: true,
	}
	_ = m.SendTickStreamResult(result)
}

// WaitForSubscription blocks until a subscription is received or timeout
func (m *MockBobServer) WaitForSubscription(timeout time.Duration) (bob.TickStreamSubscribeParams, error) {
	select {
	case params := <-m.subscriptionCh:
		return params, nil
	case <-time.After(timeout):
		return bob.TickStreamSubscribeParams{}, ErrTimeout
	}
}

// WaitForConnection waits until a client connects
func (m *MockBobServer) WaitForConnection(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		connected := m.conn != nil
		m.mu.Unlock()
		if connected {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ErrTimeout
}

// UpdateEpoch updates the current epoch (for epoch transition tests)
func (m *MockBobServer) UpdateEpoch(epoch uint16) {
	m.mu.Lock()
	m.currentEpoch = epoch
	m.mu.Unlock()
}

// UpdateVerifiedTick updates the current verified tick
func (m *MockBobServer) UpdateVerifiedTick(tick uint32) {
	m.mu.Lock()
	m.currentVerifiedTick = tick
	m.mu.Unlock()
}

// UpdateInitialTick updates the initial tick returned by the status endpoint
func (m *MockBobServer) UpdateInitialTick(tick uint32) {
	m.mu.Lock()
	m.initialTick = tick
	m.mu.Unlock()
}
