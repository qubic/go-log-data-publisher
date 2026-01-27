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

// MockBobServer simulates bob's WebSocket protocol for testing
type MockBobServer struct {
	server              *httptest.Server
	currentEpoch        uint16
	currentVerifiedTick uint32
	upgrader            websocket.Upgrader

	mu             sync.Mutex
	conn           *websocket.Conn
	subscriptionCh chan bob.SubscribeRequest // Receives subscriptions from client
	messagesToSend chan []byte               // Queue of messages to send to client
	closed         bool
}

// NewMockBobServer creates a new mock bob server
func NewMockBobServer(epoch uint16, verifiedTick uint32) *MockBobServer {
	m := &MockBobServer{
		currentEpoch:        epoch,
		currentVerifiedTick: verifiedTick,
		subscriptionCh:      make(chan bob.SubscribeRequest, 10),
		messagesToSend:      make(chan []byte, 100),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}

	m.server = httptest.NewServer(http.HandlerFunc(m.handleWS))
	return m
}

// URL returns the WebSocket URL of the mock server
func (m *MockBobServer) URL() string {
	return "ws" + strings.TrimPrefix(m.server.URL, "http") + "/ws/logs"
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

// handleWS handles incoming WebSocket connections
func (m *MockBobServer) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	m.mu.Lock()
	m.conn = conn
	m.mu.Unlock()

	// Send welcome message
	welcome := bob.WelcomeMessage{
		Type:                bob.MessageTypeWelcome,
		CurrentVerifiedTick: m.currentVerifiedTick,
		CurrentEpoch:        m.currentEpoch,
	}
	if err := conn.WriteJSON(welcome); err != nil {
		return
	}

	// Start read loop in goroutine
	go m.readLoop(conn)

	// Write loop - send queued messages
	m.writeLoop(conn)
}

// readLoop reads messages from the client and handles them
func (m *MockBobServer) readLoop(conn *websocket.Conn) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}

		// Parse the message to determine type
		var base struct {
			Action string `json:"action"`
		}
		if err := json.Unmarshal(data, &base); err != nil {
			continue
		}

		switch base.Action {
		case "subscribe":
			var req bob.SubscribeRequest
			if err := json.Unmarshal(data, &req); err == nil {
				// Send ack
				ack := bob.AckMessage{
					Type:               bob.MessageTypeAck,
					Action:             "subscribe",
					Success:            true,
					SubscriptionsAdded: 1,
				}
				if req.SCIndex != nil {
					ack.SCIndex = *req.SCIndex
				}
				if req.LogType != nil {
					ack.LogType = *req.LogType
				}

				m.mu.Lock()
				if m.conn != nil {
					_ = m.conn.WriteJSON(ack)
				}
				m.mu.Unlock()

				// Notify test
				select {
				case m.subscriptionCh <- req:
				default:
				}
			}

		case "ping":
			pong := bob.PongMessage{
				Type:        bob.MessageTypePong,
				ServerTick:  m.currentVerifiedTick,
				ServerEpoch: m.currentEpoch,
			}
			m.mu.Lock()
			if m.conn != nil {
				_ = m.conn.WriteJSON(pong)
			}
			m.mu.Unlock()
		}
	}
}

// writeLoop sends queued messages to the client
func (m *MockBobServer) writeLoop(conn *websocket.Conn) {
	for msg := range m.messagesToSend {
		m.mu.Lock()
		closed := m.closed
		m.mu.Unlock()

		if closed {
			return
		}

		if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			return
		}
	}
}

// SendLogMessage queues a log message to be sent to the client
func (m *MockBobServer) SendLogMessage(payload bob.LogPayload, scIndex, logType uint32, isCatchUp bool) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	logMsg := bob.LogMessage{
		Type:      bob.MessageTypeLog,
		SCIndex:   scIndex,
		LogType:   logType,
		IsCatchUp: isCatchUp,
		Message:   payloadJSON,
	}

	data, err := json.Marshal(logMsg)
	if err != nil {
		return err
	}

	m.messagesToSend <- data
	return nil
}

// SendCatchUpComplete queues a catchUpComplete message
func (m *MockBobServer) SendCatchUpComplete(fromLogID, toLogID int64, logsDelivered int) {
	msg := bob.CatchUpCompleteMessage{
		Type:          bob.MessageTypeCatchUpComplete,
		FromLogID:     fromLogID,
		ToLogID:       toLogID,
		LogsDelivered: logsDelivered,
	}

	data, _ := json.Marshal(msg)
	m.messagesToSend <- data
}

// WaitForSubscription blocks until a subscription is received or timeout
func (m *MockBobServer) WaitForSubscription(timeout time.Duration) (bob.SubscribeRequest, error) {
	select {
	case req := <-m.subscriptionCh:
		return req, nil
	case <-time.After(timeout):
		return bob.SubscribeRequest{}, ErrTimeout
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
