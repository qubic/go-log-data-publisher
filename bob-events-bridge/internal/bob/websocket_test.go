package bob

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// testWSServer is a minimal WebSocket server for unit testing WSClient
type testWSServer struct {
	server   *httptest.Server
	upgrader websocket.Upgrader
	handler  func(conn *websocket.Conn)
}

func newTestWSServer(handler func(conn *websocket.Conn)) *testWSServer {
	s := &testWSServer{
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
		handler:  handler,
	}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := s.upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		s.handler(conn)
	}))
	return s
}

func (s *testWSServer) URL() string {
	return "ws" + strings.TrimPrefix(s.server.URL, "http")
}

func (s *testWSServer) Close() {
	s.server.Close()
}

// sendResponse sends a JSON-RPC response with the given ID and string result
func sendResponse(conn *websocket.Conn, id int, result string) {
	resultBytes, _ := json.Marshal(result)
	resp := JsonRpcResponse{
		JsonRpc: "2.0",
		ID:      id,
		Result:  resultBytes,
	}
	_ = conn.WriteJSON(resp)
}

// sendNotification sends a JSON-RPC notification (no ID field)
func sendNotification(conn *websocket.Conn, method string, payload interface{}) {
	payloadBytes, _ := json.Marshal(payload)
	notif := JsonRpcNotification{
		JsonRpc: "2.0",
		Method:  method,
		Params: SubscriptionParams{
			Subscription: "test-sub",
			Result:       payloadBytes,
		},
	}
	_ = conn.WriteJSON(notif)
}

func TestSubscribe_ResponseFirst(t *testing.T) {
	srv := newTestWSServer(func(conn *websocket.Conn) {
		// Read subscribe request
		_, _, err := conn.ReadMessage()
		require.NoError(t, err)

		// Send response immediately
		sendResponse(conn, 1, "sub-id-123")

		// Keep connection open until test finishes
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	client := NewWSClient(srv.URL(), zap.NewNop())
	require.NoError(t, client.Connect())
	defer func() { _ = client.Close() }()

	subID, err := client.Subscribe(0)
	require.NoError(t, err)
	assert.Equal(t, "sub-id-123", subID)

	// No pending messages should be buffered
	client.mu.Lock()
	assert.Empty(t, client.pendingMessages)
	client.mu.Unlock()
}

func TestSubscribe_NotificationsBeforeResponse(t *testing.T) {
	srv := newTestWSServer(func(conn *websocket.Conn) {
		// Read subscribe request
		_, _, err := conn.ReadMessage()
		require.NoError(t, err)

		// Send 3 notifications BEFORE the response
		for i := 0; i < 3; i++ {
			sendNotification(conn, "qubic_subscription", map[string]interface{}{
				"tick": 100 + i,
			})
		}

		// Now send the response
		sendResponse(conn, 1, "sub-id-456")

		// Keep connection open
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	client := NewWSClient(srv.URL(), zap.NewNop())
	require.NoError(t, client.Connect())
	defer func() { _ = client.Close() }()

	subID, err := client.Subscribe(100)
	require.NoError(t, err)
	assert.Equal(t, "sub-id-456", subID)

	// 3 notifications should be buffered
	client.mu.Lock()
	assert.Len(t, client.pendingMessages, 3)
	client.mu.Unlock()
}

func TestReadMessage_DrainsPendingFirst(t *testing.T) {
	srv := newTestWSServer(func(conn *websocket.Conn) {
		// Read subscribe request
		_, _, err := conn.ReadMessage()
		require.NoError(t, err)

		// Send 2 notifications before response
		sendNotification(conn, "qubic_subscription", map[string]interface{}{"tick": 200})
		sendNotification(conn, "qubic_subscription", map[string]interface{}{"tick": 201})

		// Send response
		sendResponse(conn, 1, "sub-id-789")

		// Send a message after response (will be read from conn)
		sendNotification(conn, "qubic_subscription", map[string]interface{}{"tick": 202})

		// Keep connection open
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	client := NewWSClient(srv.URL(), zap.NewNop())
	require.NoError(t, client.Connect())
	defer func() { _ = client.Close() }()

	subID, err := client.Subscribe(200)
	require.NoError(t, err)
	assert.Equal(t, "sub-id-789", subID)

	// Read first pending message (tick 200)
	msg1, err := client.ReadMessage()
	require.NoError(t, err)
	var notif1 JsonRpcNotification
	require.NoError(t, json.Unmarshal(msg1, &notif1))
	assert.Equal(t, "qubic_subscription", notif1.Method)

	// Read second pending message (tick 201)
	msg2, err := client.ReadMessage()
	require.NoError(t, err)
	var notif2 JsonRpcNotification
	require.NoError(t, json.Unmarshal(msg2, &notif2))
	assert.Equal(t, "qubic_subscription", notif2.Method)

	// No more pending — next read comes from the actual connection (tick 202)
	client.mu.Lock()
	assert.Empty(t, client.pendingMessages)
	client.mu.Unlock()

	msg3, err := client.ReadMessage()
	require.NoError(t, err)
	var notif3 JsonRpcNotification
	require.NoError(t, json.Unmarshal(msg3, &notif3))
	assert.Equal(t, "qubic_subscription", notif3.Method)
}
