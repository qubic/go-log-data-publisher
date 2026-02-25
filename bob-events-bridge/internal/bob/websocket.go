package bob

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// WSClient manages the WebSocket connection to bob
type WSClient struct {
	url    string
	conn   *websocket.Conn
	logger *zap.Logger

	mu              sync.Mutex
	connected       bool
	pendingMessages [][]byte // messages received during Subscribe before the RPC response
}

// NewWSClient creates a new WebSocket client
func NewWSClient(url string, logger *zap.Logger) *WSClient {
	return &WSClient{
		url:    url,
		logger: logger,
	}
}

// Connect establishes the WebSocket connection to bob's /ws/qubic endpoint.
// Unlike the old /ws/logs, there is no welcome message on connect.
func (c *WSClient) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected {
		return fmt.Errorf("already connected")
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(c.url, nil)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	c.conn = conn
	c.connected = true

	c.logger.Info("Connected to bob", zap.String("url", c.url))

	return nil
}

// Subscribe sends a JSON-RPC 2.0 qubic_subscribe request for tickStream
// and returns the subscription ID from the response.
func (c *WSClient) Subscribe(startTick uint32) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected {
		return "", fmt.Errorf("not connected")
	}

	params := TickStreamSubscribeParams{
		ExcludeTxs:     true,
		SkipEmptyTicks: true,
	}
	if startTick > 0 {
		params.StartTick = startTick
	}

	req := JsonRpcRequest{
		JsonRpc: "2.0",
		ID:      1,
		Method:  "qubic_subscribe",
		Params:  []interface{}{"tickStream", params},
	}

	data, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("failed to marshal subscribe request: %w", err)
	}

	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return "", fmt.Errorf("failed to send subscribe request: %w", err)
	}

	c.logger.Info("Sent tickStream subscription",
		zap.Uint32("startTick", startTick))

	// Read messages until we find the JSON-RPC response with our request ID.
	// Bob may send tickStream notifications before the subscribe response
	// when startTick triggers catch-up, so we buffer those early notifications.
	c.pendingMessages = nil
	const maxIterations = 1000

	for i := 0; i < maxIterations; i++ {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			return "", fmt.Errorf("failed to read subscribe response: %w", err)
		}

		var resp JsonRpcResponse
		if err := json.Unmarshal(msg, &resp); err != nil {
			// Not valid JSON-RPC — buffer as notification
			c.pendingMessages = append(c.pendingMessages, msg)
			c.logger.Debug("Buffered non-JSON-RPC message during subscribe",
				zap.Int("buffered", len(c.pendingMessages)))
			continue
		}

		// Check if this is our response (has matching request ID)
		if resp.ID == req.ID {
			if resp.Error != nil {
				return "", fmt.Errorf("subscribe error (code %d): %s", resp.Error.Code, resp.Error.Message)
			}

			var subscriptionID string
			if err := json.Unmarshal(resp.Result, &subscriptionID); err != nil {
				return "", fmt.Errorf("failed to parse subscription ID: %w", err)
			}

			if len(c.pendingMessages) > 0 {
				c.logger.Info("Subscribed to tickStream (buffered early notifications)",
					zap.String("subscriptionID", subscriptionID),
					zap.Int("bufferedMessages", len(c.pendingMessages)))
			} else {
				c.logger.Info("Subscribed to tickStream",
					zap.String("subscriptionID", subscriptionID))
			}

			return subscriptionID, nil
		}

		// Not our response — it's an early notification, buffer it
		c.pendingMessages = append(c.pendingMessages, msg)
		c.logger.Debug("Buffered early notification during subscribe",
			zap.Int("buffered", len(c.pendingMessages)))
	}

	return "", fmt.Errorf("did not receive subscribe response after %d messages", maxIterations)
}

// ReadMessage reads the next message from the WebSocket.
// It drains any messages buffered during Subscribe before reading from the connection.
func (c *WSClient) ReadMessage() ([]byte, error) {
	c.mu.Lock()
	if len(c.pendingMessages) > 0 {
		msg := c.pendingMessages[0]
		c.pendingMessages = c.pendingMessages[1:]
		c.mu.Unlock()
		return msg, nil
	}
	conn := c.conn
	c.mu.Unlock()

	if conn == nil {
		return nil, fmt.Errorf("not connected")
	}

	_, msg, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// Close closes the WebSocket connection
func (c *WSClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected {
		return nil
	}

	c.connected = false

	// Send close message
	deadline := time.Now().Add(time.Second)
	_ = c.conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		deadline)

	return c.conn.Close()
}

// IsConnected returns true if the client is connected
func (c *WSClient) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

// SetConnected sets the connection state (used after errors)
func (c *WSClient) SetConnected(connected bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = connected
}
