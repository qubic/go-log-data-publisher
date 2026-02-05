package bob

import (
	"context"
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

	mu        sync.Mutex
	connected bool
}

// NewWSClient creates a new WebSocket client
func NewWSClient(url string, logger *zap.Logger) *WSClient {
	return &WSClient{
		url:    url,
		logger: logger,
	}
}

// Connect establishes the WebSocket connection
func (c *WSClient) Connect(ctx context.Context) (*WelcomeMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected {
		return nil, fmt.Errorf("already connected")
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.DialContext(ctx, c.url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	c.conn = conn
	c.connected = true

	// Read the welcome message
	_, msg, err := conn.ReadMessage()
	if err != nil {
		_ = c.conn.Close()
		c.connected = false
		return nil, fmt.Errorf("failed to read welcome message: %w", err)
	}

	var welcome WelcomeMessage
	if err := json.Unmarshal(msg, &welcome); err != nil {
		_ = c.conn.Close()
		c.connected = false
		return nil, fmt.Errorf("failed to parse welcome message: %w", err)
	}

	if welcome.Type != MessageTypeWelcome {
		_ = c.conn.Close()
		c.connected = false
		return nil, fmt.Errorf("expected welcome message, got: %s", welcome.Type)
	}

	c.logger.Info("Connected to bob",
		zap.Uint32("currentVerifiedTick", welcome.CurrentVerifiedTick),
		zap.Uint16("currentEpoch", welcome.CurrentEpoch))

	return &welcome, nil
}

// Subscribe sends subscription requests with optional resumption from lastTick.
// Each subscription is sent individually with scIndex + logType.
// The lastTick param is only sent on the FIRST subscription since catch-up
// happens once from that tick boundary.
func (c *WSClient) Subscribe(subscriptions []SubscriptionEntry, lastTick *uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected {
		return fmt.Errorf("not connected")
	}

	// Send individual subscription for each log type
	// Only include lastTick on the FIRST subscription
	// (catch-up only needs to happen once from that tick boundary)
	for i, sub := range subscriptions {
		scIndex := sub.SCIndex
		logType := sub.LogType

		req := SubscribeRequest{
			Action:  "subscribe",
			SCIndex: &scIndex,
			LogType: &logType,
		}

		// Only add lastTick on the first subscription
		if i == 0 && lastTick != nil && *lastTick > 0 {
			req.LastTick = lastTick
		}

		data, err := json.Marshal(req)
		if err != nil {
			return fmt.Errorf("failed to marshal subscribe request: %w", err)
		}

		if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
			return fmt.Errorf("failed to send subscribe request: %w", err)
		}

		c.logger.Debug("Sent subscription",
			zap.Uint32("scIndex", scIndex),
			zap.Uint32("logType", logType),
			zap.Bool("withResume", i == 0 && lastTick != nil))
	}

	c.logger.Info("Sent subscription requests",
		zap.Int("numSubscriptions", len(subscriptions)),
		zap.Uint32p("lastTick", lastTick))

	return nil
}

// ReadMessage reads the next message from the WebSocket
func (c *WSClient) ReadMessage() ([]byte, error) {
	c.mu.Lock()
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

// SendPing sends an application-level ping
func (c *WSClient) SendPing() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected {
		return fmt.Errorf("not connected")
	}

	req := PingRequest{Action: "ping"}
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}

	return c.conn.WriteMessage(websocket.TextMessage, data)
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
