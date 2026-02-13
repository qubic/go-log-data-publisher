package bob

import "encoding/json"

// Message types from bob WebSocket
const (
	MessageTypeWelcome         = "welcome"
	MessageTypeLog             = "log"
	MessageTypeCatchUpComplete = "catchUpComplete"
	MessageTypeAck             = "ack"
	MessageTypePong            = "pong"
	MessageTypeError           = "error"
)

// BaseMessage is used to determine the message type
type BaseMessage struct {
	Type string `json:"type"`
}

// WelcomeMessage is sent by the server on connection
type WelcomeMessage struct {
	Type                string `json:"type"`
	CurrentVerifiedTick uint32 `json:"currentVerifiedTick"`
	CurrentEpoch        uint16 `json:"currentEpoch"`
}

// LogMessage contains a log event from the server
type LogMessage struct {
	Type      string          `json:"type"`
	SCIndex   uint32          `json:"scIndex"`
	LogType   uint32          `json:"logType"`
	IsCatchUp bool            `json:"isCatchUp"`
	Message   json.RawMessage `json:"message"` // Preserve raw JSON for storage
}

// LogPayload is the parsed content of LogMessage.Message
type LogPayload struct {
	OK          bool            `json:"ok"`
	Epoch       uint16          `json:"epoch"`
	Tick        uint32          `json:"tick"`
	Type        uint32          `json:"type"`
	LogID       uint64          `json:"logId"`
	LogDigest   string          `json:"logDigest"`
	BodySize    uint32          `json:"bodySize"`
	LogTypeName string          `json:"logTypename,omitempty"`
	Timestamp   uint64          `json:"timestamp,omitempty"`
	TxHash      string          `json:"txHash,omitempty"`
	Body        json.RawMessage `json:"body,omitempty"`
}

// CatchUpCompleteMessage indicates catch-up has finished
type CatchUpCompleteMessage struct {
	Type          string `json:"type"`
	FromLogID     int64  `json:"fromLogId,omitempty"`
	ToLogID       int64  `json:"toLogId,omitempty"`
	FromTick      uint32 `json:"fromTick,omitempty"`
	ToTick        uint32 `json:"toTick,omitempty"`
	LogsDelivered int    `json:"logsDelivered"`
}

// AckMessage is the server's response to subscribe/unsubscribe
type AckMessage struct {
	Type               string `json:"type"`
	Action             string `json:"action"`
	Success            bool   `json:"success"`
	SubscriptionsAdded int    `json:"subscriptionsAdded,omitempty"`
	SCIndex            uint32 `json:"scIndex,omitempty"`
	LogType            uint32 `json:"logType,omitempty"`
}

// PongMessage is the response to a ping
type PongMessage struct {
	Type        string `json:"type"`
	ServerTick  uint32 `json:"serverTick"`
	ServerEpoch uint16 `json:"serverEpoch"`
}

// ErrorMessage indicates an error from the server
type ErrorMessage struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Code    string `json:"code"`
}

// SubscribeRequest is sent to subscribe to log types
type SubscribeRequest struct {
	Action        string              `json:"action"`
	Subscriptions []SubscriptionEntry `json:"subscriptions,omitempty"`
	SCIndex       *uint32             `json:"scIndex,omitempty"`
	LogType       *uint32             `json:"logType,omitempty"`
	LastLogID     *int64              `json:"lastLogId,omitempty"`
	LastTick      *uint32             `json:"lastTick,omitempty"`
}

// SubscriptionEntry represents a single subscription in a batch
type SubscriptionEntry struct {
	SCIndex uint32 `json:"scIndex"`
	LogType uint32 `json:"logType"`
}

// PingRequest is sent to check connectivity
type PingRequest struct {
	Action string `json:"action"`
}

// StatusResponse represents the /status endpoint response
type StatusResponse struct {
	CurrentProcessingEpoch   uint16 `json:"currentProcessingEpoch"`
	CurrentFetchingTick      uint32 `json:"currentFetchingTick"`
	CurrentFetchingLogTick   uint32 `json:"currentFetchingLogTick"`
	CurrentVerifyLoggingTick uint32 `json:"currentVerifyLoggingTick"`
	CurrentIndexingTick      uint32 `json:"currentIndexingTick"`
	InitialTick              uint32 `json:"initialTick"`
}
