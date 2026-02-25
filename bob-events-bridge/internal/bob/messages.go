package bob

import "encoding/json"

// JSON-RPC 2.0 request for subscribing
type JsonRpcRequest struct {
	JsonRpc string      `json:"jsonrpc"`
	ID      int         `json:"id"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
}

// JSON-RPC 2.0 response (for subscription ID)
type JsonRpcResponse struct {
	JsonRpc string          `json:"jsonrpc"`
	ID      int             `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *JsonRpcError   `json:"error,omitempty"`
}

// JsonRpcError represents a JSON-RPC 2.0 error
type JsonRpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// JSON-RPC 2.0 notification (subscription events)
type JsonRpcNotification struct {
	JsonRpc string             `json:"jsonrpc"`
	Method  string             `json:"method"`
	Params  SubscriptionParams `json:"params"`
}

// SubscriptionParams wraps the subscription ID and result payload
type SubscriptionParams struct {
	Subscription string          `json:"subscription"`
	Result       json.RawMessage `json:"result"`
}

// TickStreamResult is the payload for a tickStream notification
type TickStreamResult struct {
	Epoch           uint16       `json:"epoch"`
	Tick            uint32       `json:"tick"`
	IsCatchUp       bool         `json:"isCatchUp"`
	IsSkipped       bool         `json:"isSkipped"`
	TotalLogs       int          `json:"totalLogs"`
	FilteredLogs    int          `json:"filteredLogs"`
	Logs            []LogPayload `json:"logs"`
	CatchUpComplete bool         `json:"catchUpComplete,omitempty"`
}

// LogFilter for tickStream subscription
type LogFilter struct {
	SCIndex uint32 `json:"scIndex"`
	LogType uint32 `json:"logType"`
}

// TickStreamSubscribeParams for qubic_subscribe
type TickStreamSubscribeParams struct {
	LogFilters     []LogFilter `json:"logFilters,omitempty"`
	ExcludeTxs     bool        `json:"excludeTxs"`
	SkipEmptyTicks bool        `json:"skipEmptyTicks"`
	StartTick      uint32      `json:"startTick,omitempty"`
}

// LogPayload is the parsed content of an individual log event
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

// StatusResponse represents the /status endpoint response
type StatusResponse struct {
	CurrentProcessingEpoch   uint16 `json:"currentProcessingEpoch"`
	CurrentFetchingTick      uint32 `json:"currentFetchingTick"`
	CurrentFetchingLogTick   uint32 `json:"currentFetchingLogTick"`
	CurrentVerifyLoggingTick uint32 `json:"currentVerifyLoggingTick"`
	CurrentIndexingTick      uint32 `json:"currentIndexingTick"`
	InitialTick              uint32 `json:"initialTick"`
}
