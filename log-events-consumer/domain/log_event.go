package domain

type LogEvent struct {
	Ok        bool   `json:"ok"`
	Epoch     uint32 `json:"epoch"`
	Tick      uint32 `json:"tick"`
	Id        uint64 `json:"id"`
	Hash      string `json:"hash"` // hex -> base64
	Type      uint32 `json:"type"`
	TypeName  string `json:"typeName"`
	Timestamp string `json:"timestamp"`
	TxHash    string `json:"txHash"` // hex -> base64
	BodySize  uint32 `json:"bodySize"`
	Body      string `json:"body"` // []byte -> base64
}
