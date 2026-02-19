package domain

import (
	"fmt"
)

// LogEventPtr mirrors LogEvent but uses pointers for all fields to detect presence during JSON unmarshalling.
type LogEventPtr struct {
	Epoch                 *uint32         `json:"epoch"`
	TickNumber            *uint64         `json:"tickNumber"`
	Index                 *uint64         `json:"index"`
	Type                  *int16          `json:"type"`
	EmittingContractIndex *uint64         `json:"emittingContractIndex"`
	LogId                 *uint64         `json:"logId"`
	LogDigest             *string         `json:"logDigest"`
	TransactionHash       *string         `json:"transactionHash"`
	Timestamp             *uint64         `json:"timestamp"`
	BodySize              *uint32         `json:"bodySize"`
	Body                  *map[string]any `json:"body"`
}

// ToLogEvent converts the pointer-based LogEventPtr into a concrete LogEvent.
// It validates the presence (non-nil) of required fields and returns an error if any are missing.
func (lep LogEventPtr) ToLogEvent() (LogEvent, error) {
	missing := make([]string, 0, 6)
	if lep.Epoch == nil {
		missing = append(missing, "epoch")
	}
	if lep.TickNumber == nil {
		missing = append(missing, "tickNumber")
	}
	if lep.LogId == nil {
		missing = append(missing, "logId")
	}
	if lep.Timestamp == nil {
		missing = append(missing, "timestamp")
	}
	if lep.LogDigest == nil {
		missing = append(missing, "logDigest")
	}
	if lep.Index == nil {
		missing = append(missing, "index")
	}
	if lep.Type == nil {
		missing = append(missing, "type")
	}
	if lep.EmittingContractIndex == nil {
		missing = append(missing, "emittingContractIndex")
	}

	if len(missing) > 0 {
		return LogEvent{}, fmt.Errorf("missing required fields: %v", missing)
	}

	if *lep.Epoch == 0 {
		missing = append(missing, "epoch")
	}
	if *lep.TickNumber == 0 {
		missing = append(missing, "tickNumber")
	}
	if *lep.LogId == 0 {
		missing = append(missing, "logId")
	}
	if *lep.LogDigest == "" {
		missing = append(missing, "logDigest")
	}
	// Disabled for the time being.
	/*if *lep.Timestamp == 0 {
		missing = append(missing, "timestamp")
	}*/

	if len(missing) > 0 {
		return LogEvent{}, fmt.Errorf("invalid zero value field(s): %v", missing)
	}

	// Optional fields: if absent, use zero-values
	var hash string
	if lep.TransactionHash != nil {
		hash = *lep.TransactionHash
	}
	var bodySize uint32
	if lep.BodySize != nil {
		bodySize = *lep.BodySize
	}
	var body map[string]any
	if lep.Body != nil {
		body = *lep.Body
	}

	return LogEvent{
		Epoch:                 *lep.Epoch,
		TickNumber:            *lep.TickNumber,
		Index:                 *lep.Index,
		Type:                  *lep.Type,
		EmittingContractIndex: *lep.EmittingContractIndex,
		LogId:                 *lep.LogId,
		LogDigest:             *lep.LogDigest,
		TransactionHash:       hash,
		Timestamp:             *lep.Timestamp,
		BodySize:              bodySize,
		Body:                  body,
	}, nil
}
