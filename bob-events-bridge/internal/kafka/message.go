package kafka

import (
	"fmt"
	"time"

	"github.com/qubic/bob-events-bridge/internal/bob"
)

// EventMessage is the Kafka message payload
type EventMessage struct {
	Index                 uint64         `json:"index"`
	EmittingContractIndex uint32         `json:"emittingContractIndex"`
	Type                  uint32         `json:"type"`
	TickNumber            uint32         `json:"tickNumber"`
	Epoch                 uint32         `json:"epoch"`
	LogDigest             string         `json:"logDigest"`
	LogID                 uint64         `json:"logId"`
	BodySize              uint32         `json:"bodySize"`
	Timestamp             int64          `json:"timestamp"`
	TransactionHash       string         `json:"transactionHash"`
	Body                  map[string]any `json:"body"`
}

// ParseBobTimestamp parses a bob timestamp string into Unix seconds.
// Bob sends 2-digit year format ("26-01-28 21:37:48"), with fallbacks for
// 4-digit year ("2006-01-02 15:04:05") and RFC3339.
func ParseBobTimestamp(ts string) (int64, error) {
	// Bob sends 2-digit year: "26-01-28 21:37:48"
	t, err := time.Parse("06-01-02 15:04:05", ts)
	if err == nil {
		return t.Unix(), nil
	}
	// 4-digit year fallback: "2024-06-15 14:30:00"
	t, err = time.Parse("2006-01-02 15:04:05", ts)
	if err == nil {
		return t.Unix(), nil
	}
	// RFC3339 fallback (used in tests)
	t, err = time.Parse(time.RFC3339, ts)
	if err != nil {
		return 0, fmt.Errorf("failed to parse timestamp %q: %w", ts, err)
	}
	return t.Unix(), nil
}

// TransformEventBody converts a typed bob event body into the Kafka body format
// with the appropriate field renames per event type.
func TransformEventBody(eventType uint32, body interface{}) (map[string]any, error) {
	if body == nil {
		return nil, nil
	}

	switch b := body.(type) {
	case *bob.QuTransferBody:
		return map[string]any{
			"source":      b.From,
			"destination": b.To,
			"amount":      b.Amount,
		}, nil

	case *bob.AssetIssuanceBody:
		return map[string]any{
			"assetIssuer":           b.IssuerPublicKey,
			"numberOfShares":        b.NumberOfShares,
			"managingContractIndex": b.ManagingContractIndex,
			"assetName":             b.Name,
			"numberOfDecimalPlaces": b.NumberOfDecimalPlaces,
			"unitOfMeasurement":     b.UnitOfMeasurement,
		}, nil

	case *bob.AssetOwnershipChangeBody:
		return map[string]any{
			"source":         b.SourcePublicKey,
			"destination":    b.DestinationPublicKey,
			"assetName":      b.AssetName,
			"numberOfShares": b.NumberOfShares,
		}, nil

	case *bob.AssetPossessionChangeBody:
		return map[string]any{
			"source":         b.SourcePublicKey,
			"destination":    b.DestinationPublicKey,
			"assetName":      b.AssetName,
			"numberOfShares": b.NumberOfShares,
		}, nil

	case *bob.BurningBody:
		return map[string]any{
			"source":                 b.PublicKey,
			"amount":                 b.Amount,
			"contractIndexBurnedFor": b.ContractIndexBurnedFor,
		}, nil

	case *bob.ContractReserveDeductionBody:
		return map[string]any{
			"deductedAmount":  b.DeductedAmount,
			"remainingAmount": b.RemainingAmount,
			"contractIndex":   b.ContractIndex,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported event body type: %T", body)
	}
}

// BuildEventMessage assembles a full Kafka EventMessage from bob message components.
func BuildEventMessage(logMsg *bob.LogMessage, payload *bob.LogPayload, parsedBody interface{}, indexInTick uint32) (*EventMessage, error) {
	ts, err := ParseBobTimestamp(payload.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	body, err := TransformEventBody(payload.Type, parsedBody)
	if err != nil {
		return nil, fmt.Errorf("failed to transform event body: %w", err)
	}

	return &EventMessage{
		Index:                 uint64(indexInTick),
		EmittingContractIndex: logMsg.SCIndex,
		Type:                  logMsg.LogType,
		TickNumber:            payload.Tick,
		Epoch:                 uint32(payload.Epoch),
		LogDigest:             payload.LogDigest,
		LogID:                 payload.LogID,
		BodySize:              payload.BodySize,
		Timestamp:             ts,
		TransactionHash:       payload.TxHash,
		Body:                  body,
	}, nil
}
