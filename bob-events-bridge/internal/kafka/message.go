package kafka

import (
	"fmt"

	"github.com/qubic/bob-events-bridge/internal/bob"
)

// EventMessage is the Kafka message payload
type EventMessage struct {
	Index           uint64         `json:"index"`
	Type            uint32         `json:"type"`
	TickNumber      uint32         `json:"tickNumber"`
	Epoch           uint32         `json:"epoch"`
	LogDigest       string         `json:"logDigest"`
	LogID           uint64         `json:"logId"`
	BodySize        uint32         `json:"bodySize"`
	Timestamp       uint64         `json:"timestamp"`
	TransactionHash string         `json:"transactionHash"`
	Body            map[string]any `json:"body"`
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
			"assetIssuer":    b.IssuerPublicKey,
			"assetName":      b.AssetName,
			"numberOfShares": b.NumberOfShares,
		}, nil

	case *bob.AssetPossessionChangeBody:
		return map[string]any{
			"source":         b.SourcePublicKey,
			"destination":    b.DestinationPublicKey,
			"assetIssuer":    b.IssuerPublicKey,
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

	case *bob.ContractMessageBody:
		return map[string]any{
			"scIndex":   b.SCIndex,
			"scLogType": b.SCLogType,
			"content":   b.Content,
		}, nil

	case *bob.HexBody:
		return map[string]any{
			"hex": b.Hex,
		}, nil

	case *bob.CustomMessageBody:
		return map[string]any{
			"customMessage": b.CustomMessage,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported event body type: %T", body)
	}
}

// BuildEventMessage assembles a full Kafka EventMessage from bob log payload components.
func BuildEventMessage(payload *bob.LogPayload, parsedBody interface{}, indexInTick uint32) (*EventMessage, error) {
	body, err := TransformEventBody(payload.Type, parsedBody)
	if err != nil {
		return nil, fmt.Errorf("failed to transform event body: %w", err)
	}

	return &EventMessage{
		Index:           uint64(indexInTick),
		Type:            payload.Type,
		TickNumber:      payload.Tick,
		Epoch:           uint32(payload.Epoch),
		LogDigest:       payload.LogDigest,
		LogID:           payload.LogID,
		BodySize:        payload.BodySize,
		Timestamp:       payload.Timestamp,
		TransactionHash: payload.TxHash,
		Body:            body,
	}, nil
}
