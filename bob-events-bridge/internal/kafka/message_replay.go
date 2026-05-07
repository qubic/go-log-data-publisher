package kafka

import (
	"fmt"

	eventsbridge "github.com/qubic/bob-events-bridge/api/events-bridge/v1"
	"github.com/qubic/bob-events-bridge/internal/bob"
)

// TransformEventBodyMap mirrors TransformEventBody but operates on the
// bob-format map[string]any read back from a stored proto Event (Body.AsMap()),
// instead of a typed bob struct. The rename rules MUST stay in sync with
// TransformEventBody — if you change one, change the other.
func TransformEventBodyMap(eventType uint32, body map[string]any) (map[string]any, error) {
	if body == nil {
		return nil, nil
	}

	switch eventType {
	case bob.LogTypeQuTransfer:
		return map[string]any{
			"source":      body["from"],
			"destination": body["to"],
			"amount":      body["amount"],
		}, nil

	case bob.LogTypeAssetIssuance:
		return map[string]any{
			"assetIssuer":           body["issuerPublicKey"],
			"numberOfShares":        body["numberOfShares"],
			"managingContractIndex": body["managingContractIndex"],
			"assetName":             body["name"],
			"numberOfDecimalPlaces": body["numberOfDecimalPlaces"],
			"unitOfMeasurement":     body["unitOfMeasurement"],
		}, nil

	case bob.LogTypeAssetOwnershipChange, bob.LogTypeAssetPossessionChange:
		return map[string]any{
			"source":         body["sourcePublicKey"],
			"destination":    body["destinationPublicKey"],
			"assetIssuer":    body["issuerPublicKey"],
			"assetName":      body["assetName"],
			"numberOfShares": body["numberOfShares"],
		}, nil

	case bob.LogTypeBurning:
		return map[string]any{
			"source":                 body["publicKey"],
			"amount":                 body["amount"],
			"contractIndexBurnedFor": body["contractIndexBurnedFor"],
		}, nil

	case bob.LogTypeAssetOwnershipManagingContractChange:
		return map[string]any{
			"owner":                    body["ownershipPublicKey"],
			"assetIssuer":              body["issuerPublicKey"],
			"sourceContractIndex":      body["sourceContractIndex"],
			"destinationContractIndex": body["destinationContractIndex"],
			"numberOfShares":           body["numberOfShares"],
			"assetName":                body["assetName"],
		}, nil

	case bob.LogTypeAssetPossessionManagingContractChange:
		return map[string]any{
			"possessor":                body["possessionPublicKey"],
			"owner":                    body["ownershipPublicKey"],
			"assetIssuer":              body["issuerPublicKey"],
			"sourceContractIndex":      body["sourceContractIndex"],
			"destinationContractIndex": body["destinationContractIndex"],
			"numberOfShares":           body["numberOfShares"],
			"assetName":                body["assetName"],
		}, nil

	// No-rename types: bob and kafka share the same field names.
	case bob.LogTypeContractErrorMessage,
		bob.LogTypeContractWarningMessage,
		bob.LogTypeContractInformationMessage,
		bob.LogTypeContractDebugMessage,
		bob.LogTypeDustBurning,
		bob.LogTypeSpectrumStats,
		bob.LogTypeContractReserveDeduction,
		bob.LogTypeOracleQueryStatusChange,
		bob.LogTypeOracleSubscriberLogMessage,
		bob.LogTypeCustomMessage:
		return body, nil

	default:
		return nil, fmt.Errorf("unsupported event type for replay transform: %d", eventType)
	}
}

// BuildEventMessageFromStored assembles a Kafka EventMessage from a stored proto Event.
// Used by the replay path; mirrors BuildEventMessage on the live path.
func BuildEventMessageFromStored(event *eventsbridge.Event) (*EventMessage, error) {
	var bodyMap map[string]any
	if event.Body != nil {
		bodyMap = event.Body.AsMap()
	}

	transformed, err := TransformEventBodyMap(event.EventType, bodyMap)
	if err != nil {
		return nil, fmt.Errorf("failed to transform stored event body: %w", err)
	}

	return &EventMessage{
		Index:      uint64(event.IndexInTick),
		Type:       event.EventType,
		TickNumber: event.Tick,
		Epoch:      event.Epoch,
		LogDigest:  event.LogDigest,
		LogID:      event.LogId,
		// Body size does not seem to be sent to elastic in the consumer, and we do not store it in the database.
		BodySize:        0,
		Timestamp:       event.Timestamp,
		TransactionHash: event.TxHash,
		Body:            transformed,
		LastLogForTick:  event.LastLogForTick,
	}, nil
}
