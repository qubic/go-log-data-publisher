package kafka

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	eventsbridge "github.com/qubic/bob-events-bridge/api/events-bridge/v1"
	"github.com/qubic/bob-events-bridge/internal/bob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestTransformEventBodyMap_QuTransfer(t *testing.T) {
	body := map[string]any{
		"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"amount": float64(1000),
	}

	result, err := TransformEventBodyMap(bob.LogTypeQuTransfer, body)
	require.NoError(t, err)

	expected := map[string]any{
		"source":      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"destination": "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"amount":      float64(1000),
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBodyMap_AssetIssuance(t *testing.T) {
	body := map[string]any{
		"issuerPublicKey":       "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"numberOfShares":        float64(100000),
		"managingContractIndex": float64(5),
		"name":                  "QX",
		"numberOfDecimalPlaces": float64(0),
		"unitOfMeasurement":     "shares",
	}

	result, err := TransformEventBodyMap(bob.LogTypeAssetIssuance, body)
	require.NoError(t, err)

	expected := map[string]any{
		"assetIssuer":           "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetName":             "QX",
		"numberOfShares":        float64(100000),
		"managingContractIndex": float64(5),
		"numberOfDecimalPlaces": float64(0),
		"unitOfMeasurement":     "shares",
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBodyMap_AssetOwnershipChange(t *testing.T) {
	body := map[string]any{
		"sourcePublicKey":      "SRCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"destinationPublicKey": "DSTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"issuerPublicKey":      "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetName":            "QX",
		"numberOfShares":       float64(500),
	}

	result, err := TransformEventBodyMap(bob.LogTypeAssetOwnershipChange, body)
	require.NoError(t, err)

	expected := map[string]any{
		"source":         "SRCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"destination":    "DSTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetIssuer":    "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetName":      "QX",
		"numberOfShares": float64(500),
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBodyMap_AssetPossessionChange(t *testing.T) {
	body := map[string]any{
		"sourcePublicKey":      "SRCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"destinationPublicKey": "DSTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"issuerPublicKey":      "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetName":            "CFB",
		"numberOfShares":       float64(200),
	}

	result, err := TransformEventBodyMap(bob.LogTypeAssetPossessionChange, body)
	require.NoError(t, err)

	expected := map[string]any{
		"source":         "SRCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"destination":    "DSTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetIssuer":    "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetName":      "CFB",
		"numberOfShares": float64(200),
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBodyMap_Burning(t *testing.T) {
	body := map[string]any{
		"publicKey":              "BURNAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"amount":                 float64(999),
		"contractIndexBurnedFor": float64(7),
	}

	result, err := TransformEventBodyMap(bob.LogTypeBurning, body)
	require.NoError(t, err)

	expected := map[string]any{
		"source":                 "BURNAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"amount":                 float64(999),
		"contractIndexBurnedFor": float64(7),
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBodyMap_AssetOwnershipManagingContractChange(t *testing.T) {
	body := map[string]any{
		"ownershipPublicKey":       "OWNERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"issuerPublicKey":          "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"sourceContractIndex":      float64(1),
		"destinationContractIndex": float64(2),
		"numberOfShares":           float64(500),
		"assetName":                "QX",
	}

	result, err := TransformEventBodyMap(bob.LogTypeAssetOwnershipManagingContractChange, body)
	require.NoError(t, err)

	expected := map[string]any{
		"owner":                    "OWNERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetIssuer":              "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"sourceContractIndex":      float64(1),
		"destinationContractIndex": float64(2),
		"numberOfShares":           float64(500),
		"assetName":                "QX",
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBodyMap_AssetPossessionManagingContractChange(t *testing.T) {
	body := map[string]any{
		"possessionPublicKey":      "POSSESAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"ownershipPublicKey":       "OWNERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"issuerPublicKey":          "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"sourceContractIndex":      float64(3),
		"destinationContractIndex": float64(4),
		"numberOfShares":           float64(750),
		"assetName":                "CFB",
	}

	result, err := TransformEventBodyMap(bob.LogTypeAssetPossessionManagingContractChange, body)
	require.NoError(t, err)

	expected := map[string]any{
		"possessor":                "POSSESAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"owner":                    "OWNERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetIssuer":              "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"sourceContractIndex":      float64(3),
		"destinationContractIndex": float64(4),
		"numberOfShares":           float64(750),
		"assetName":                "CFB",
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBodyMap_NoRenamePassthrough(t *testing.T) {
	cases := []struct {
		name      string
		eventType uint32
		body      map[string]any
	}{
		{
			name:      "ContractMessage",
			eventType: bob.LogTypeContractErrorMessage,
			body: map[string]any{
				"scIndex":   float64(5),
				"scLogType": float64(42),
				"content":   "boom",
			},
		},
		{
			name:      "ContractReserveDeduction",
			eventType: bob.LogTypeContractReserveDeduction,
			body: map[string]any{
				"deductedAmount":  float64(5000),
				"remainingAmount": float64(95000),
				"contractIndex":   float64(3),
			},
		},
		{
			name:      "DustBurningHex",
			eventType: bob.LogTypeDustBurning,
			body:      map[string]any{"hex": "deadbeef"},
		},
		{
			name:      "OracleQueryStatusChange",
			eventType: bob.LogTypeOracleQueryStatusChange,
			body: map[string]any{
				"queryingEntity": "QQQQQQ",
				"queryId":        float64(123),
				"interfaceIndex": float64(1),
				"type":           float64(2),
				"status":         "pending",
			},
		},
		{
			name:      "CustomMessage",
			eventType: bob.LogTypeCustomMessage,
			body:      map[string]any{"customMessage": "12345"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := TransformEventBodyMap(tc.eventType, tc.body)
			require.NoError(t, err)
			if diff := cmp.Diff(tc.body, result); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTransformEventBodyMap_NilBody(t *testing.T) {
	result, err := TransformEventBodyMap(bob.LogTypeQuTransfer, nil)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestTransformEventBodyMap_UnsupportedType(t *testing.T) {
	_, err := TransformEventBodyMap(999, map[string]any{"foo": "bar"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported event type")
}

func TestBuildEventMessageFromStored(t *testing.T) {
	bodyStruct, err := structpb.NewStruct(map[string]any{
		"from":   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"to":     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"amount": float64(1000),
	})
	require.NoError(t, err)

	event := &eventsbridge.Event{
		LogId:          42,
		Tick:           22000001,
		Epoch:          145,
		EventType:      bob.LogTypeQuTransfer,
		TxHash:         "TXHASH",
		Timestamp:      1718461800,
		Body:           bodyStruct,
		IndexInTick:    3,
		LogDigest:      "abc123",
		LastLogForTick: true,
	}

	msg, err := BuildEventMessageFromStored(event)
	require.NoError(t, err)

	assert.Equal(t, uint64(3), msg.Index)
	assert.Equal(t, bob.LogTypeQuTransfer, msg.Type)
	assert.Equal(t, uint32(22000001), msg.TickNumber)
	assert.Equal(t, uint32(145), msg.Epoch)
	assert.Equal(t, "abc123", msg.LogDigest)
	assert.Equal(t, uint64(42), msg.LogID)
	assert.Equal(t, uint32(0), msg.BodySize, "BodySize must be 0 on replay (not stored in proto)")
	assert.Equal(t, uint64(1718461800), msg.Timestamp)
	assert.Equal(t, "TXHASH", msg.TransactionHash)
	assert.True(t, msg.LastLogForTick)

	require.NotNil(t, msg.Body)
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", msg.Body["source"])
	assert.Equal(t, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", msg.Body["destination"])
	assert.Equal(t, float64(1000), msg.Body["amount"])
	assert.NotContains(t, msg.Body, "from", "from→source rename not applied")
	assert.NotContains(t, msg.Body, "to", "to→destination rename not applied")
}

func TestBuildEventMessageFromStored_NilBody(t *testing.T) {
	event := &eventsbridge.Event{
		LogId:     1,
		Tick:      100,
		Epoch:     1,
		EventType: bob.LogTypeQuTransfer,
		Body:      nil,
	}

	msg, err := BuildEventMessageFromStored(event)
	require.NoError(t, err)
	assert.Nil(t, msg.Body)
	assert.Equal(t, uint32(0), msg.BodySize)
}

func TestBuildEventMessageFromStored_UnsupportedTypePropagatesError(t *testing.T) {
	bodyStruct, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)

	event := &eventsbridge.Event{
		EventType: 999,
		Body:      bodyStruct,
	}

	_, err = BuildEventMessageFromStored(event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to transform stored event body")
}
