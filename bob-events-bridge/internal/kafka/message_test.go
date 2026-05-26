package kafka

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/qubic/bob-events-bridge/internal/bob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransformEventBody_QuTransfer(t *testing.T) {
	body := &bob.QuTransferBody{
		From:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		To:     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		Amount: 1000,
	}

	result, err := TransformEventBody(bob.LogTypeQuTransfer, body)
	require.NoError(t, err)

	expected := map[string]any{
		"source":      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"destination": "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"amount":      int64(1000),
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBody_AssetIssuance(t *testing.T) {
	body := &bob.AssetIssuanceBody{
		IssuerPublicKey:       "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		NumberOfShares:        100000,
		ManagingContractIndex: 5,
		Name:                  "QX",
		NumberOfDecimalPlaces: 0,
		UnitOfMeasurement:     "shares",
	}

	result, err := TransformEventBody(bob.LogTypeAssetIssuance, body)
	require.NoError(t, err)

	expected := map[string]any{
		"assetIssuer":           "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetName":             "QX",
		"numberOfShares":        int64(100000),
		"managingContractIndex": int64(5),
		"numberOfDecimalPlaces": 0,
		"unitOfMeasurement":     "shares",
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBody_AssetOwnershipChange(t *testing.T) {
	body := &bob.AssetOwnershipChangeBody{
		SourcePublicKey:      "SRCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		DestinationPublicKey: "DSTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		IssuerPublicKey:      "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		AssetName:            "QX",
		NumberOfShares:       500,
	}

	result, err := TransformEventBody(bob.LogTypeAssetOwnershipChange, body)
	require.NoError(t, err)

	expected := map[string]any{
		"source":         "SRCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"destination":    "DSTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetIssuer":    "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetName":      "QX",
		"numberOfShares": int64(500),
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBody_AssetPossessionChange(t *testing.T) {
	body := &bob.AssetPossessionChangeBody{
		SourcePublicKey:      "SRCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		DestinationPublicKey: "DSTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		IssuerPublicKey:      "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		AssetName:            "CFB",
		NumberOfShares:       200,
	}

	result, err := TransformEventBody(bob.LogTypeAssetPossessionChange, body)
	require.NoError(t, err)

	expected := map[string]any{
		"source":         "SRCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"destination":    "DSTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetIssuer":    "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetName":      "CFB",
		"numberOfShares": int64(200),
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBody_Burning(t *testing.T) {
	body := &bob.BurningBody{
		PublicKey:              "BURNAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		Amount:                 999,
		ContractIndexBurnedFor: 7,
	}

	result, err := TransformEventBody(bob.LogTypeBurning, body)
	require.NoError(t, err)

	expected := map[string]any{
		"source":                 "BURNAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"amount":                 int64(999),
		"contractIndexBurnedFor": uint32(7),
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBody_ContractReserveDeduction(t *testing.T) {
	body := &bob.ContractReserveDeductionBody{
		DeductedAmount:  5000,
		RemainingAmount: 95000,
		ContractIndex:   3,
	}

	result, err := TransformEventBody(bob.LogTypeContractReserveDeduction, body)
	require.NoError(t, err)

	expected := map[string]any{
		"deductedAmount":  uint64(5000),
		"remainingAmount": int64(95000),
		"contractIndex":   uint32(3),
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBody_NilBody(t *testing.T) {
	result, err := TransformEventBody(bob.LogTypeQuTransfer, nil)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestTransformEventBody_ContractMessage(t *testing.T) {
	body := &bob.ContractMessageBody{
		SCIndex:   5,
		SCLogType: 42,
		Content:   "error: something failed",
	}

	result, err := TransformEventBody(bob.LogTypeContractErrorMessage, body)
	require.NoError(t, err)

	expected := map[string]any{
		"scIndex":   uint32(5),
		"scLogType": uint32(42),
		"content":   "error: something failed",
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBody_AssetOwnershipManagingContractChange(t *testing.T) {
	body := &bob.AssetOwnershipManagingContractChangeBody{
		OwnershipPublicKey:       "OWNERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		IssuerPublicKey:          "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		SourceContractIndex:      1,
		DestinationContractIndex: 2,
		NumberOfShares:           500,
		AssetName:                "QX",
	}

	result, err := TransformEventBody(bob.LogTypeAssetOwnershipManagingContractChange, body)
	require.NoError(t, err)

	expected := map[string]any{
		"owner":                    "OWNERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetIssuer":              "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"sourceContractIndex":      uint32(1),
		"destinationContractIndex": uint32(2),
		"numberOfShares":           int64(500),
		"assetName":                "QX",
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBody_AssetPossessionManagingContractChange(t *testing.T) {
	body := &bob.AssetPossessionManagingContractChangeBody{
		PossessionPublicKey:      "POSSESAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		OwnershipPublicKey:       "OWNERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		IssuerPublicKey:          "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		SourceContractIndex:      3,
		DestinationContractIndex: 4,
		NumberOfShares:           750,
		AssetName:                "CFB",
	}

	result, err := TransformEventBody(bob.LogTypeAssetPossessionManagingContractChange, body)
	require.NoError(t, err)

	expected := map[string]any{
		"possessor":                "POSSESAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"owner":                    "OWNERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetIssuer":              "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"sourceContractIndex":      uint32(3),
		"destinationContractIndex": uint32(4),
		"numberOfShares":           int64(750),
		"assetName":                "CFB",
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBody_OracleQueryStatusChange(t *testing.T) {
	body := &bob.OracleQueryStatusChangeBody{
		QueryingEntity: "QUERRYINGAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		QueryID:        12345321,
		InterfaceIndex: 1,
		Type:           2,
		Status:         "pending",
	}

	result, err := TransformEventBody(bob.LogTypeOracleQueryStatusChange, body)
	require.NoError(t, err)

	expected := map[string]any{
		"queryingEntity": "QUERRYINGAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"queryId":        int64(12345321),
		"interfaceIndex": uint32(1),
		"type":           uint32(2),
		"status":         "pending",
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBody_OracleSubscriberLogMessage(t *testing.T) {
	body := &bob.OracleSubscriberLogMessageBody{
		SubscriptionID:        1234321,
		InterfaceIndex:        2,
		ContractIndex:         3,
		PeriodInMilliseconds:  300000,
		FirstQueryDateAndTime: "2025-01-15T12:00:00",
	}

	result, err := TransformEventBody(bob.LogTypeOracleSubscriberLogMessage, body)
	require.NoError(t, err)

	expected := map[string]any{
		"subscriptionId":        int32(1234321),
		"interfaceIndex":        uint32(2),
		"contractIndex":         uint32(3),
		"periodInMilliseconds":  uint32(300000),
		"firstQueryDateAndTime": "2025-01-15T12:00:00",
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBody_HexBody(t *testing.T) {
	body := &bob.HexBody{
		Hex: "deadbeef0123456789abcdef",
	}

	result, err := TransformEventBody(bob.LogTypeDustBurning, body)
	require.NoError(t, err)

	expected := map[string]any{
		"hex": "deadbeef0123456789abcdef",
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBody_CustomMessage(t *testing.T) {
	body := &bob.CustomMessageBody{
		CustomMessage: "12345",
	}

	result, err := TransformEventBody(bob.LogTypeCustomMessage, body)
	require.NoError(t, err)

	expected := map[string]any{
		"customMessage": "12345",
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBody_UnknownLogTypeHexBody(t *testing.T) {
	body := &bob.HexBody{
		Hex: "deadbeef0123456789abcdef",
	}

	result, err := TransformEventBody(999, body)
	require.NoError(t, err)

	expected := map[string]any{
		"hex": "deadbeef0123456789abcdef",
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformEventBody_UnsupportedType(t *testing.T) {
	body := struct{ Foo string }{Foo: "bar"}
	_, err := TransformEventBody(99, body)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported event body type")
}

func TestBuildEventMessage(t *testing.T) {
	payload := &bob.LogPayload{
		OK:        true,
		Epoch:     145,
		Tick:      22000001,
		Type:      0,
		LogID:     42,
		LogDigest: "abc123",
		BodySize:  64,
		Timestamp: uint64(1718461800),
		TxHash:    "TXHASHAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
	}

	body := &bob.QuTransferBody{
		From:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		To:     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		Amount: 1000,
	}

	msg, err := BuildEventMessage(body, 3, false, payload, false)
	require.NoError(t, err)

	assert.Equal(t, uint64(3), msg.Index)
	assert.Equal(t, uint32(0), msg.Type)
	assert.Equal(t, uint32(22000001), msg.TickNumber)
	assert.Equal(t, uint32(145), msg.Epoch)
	assert.Equal(t, "abc123", msg.LogDigest)
	assert.Equal(t, uint64(42), msg.LogID)
	assert.Equal(t, uint32(64), msg.BodySize)
	assert.Equal(t, uint64(1718461800), msg.Timestamp)
	assert.Equal(t, "TXHASHAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", msg.TransactionHash)
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", msg.Body["source"])
	assert.Equal(t, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", msg.Body["destination"])
	assert.Equal(t, int64(1000), msg.Body["amount"])
	assert.False(t, msg.LastLogForTick)
}

func TestBuildEventMessage_NilBody(t *testing.T) {
	payload := &bob.LogPayload{
		OK:        true,
		Epoch:     145,
		Tick:      22000001,
		Type:      0,
		LogID:     42,
		LogDigest: "abc123",
		Timestamp: uint64(1718461800),
		TxHash:    "TXHASH",
	}

	msg, err := BuildEventMessage(nil, 0, false, payload, false)
	require.NoError(t, err)
	assert.Nil(t, msg.Body)
	assert.False(t, msg.LastLogForTick)
}

func TestBuildEventMessage_LastLogForTick(t *testing.T) {
	payload := &bob.LogPayload{
		OK:        true,
		Epoch:     145,
		Tick:      22000001,
		Type:      0,
		LogID:     99,
		LogDigest: "def456",
		BodySize:  64,
		Timestamp: uint64(1718461800),
		TxHash:    "TXHASH",
	}

	body := &bob.QuTransferBody{
		From:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		To:     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		Amount: 500,
	}

	msg, err := BuildEventMessage(body, 5, true, payload, false)
	require.NoError(t, err)
	assert.True(t, msg.LastLogForTick)
}

func TestBuildEventMessage_IsDividend(t *testing.T) {
	msg, err := BuildEventMessage(&bob.QuTransferBody{}, 5, false, &bob.LogPayload{}, true)
	require.NoError(t, err)
	assert.True(t, msg.Dividend)
}

func TestBuildEventMessage_IsNotDividend(t *testing.T) {
	msg, err := BuildEventMessage(&bob.QuTransferBody{}, 5, false, &bob.LogPayload{}, false)
	require.NoError(t, err)
	assert.False(t, msg.Dividend)
}

func TestEventMessage_JSON_OmitsDividendWhenFalse(t *testing.T) {
	msg := EventMessage{}

	data, err := json.Marshal(msg)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "dividend")
}

func TestEventMessage_JSON_IncludesDividendWhenTrue(t *testing.T) {
	msg := EventMessage{Dividend: true}

	data, err := json.Marshal(msg)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"dividend":true`)
}

func TestEventMessage_JSON_OmitsLastLogForTickWhenFalse(t *testing.T) {
	msg := EventMessage{
		Index:      1,
		Type:       0,
		TickNumber: 100,
	}

	data, err := json.Marshal(msg)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "lastLogForTick")
}

func TestEventMessage_JSON_IncludesLastLogForTickWhenTrue(t *testing.T) {
	msg := EventMessage{
		Index:          1,
		Type:           0,
		TickNumber:     100,
		LastLogForTick: true,
	}

	data, err := json.Marshal(msg)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"lastLogForTick":true`)
}
