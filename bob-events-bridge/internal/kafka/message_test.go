package kafka

import (
	"encoding/json"
	"testing"

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
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", result["source"])
	assert.Equal(t, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", result["destination"])
	assert.Equal(t, int64(1000), result["amount"])
	// Verify old field names are NOT present
	assert.Nil(t, result["from"])
	assert.Nil(t, result["to"])
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
	assert.Equal(t, "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", result["assetIssuer"])
	assert.Equal(t, "QX", result["assetName"])
	assert.Equal(t, int64(100000), result["numberOfShares"])
	assert.Equal(t, int64(5), result["managingContractIndex"])
	assert.Equal(t, 0, result["numberOfDecimalPlaces"])
	assert.Equal(t, "shares", result["unitOfMeasurement"])
	// Verify old field names are NOT present
	assert.Nil(t, result["issuerPublicKey"])
	assert.Nil(t, result["name"])
}

func TestTransformEventBody_AssetOwnershipChange(t *testing.T) {
	body := &bob.AssetOwnershipChangeBody{
		SourcePublicKey:      "SRCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		DestinationPublicKey: "DSTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		AssetName:            "QX",
		NumberOfShares:       500,
	}

	result, err := TransformEventBody(bob.LogTypeAssetOwnershipChange, body)
	require.NoError(t, err)
	assert.Equal(t, "SRCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", result["source"])
	assert.Equal(t, "DSTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", result["destination"])
	assert.Equal(t, "QX", result["assetName"])
	assert.Equal(t, int64(500), result["numberOfShares"])
	assert.Nil(t, result["sourcePublicKey"])
	assert.Nil(t, result["destinationPublicKey"])
}

func TestTransformEventBody_AssetPossessionChange(t *testing.T) {
	body := &bob.AssetPossessionChangeBody{
		SourcePublicKey:      "SRCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		DestinationPublicKey: "DSTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		AssetName:            "CFB",
		NumberOfShares:       200,
	}

	result, err := TransformEventBody(bob.LogTypeAssetPossessionChange, body)
	require.NoError(t, err)
	assert.Equal(t, "SRCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", result["source"])
	assert.Equal(t, "DSTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", result["destination"])
	assert.Equal(t, "CFB", result["assetName"])
	assert.Equal(t, int64(200), result["numberOfShares"])
	assert.Nil(t, result["sourcePublicKey"])
	assert.Nil(t, result["destinationPublicKey"])
}

func TestTransformEventBody_Burning(t *testing.T) {
	body := &bob.BurningBody{
		PublicKey:              "BURNAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		Amount:                 999,
		ContractIndexBurnedFor: 7,
	}

	result, err := TransformEventBody(bob.LogTypeBurning, body)
	require.NoError(t, err)
	assert.Equal(t, "BURNAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", result["source"])
	assert.Equal(t, int64(999), result["amount"])
	assert.Equal(t, uint32(7), result["contractIndexBurnedFor"])
	assert.Nil(t, result["publicKey"])
}

func TestTransformEventBody_ContractReserveDeduction(t *testing.T) {
	body := &bob.ContractReserveDeductionBody{
		DeductedAmount:  5000,
		RemainingAmount: 95000,
		ContractIndex:   3,
	}

	result, err := TransformEventBody(bob.LogTypeContractReserveDeduction, body)
	require.NoError(t, err)
	assert.Equal(t, uint64(5000), result["deductedAmount"])
	assert.Equal(t, int64(95000), result["remainingAmount"])
	assert.Equal(t, uint32(3), result["contractIndex"])
}

func TestTransformEventBody_NilBody(t *testing.T) {
	result, err := TransformEventBody(bob.LogTypeQuTransfer, nil)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestTransformEventBody_UnsupportedType(t *testing.T) {
	body := struct{ Foo string }{Foo: "bar"}
	_, err := TransformEventBody(99, &body)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported event body type")
}

func TestParseBobTimestamp_TwoDigitYear(t *testing.T) {
	ts, err := ParseBobTimestamp("26-01-28 21:37:48")
	require.NoError(t, err)
	// 2026-01-28 21:37:48 UTC = 1769636268
	assert.Equal(t, int64(1769636268), ts)
}

func TestParseBobTimestamp_Valid(t *testing.T) {
	ts, err := ParseBobTimestamp("2024-06-15 14:30:00")
	require.NoError(t, err)
	assert.Equal(t, int64(1718461800), ts)
}

func TestParseBobTimestamp_RFC3339Fallback(t *testing.T) {
	ts, err := ParseBobTimestamp("2024-06-15T14:30:00Z")
	require.NoError(t, err)
	assert.Equal(t, int64(1718461800), ts)
}

func TestParseBobTimestamp_ZeroTimestamp(t *testing.T) {
	ts, err := ParseBobTimestamp("00-00-00 00:00:00")
	require.NoError(t, err)
	assert.Equal(t, int64(0), ts)
}

func TestParseBobTimestamp_EmptyString(t *testing.T) {
	ts, err := ParseBobTimestamp("")
	require.NoError(t, err)
	assert.Equal(t, int64(0), ts)
}

func TestParseBobTimestamp_Invalid(t *testing.T) {
	_, err := ParseBobTimestamp("not-a-timestamp")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse timestamp")
}

func TestBuildEventMessage(t *testing.T) {
	logMsg := &bob.LogMessage{
		Type:      bob.MessageTypeLog,
		SCIndex:   0,
		LogType:   0,
		IsCatchUp: false,
		Message:   json.RawMessage(`{}`),
	}

	payload := &bob.LogPayload{
		OK:        true,
		Epoch:     145,
		Tick:      22000001,
		Type:      0,
		LogID:     42,
		LogDigest: "abc123",
		BodySize:  64,
		Timestamp: "2024-06-15 14:30:00",
		TxHash:    "TXHASHAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
	}

	body := &bob.QuTransferBody{
		From:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		To:     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		Amount: 1000,
	}

	msg, err := BuildEventMessage(logMsg, payload, body, 3)
	require.NoError(t, err)

	assert.Equal(t, uint64(3), msg.Index)
	assert.Equal(t, uint32(0), msg.EmittingContractIndex)
	assert.Equal(t, uint32(0), msg.Type)
	assert.Equal(t, uint32(22000001), msg.TickNumber)
	assert.Equal(t, uint32(145), msg.Epoch)
	assert.Equal(t, "abc123", msg.LogDigest)
	assert.Equal(t, uint64(42), msg.LogID)
	assert.Equal(t, uint32(64), msg.BodySize)
	assert.Equal(t, int64(1718461800), msg.Timestamp)
	assert.Equal(t, "TXHASHAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", msg.TransactionHash)
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", msg.Body["source"])
	assert.Equal(t, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", msg.Body["destination"])
	assert.Equal(t, int64(1000), msg.Body["amount"])
}

func TestBuildEventMessage_NilBody(t *testing.T) {
	logMsg := &bob.LogMessage{
		Type:    bob.MessageTypeLog,
		SCIndex: 0,
		LogType: 0,
		Message: json.RawMessage(`{}`),
	}

	payload := &bob.LogPayload{
		OK:        true,
		Epoch:     145,
		Tick:      22000001,
		Type:      0,
		LogID:     42,
		LogDigest: "abc123",
		Timestamp: "2024-06-15 14:30:00",
		TxHash:    "TXHASH",
	}

	msg, err := BuildEventMessage(logMsg, payload, nil, 0)
	require.NoError(t, err)
	assert.Nil(t, msg.Body)
}

func TestBuildEventMessage_InvalidTimestamp(t *testing.T) {
	logMsg := &bob.LogMessage{
		Type:    bob.MessageTypeLog,
		Message: json.RawMessage(`{}`),
	}

	payload := &bob.LogPayload{
		OK:        true,
		Timestamp: "invalid",
	}

	_, err := BuildEventMessage(logMsg, payload, nil, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse timestamp")
}
