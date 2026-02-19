package kafka

import (
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

func TestTransformEventBody_UnsupportedType(t *testing.T) {
	body := struct{ Foo string }{Foo: "bar"}
	_, err := TransformEventBody(99, &body)
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

	msg, err := BuildEventMessage(0, payload, body, 3)
	require.NoError(t, err)

	assert.Equal(t, uint64(3), msg.Index)
	assert.Equal(t, uint32(0), msg.EmittingContractIndex)
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

	msg, err := BuildEventMessage(0, payload, nil, 0)
	require.NoError(t, err)
	assert.Nil(t, msg.Body)
}
