package bob

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseEventBody_QuTransfer(t *testing.T) {
	body := json.RawMessage(`{
		"from": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"to":   "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"amount": 1000
	}`)

	result, err := ParseEventBody(LogTypeQuTransfer, body)
	require.NoError(t, err)

	parsed, ok := result.(*QuTransferBody)
	require.True(t, ok)
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", parsed.From)
	assert.Equal(t, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", parsed.To)
	assert.Equal(t, int64(1000), parsed.Amount)
}

func TestParseEventBody_AssetIssuance(t *testing.T) {
	body := json.RawMessage(`{
		"issuerPublicKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"numberOfShares": 1000000,
		"managingContractIndex": 1,
		"name": "TEST",
		"numberOfDecimalPlaces": 2,
		"unitOfMeasurement": "units"
	}`)

	result, err := ParseEventBody(LogTypeAssetIssuance, body)
	require.NoError(t, err)

	parsed, ok := result.(*AssetIssuanceBody)
	require.True(t, ok)
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", parsed.IssuerPublicKey)
	assert.Equal(t, int64(1000000), parsed.NumberOfShares)
	assert.Equal(t, int64(1), parsed.ManagingContractIndex)
	assert.Equal(t, "TEST", parsed.Name)
	assert.Equal(t, 2, parsed.NumberOfDecimalPlaces)
	assert.Equal(t, "units", parsed.UnitOfMeasurement)
}

func TestParseEventBody_AssetOwnershipChange(t *testing.T) {
	body := json.RawMessage(`{
		"sourcePublicKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"destinationPublicKey": "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"assetName": "TEST",
		"numberOfShares": 500
	}`)

	result, err := ParseEventBody(LogTypeAssetOwnershipChange, body)
	require.NoError(t, err)

	parsed, ok := result.(*AssetOwnershipChangeBody)
	require.True(t, ok)
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", parsed.SourcePublicKey)
	assert.Equal(t, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", parsed.DestinationPublicKey)
	assert.Equal(t, "TEST", parsed.AssetName)
	assert.Equal(t, int64(500), parsed.NumberOfShares)
}

func TestParseEventBody_AssetPossessionChange(t *testing.T) {
	body := json.RawMessage(`{
		"sourcePublicKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"destinationPublicKey": "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"assetName": "ASSET",
		"numberOfShares": 250
	}`)

	result, err := ParseEventBody(LogTypeAssetPossessionChange, body)
	require.NoError(t, err)

	parsed, ok := result.(*AssetPossessionChangeBody)
	require.True(t, ok)
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", parsed.SourcePublicKey)
	assert.Equal(t, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", parsed.DestinationPublicKey)
	assert.Equal(t, "ASSET", parsed.AssetName)
	assert.Equal(t, int64(250), parsed.NumberOfShares)
}

func TestParseEventBody_Burning(t *testing.T) {
	body := json.RawMessage(`{
		"publicKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"amount": 5000,
		"contractIndexBurnedFor": 7
	}`)

	result, err := ParseEventBody(LogTypeBurning, body)
	require.NoError(t, err)

	parsed, ok := result.(*BurningBody)
	require.True(t, ok)
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", parsed.PublicKey)
	assert.Equal(t, int64(5000), parsed.Amount)
	assert.Equal(t, uint32(7), parsed.ContractIndexBurnedFor)
}

func TestParseEventBody_ContractReserveDeduction(t *testing.T) {
	body := json.RawMessage(`{
		"deductedAmount": 100,
		"remainingAmount": 9900,
		"contractIndex": 3
	}`)

	result, err := ParseEventBody(LogTypeContractReserveDeduction, body)
	require.NoError(t, err)

	parsed, ok := result.(*ContractReserveDeductionBody)
	require.True(t, ok)
	assert.Equal(t, uint64(100), parsed.DeductedAmount)
	assert.Equal(t, int64(9900), parsed.RemainingAmount)
	assert.Equal(t, uint32(3), parsed.ContractIndex)
}

func TestParseEventBody_UnknownLogType(t *testing.T) {
	body := json.RawMessage(`{"foo": "bar"}`)

	result, err := ParseEventBody(999, body)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown log type: 999")
}

func TestParseEventBody_MalformedBody(t *testing.T) {
	body := json.RawMessage(`{invalid json`)

	result, err := ParseEventBody(LogTypeQuTransfer, body)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal body for log type 0")
}

func TestParseEventBody_EmptyBody(t *testing.T) {
	result, err := ParseEventBody(LogTypeQuTransfer, nil)
	assert.NoError(t, err)
	assert.Nil(t, result)

	result, err = ParseEventBody(LogTypeQuTransfer, json.RawMessage{})
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestEventBodyToMap(t *testing.T) {
	body := &QuTransferBody{
		From:   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		To:     "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		Amount: 1000,
	}

	result, err := EventBodyToMap(body)
	require.NoError(t, err)
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", result["from"])
	assert.Equal(t, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", result["to"])
	assert.Equal(t, float64(1000), result["amount"])
}
