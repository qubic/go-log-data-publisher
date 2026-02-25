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
		"issuerPublicKey": "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetName": "TEST",
		"numberOfShares": 500
	}`)

	result, err := ParseEventBody(LogTypeAssetOwnershipChange, body)
	require.NoError(t, err)

	parsed, ok := result.(*AssetOwnershipChangeBody)
	require.True(t, ok)
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", parsed.SourcePublicKey)
	assert.Equal(t, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", parsed.DestinationPublicKey)
	assert.Equal(t, "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", parsed.IssuerPublicKey)
	assert.Equal(t, "TEST", parsed.AssetName)
	assert.Equal(t, int64(500), parsed.NumberOfShares)
}

func TestParseEventBody_AssetPossessionChange(t *testing.T) {
	body := json.RawMessage(`{
		"sourcePublicKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"destinationPublicKey": "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
		"issuerPublicKey": "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"assetName": "ASSET",
		"numberOfShares": 250
	}`)

	result, err := ParseEventBody(LogTypeAssetPossessionChange, body)
	require.NoError(t, err)

	parsed, ok := result.(*AssetPossessionChangeBody)
	require.True(t, ok)
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", parsed.SourcePublicKey)
	assert.Equal(t, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", parsed.DestinationPublicKey)
	assert.Equal(t, "ISSUERAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", parsed.IssuerPublicKey)
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

func TestParseEventBody_ContractMessage(t *testing.T) {
	body := json.RawMessage(`{
		"scIndex": 5,
		"scLogType": 42,
		"content": "error: something failed"
	}`)

	// Test for all contract message types (4, 5, 6, 7)
	for _, logType := range []uint32{LogTypeContractErrorMessage, LogTypeContractWarningMessage,
		LogTypeContractInformationMessage, LogTypeContractDebugMessage} {
		result, err := ParseEventBody(logType, body)
		require.NoError(t, err, "logType %d", logType)

		parsed, ok := result.(*ContractMessageBody)
		require.True(t, ok, "logType %d", logType)
		assert.Equal(t, uint32(5), parsed.SCIndex)
		assert.Equal(t, uint32(42), parsed.SCLogType)
		assert.Equal(t, "error: something failed", parsed.Content)
	}
}

func TestParseEventBody_HexBody(t *testing.T) {
	body := json.RawMessage(`{
		"hex": "deadbeef0123456789abcdef"
	}`)

	// Test for all hex types (9, 10, 11, 12)
	for _, logType := range []uint32{LogTypeDustBurning, LogTypeSpectrumStats,
		LogTypeAssetOwnershipManagingContractChange, LogTypeAssetPossessionManagingContractChange} {
		result, err := ParseEventBody(logType, body)
		require.NoError(t, err, "logType %d", logType)

		parsed, ok := result.(*HexBody)
		require.True(t, ok, "logType %d", logType)
		assert.Equal(t, "deadbeef0123456789abcdef", parsed.Hex)
	}
}

func TestParseEventBody_CustomMessage(t *testing.T) {
	body := json.RawMessage(`{
		"customMessage": "12345"
	}`)

	result, err := ParseEventBody(LogTypeCustomMessage, body)
	require.NoError(t, err)

	parsed, ok := result.(*CustomMessageBody)
	require.True(t, ok)
	assert.Equal(t, "12345", parsed.CustomMessage)
}

func TestParseEventBody_UnknownLogType(t *testing.T) {
	body := json.RawMessage(`{"hex": "deadbeef"}`)

	result, err := ParseEventBody(999, body)
	require.NoError(t, err)

	parsed, ok := result.(*HexBody)
	require.True(t, ok, "unknown log type should return *HexBody")
	assert.Equal(t, "deadbeef", parsed.Hex)
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
