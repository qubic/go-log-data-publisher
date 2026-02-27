package domain

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogEvent_ToLogEventElastic_SpecialSystemTransactions(t *testing.T) {
	tests := []struct {
		name                string
		transactionHash     string
		expectedCategory    byte
		expectedTxHashEmpty bool
	}{
		{
			name:                "SC_INITIALIZE_TX with tick suffix",
			transactionHash:     "SC_INITIALIZE_TX_12345",
			expectedCategory:    1,
			expectedTxHashEmpty: true,
		},
		{
			name:                "SC_BEGIN_EPOCH_TX with tick suffix",
			transactionHash:     "SC_BEGIN_EPOCH_TX_67890",
			expectedCategory:    2,
			expectedTxHashEmpty: true,
		},
		{
			name:                "SC_BEGIN_TICK_TX with tick suffix",
			transactionHash:     "SC_BEGIN_TICK_TX_11111",
			expectedCategory:    3,
			expectedTxHashEmpty: true,
		},
		{
			name:                "SC_END_TICK_TX with tick suffix",
			transactionHash:     "SC_END_TICK_TX_22222",
			expectedCategory:    4,
			expectedTxHashEmpty: true,
		},
		{
			name:                "SC_END_EPOCH_TX with tick suffix",
			transactionHash:     "SC_END_EPOCH_TX_33333",
			expectedCategory:    5,
			expectedTxHashEmpty: true,
		},
		{
			name:                "SC_NOTIFICATION_TX with tick suffix",
			transactionHash:     "SC_NOTIFICATION_TX_44444",
			expectedCategory:    6,
			expectedTxHashEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logEvent := LogEvent{
				Epoch:           100,
				TickNumber:      200,
				Type:            0,
				LogId:           400,
				LogDigest:       "abc1234",
				TransactionHash: tt.transactionHash,
				Timestamp:       1234567890,
				Body: map[string]any{
					"source":      "TEST",
					"destination": "TEST",
					"amount":      float64(100),
				},
			}

			result, err := logEvent.ToLogEventElastic()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Verify categories are set correctly
			if result.Categories == nil || len(result.Categories) == 0 {
				t.Fatalf("expected Categories to be set, got nil or empty")
			}
			if result.Categories[0] != tt.expectedCategory {
				t.Errorf("expected Categories[0]=%d, got %d", tt.expectedCategory, result.Categories[0])
			}

			// Verify transaction hash is cleared
			if tt.expectedTxHashEmpty && result.TransactionHash != "" {
				t.Errorf("expected TransactionHash to be empty, got %s", result.TransactionHash)
			}
		})
	}
}

func TestLogEvent_ToLogEventElastic_SpecialTransactionJSON(t *testing.T) {
	tests := []struct {
		name             string
		transactionHash  string
		expectedCategory byte
	}{
		{
			name:             "SC_INITIALIZE_TX category 0 included in JSON",
			transactionHash:  "SC_INITIALIZE_TX_12345",
			expectedCategory: 1,
		},
		{
			name:             "SC_END_TICK_TX category 3 included in JSON",
			transactionHash:  "SC_END_TICK_TX_67890",
			expectedCategory: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logEvent := LogEvent{
				Epoch:           100,
				TickNumber:      200,
				Type:            0,
				LogId:           400,
				LogDigest:       "abc1234",
				TransactionHash: tt.transactionHash,
				Timestamp:       1234567890,
				Body: map[string]any{
					"source":      "TEST",
					"destination": "TEST",
					"amount":      float64(100),
				},
			}

			result, err := logEvent.ToLogEventElastic()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Serialize to JSON
			jsonData, err := json.Marshal(result)
			if err != nil {
				t.Fatalf("failed to marshal JSON: %v", err)
			}

			// Deserialize to map for checking
			var jsonMap map[string]any
			err = json.Unmarshal(jsonData, &jsonMap)
			if err != nil {
				t.Fatalf("failed to unmarshal JSON: %v", err)
			}

			// Verify categories is present in JSON
			categoriesValue, exists := jsonMap["categories"]
			if !exists {
				t.Errorf("expected 'categories' to be present in JSON, but it was missing")
			} else {
				// JSON arrays are []interface{}
				if categoriesArray, ok := categoriesValue.([]interface{}); !ok {
					t.Errorf("expected categories to be an array, got %T", categoriesValue)
				} else if len(categoriesArray) == 0 {
					t.Errorf("expected categories array to have at least one element")
				} else if categoryFloat, ok := categoriesArray[0].(float64); !ok {
					t.Errorf("expected categories[0] to be a number, got %T", categoriesArray[0])
				} else if byte(categoryFloat) != tt.expectedCategory {
					t.Errorf("expected categories[0]=%d in JSON, got %f", tt.expectedCategory, categoryFloat)
				}
			}

			// Verify transactionHash is NOT present in JSON (omitted because empty)
			if _, exists := jsonMap["transactionHash"]; exists {
				t.Errorf("expected 'transactionHash' to be omitted from JSON for special transactions, but it was present with value: %v", jsonMap["transactionHash"])
			}
		})
	}
}

func TestLogEvent_ToLogEventElastic_UnknownSpecialTransaction(t *testing.T) {
	logEvent := LogEvent{
		Epoch:           100,
		TickNumber:      200,
		LogId:           400,
		Timestamp:       500,
		LogDigest:       "foo",
		Type:            1,
		TransactionHash: "SC_UNKNOWN_TX_12345",
		Body:            map[string]any{},
	}

	_, err := logEvent.ToLogEventElastic()
	if err == nil {
		t.Fatal("expected error for unknown special transaction, got nil")
	}
	expectedMsg := "SC_UNKNOWN_TX"
	if !strings.Contains(err.Error(), expectedMsg) {
		t.Errorf("expected error message to contain '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestLogEvent_ToLogEventElastic_MalformedSpecialTransaction(t *testing.T) {
	tests := []struct {
		name            string
		transactionHash string
		expectedErrMsg  string
	}{
		{
			name:            "SC_ prefix but missing underscore after",
			transactionHash: "SC_ENDTICK",
			expectedErrMsg:  "unexpected special event log type [SC_ENDTICK]",
		},
		{
			name:            "SC_ only",
			transactionHash: "SC_",
			expectedErrMsg:  "unexpected special event log type [SC_]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logEvent := LogEvent{
				Epoch:           100,
				TickNumber:      200,
				Type:            1,
				LogId:           400,
				Timestamp:       500,
				LogDigest:       "foo",
				TransactionHash: tt.transactionHash,
				Body:            map[string]any{},
			}

			_, err := logEvent.ToLogEventElastic()
			if err == nil {
				t.Fatal("expected error for malformed special transaction, got nil")
			}
			if !strings.Contains(err.Error(), tt.expectedErrMsg) {
				t.Errorf("expected error message to contain '%s', got '%s'", tt.expectedErrMsg, err.Error())
			}
		})
	}
}

func TestLogEvent_ToLogEventElastic_QuTransfer_Success(t *testing.T) {
	le := LogEvent{
		Epoch:           100,
		TickNumber:      200,
		Timestamp:       1234567890,
		TransactionHash: validTxHash,
		LogId:           300,
		LogDigest:       "digest",
		Type:            0,
		Body: map[string]any{
			"source":      "source-identity",
			"destination": "dest-identity",
			"amount":      1000.0,
		},
	}

	lee, err := le.ToLogEventElastic()
	require.NoError(t, err)

	assert.Equal(t, "source-identity", lee.Source)
	assert.Equal(t, "dest-identity", lee.Destination)
	assert.Equal(t, 1000, int(*lee.Amount))
}

func TestLogEvent_ToLogEventElastic_QuTransfer_Error(t *testing.T) {
	le := LogEvent{
		Epoch:           100,
		TickNumber:      200,
		Timestamp:       1234567890,
		TransactionHash: validTxHash,
		LogId:           300,
		LogDigest:       "digest",
		Type:            0,
		Body: map[string]any{
			"destination": "dest-identity",
			"amount":      1000.0,
		},
	}

	_, err := le.ToLogEventElastic()
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing or invalid source")
}

func TestLogEvent_ToLogEventElastic_AssetIssuance_Success(t *testing.T) {
	le := LogEvent{
		Epoch:           100,
		TickNumber:      200,
		Timestamp:       1234567890,
		TransactionHash: validTxHash,
		LogId:           300,
		LogDigest:       "digest",
		Type:            1,
		Body: map[string]any{
			"assetIssuer":           "issuer-identity",
			"assetName":             "QUBIC",
			"managingContractIndex": 10.0,
			"numberOfDecimalPlaces": 2.0,
			"numberOfShares":        1000000.0,
			"unitOfMeasurement":     "1234567",
		},
	}

	lee, err := le.ToLogEventElastic()
	require.NoError(t, err)

	assert.Equal(t, "issuer-identity", lee.AssetIssuer)
	assert.Equal(t, "QUBIC", lee.AssetName)
	assert.Equal(t, uint64(10), *lee.ManagingContractIndex)
	assert.Equal(t, byte(2), *lee.NumberOfDecimalPlaces)
	assert.Equal(t, uint64(1000000), *lee.NumberOfShares)
	assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7}, lee.UnitOfMeasurement)
}

func TestLogEvent_ToLogEventElastic_AssetIssuance_Error(t *testing.T) {
	le := LogEvent{
		Epoch:           100,
		TickNumber:      200,
		Timestamp:       1234567890,
		TransactionHash: validTxHash,
		LogId:           300,
		LogDigest:       "digest",
		Type:            1,
		Body: map[string]any{
			"assetName":             "QUBIC",
			"managingContractIndex": 10.0,
			"numberOfDecimalPlaces": 2.0,
			"numberOfShares":        1000000.0,
			"unitOfMeasurement":     "1234567",
		},
	}

	_, err := le.ToLogEventElastic()
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing or invalid asset issuer")
}

func TestLogEvent_ToLogEventElastic_AssetTransfer_Success(t *testing.T) {
	le := LogEvent{
		Epoch:           100,
		TickNumber:      200,
		Timestamp:       1234567890,
		TransactionHash: validTxHash,
		LogId:           300,
		LogDigest:       "digest",
		Type:            2,
		Body: map[string]any{
			"assetIssuer":    "issuer-identity",
			"assetName":      "ASSET",
			"source":         "source-identity",
			"destination":    "dest-identity",
			"numberOfShares": 500.0,
		},
	}

	lee, err := le.ToLogEventElastic()
	require.NoError(t, err)

	assert.Equal(t, "issuer-identity", lee.AssetIssuer)
	assert.Equal(t, "ASSET", lee.AssetName)
	assert.Equal(t, "source-identity", lee.Source)
	assert.Equal(t, "dest-identity", lee.Destination)
	assert.Equal(t, uint64(500), *lee.NumberOfShares)
}

func TestLogEvent_ToLogEventElastic_AssetTransfer_Error(t *testing.T) {
	le := LogEvent{
		Epoch:           100,
		TickNumber:      200,
		Timestamp:       1234567890,
		TransactionHash: validTxHash,
		LogId:           300,
		LogDigest:       "digest",
		Type:            2,
		Body: map[string]any{
			"assetIssuer": "issuer-identity",
			"assetName":   "ASSET",
			"source":      "source-identity",
			"destination": "dest-identity",
			// missing numberOfShares
		},
	}

	_, err := le.ToLogEventElastic()
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing or invalid number of shares")
}

func TestLogEvent_ToLogEventElastic_Burn_Success(t *testing.T) {
	le := LogEvent{
		Epoch:           100,
		TickNumber:      200,
		Timestamp:       1234567890,
		TransactionHash: validTxHash,
		LogId:           300,
		LogDigest:       "digest",
		Type:            8,
		Body: map[string]any{
			"source":                 "source-identity",
			"amount":                 100.0,
			"contractIndexBurnedFor": 1.0,
		},
	}

	lee, err := le.ToLogEventElastic()
	require.NoError(t, err)

	assert.Equal(t, "source-identity", lee.Source)
	assert.Equal(t, uint64(100), *lee.Amount)
	assert.Equal(t, uint64(1), *lee.ContractIndexBurnedFor)
}

func TestLogEvent_ToLogEventElastic_Burn_Error(t *testing.T) {
	le := LogEvent{
		Epoch:           100,
		TickNumber:      200,
		Timestamp:       1234567890,
		TransactionHash: validTxHash,
		LogId:           300,
		LogDigest:       "digest",
		Type:            8,
		Body: map[string]any{
			"source": "source-identity",
			"amount": 100.0,
			// missing contractIndexBurnedFor
		},
	}

	_, err := le.ToLogEventElastic()
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing or invalid contract index burned for")
}

func TestLogEvent_ToLogEventElastic_ReserveDeduction_Success(t *testing.T) {
	le := LogEvent{
		Epoch:           100,
		TickNumber:      200,
		Timestamp:       1234567890,
		TransactionHash: validTxHash,
		LogId:           300,
		LogDigest:       "digest",
		Type:            13,
		Body: map[string]any{
			"contractIndex":   1.0,
			"deductedAmount":  50.0,
			"remainingAmount": 950.0,
		},
	}

	lee, err := le.ToLogEventElastic()
	require.NoError(t, err)

	assert.Equal(t, uint64(1), *lee.ContractIndex)
	assert.Equal(t, uint64(50), *lee.DeductedAmount)
	assert.Equal(t, int64(950), *lee.RemainingAmount)
}

func TestLogEvent_ToLogEventElastic_ReserveDeduction_Error(t *testing.T) {
	le := LogEvent{
		Epoch:           100,
		TickNumber:      200,
		Timestamp:       1234567890,
		TransactionHash: validTxHash,
		LogId:           300,
		LogDigest:       "digest",
		Type:            13,
		Body: map[string]any{
			"contractIndex":  1.0,
			"deductedAmount": 50.0,
			// missing remainingAmount
		},
	}

	_, err := le.ToLogEventElastic()
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing or invalid remaining amount")
}
