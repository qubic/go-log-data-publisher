package domain

import (
	"encoding/json"
	"testing"
)

const validTxHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafxib"

func TestLogEventToElastic_AllFields(t *testing.T) {
	logEvent := LogEvent{
		Epoch:                 100,
		TickNumber:            200,
		Index:                 300,
		Type:                  1,
		EmittingContractIndex: 5,
		LogId:                 400,
		LogDigest:             "abcd1234",
		//TransactionHash:       "ohdjqzrwjlugzbzmmqswdxkqapgafkeokblkmjmrdexmcazkfxxmgpveqqik",
		TransactionHash: validTxHash,
		Timestamp:       1234567890,
		BodySize:        50,
		Body: map[string]any{
			"source":                 "SOURCEADDRESS",
			"destination":            "DESTADDRESS",
			"amount":                 int64(1000),
			"assetName":              "TESTASSET",
			"assetIssuer":            "ISSUERADDRESS",
			"numberOfShares":         int64(500),
			"managingContractIndex":  int64(10),
			"unitOfMeasurement":      "1234567",
			"numberOfDecimalPlaces":  float64(8),
			"deductedAmount":         uint64(100),
			"remainingAmount":        int64(900),
			"contractIndex":          uint32(15),
			"contractIndexBurnedFor": uint32(20),
		},
	}

	result, err := logEvent.ToLogEventElastic()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify all base fields
	if result.Epoch != 100 {
		t.Errorf("expected Epoch=100, got %d", result.Epoch)
	}
	if result.TickNumber != 200 {
		t.Errorf("expected TickNumber=200, got %d", result.TickNumber)
	}
	if result.Timestamp != 1234567890 {
		t.Errorf("expected Timestamp=1234567890, got %d", result.Timestamp)
	}
	if result.EmittingContractIndex != 5 {
		t.Errorf("expected EmittingContractIndex=5, got %d", result.EmittingContractIndex)
	}
	if result.TransactionHash != validTxHash {
		t.Errorf("expected TransactionHash=hash123, got %s", result.TransactionHash)
	}
	if result.LogId != 400 {
		t.Errorf("expected LogId=400, got %d", result.LogId)
	}
	if result.LogDigest != "abcd1234" {
		t.Errorf("expected LogDigest=abcd1234, got %s", result.LogDigest)
	}
	if result.Type != 1 {
		t.Errorf("expected Type=1, got %d", result.Type)
	}
	if result.Category != nil {
		t.Errorf("expected Category=nil for regular transaction, got %v", *result.Category)
	}

	// Verify all body fields
	if result.Source != "SOURCEADDRESS" {
		t.Errorf("expected Source=SOURCEADDRESS, got %s", result.Source)
	}
	if result.Destination != "DESTADDRESS" {
		t.Errorf("expected Destination=DESTADDRESS, got %s", result.Destination)
	}
	if result.Amount != 1000 {
		t.Errorf("expected Amount=1000, got %d", result.Amount)
	}
	if result.AssetName != "TESTASSET" {
		t.Errorf("expected AssetName=TESTASSET, got %s", result.AssetName)
	}
	if result.AssetIssuer != "ISSUERADDRESS" {
		t.Errorf("expected AssetIssuer=ISSUERADDRESS, got %s", result.AssetIssuer)
	}
	if result.NumberOfShares != 500 {
		t.Errorf("expected NumberOfShares=500, got %d", result.NumberOfShares)
	}
	if result.ManagingContractIndex != 10 {
		t.Errorf("expected ManagingContractIndex=10, got %d", result.ManagingContractIndex)
	}

	expectedBytes := []byte{1, 2, 3, 4, 5, 6, 7}
	if len(result.UnitOfMeasurement) != 7 {
		t.Fatalf("expected UnitOfMeasurement length=7, got %d", len(result.UnitOfMeasurement))
	}
	for i := 0; i < 7; i++ {
		if result.UnitOfMeasurement[i] != expectedBytes[i] {
			t.Errorf("expected UnitOfMeasurement[%d]=%d, got %d", i, expectedBytes[i], result.UnitOfMeasurement[i])
		}
	}

	if result.NumberOfDecimalPlaces != 8 {
		t.Errorf("expected NumberOfDecimalPlaces=8, got %d", result.NumberOfDecimalPlaces)
	}
	if result.DeductedAmount != 100 {
		t.Errorf("expected DeductedAmount=100, got %d", result.DeductedAmount)
	}
	if result.RemainingAmount != 900 {
		t.Errorf("expected RemainingAmount=900, got %d", result.RemainingAmount)
	}
	if result.ContractIndex != 15 {
		t.Errorf("expected ContractIndex=15, got %d", result.ContractIndex)
	}
	if result.ContractIndexBurnedFor != 20 {
		t.Errorf("expected ContractIndexBurnedFor=20, got %d", result.ContractIndexBurnedFor)
	}
}

func TestLogEventToElastic_OmitEmptyFields(t *testing.T) {
	tests := []struct {
		name           string
		body           map[string]any
		expectedJSON   map[string]any
		unexpectedKeys []string
	}{
		{
			name: "only source and destination",
			body: map[string]any{
				"source":      "SOURCEADDR",
				"destination": "DESTADDR",
			},
			expectedJSON: map[string]any{
				"epoch":                 float64(100),
				"tickNumber":            float64(200),
				"timestamp":             float64(1234567890),
				"emittingContractIndex": float64(5),
				"transactionHash":       validTxHash,
				"logId":                 float64(400),
				"logDigest":             "abcd1234",
				"type":                  float64(1),
				"source":                "SOURCEADDR",
				"destination":           "DESTADDR",
			},
			unexpectedKeys: []string{
				"category", "amount", "assetName", "assetIssuer", "numberOfShares",
				"managingContractIndex", "unitOfMeasurement", "numberOfDecimalPlaces",
				"deductedAmount", "remainingAmount", "contractIndex", "contractIndexBurnedFor",
			},
		},
		{
			name: "only amount field",
			body: map[string]any{
				"amount": int64(5000),
			},
			expectedJSON: map[string]any{
				"epoch":                 float64(100),
				"tickNumber":            float64(200),
				"timestamp":             float64(1234567890),
				"emittingContractIndex": float64(5),
				"transactionHash":       validTxHash,
				"logId":                 float64(400),
				"logDigest":             "abcd1234",
				"type":                  float64(1),
				"amount":                float64(5000),
			},
			unexpectedKeys: []string{
				"category", "source", "destination", "assetName", "assetIssuer", "numberOfShares",
				"managingContractIndex", "unitOfMeasurement", "numberOfDecimalPlaces",
				"deductedAmount", "remainingAmount", "contractIndex", "contractIndexBurnedFor",
			},
		},
		{
			name: "asset related fields",
			body: map[string]any{
				"assetName":             "MYASSET",
				"assetIssuer":           "ISSUER",
				"numberOfShares":        int64(1000),
				"managingContractIndex": int64(3),
			},
			expectedJSON: map[string]any{
				"epoch":                 float64(100),
				"tickNumber":            float64(200),
				"timestamp":             float64(1234567890),
				"emittingContractIndex": float64(5),
				"transactionHash":       validTxHash,
				"logId":                 float64(400),
				"logDigest":             "abcd1234",
				"type":                  float64(1),
				"assetName":             "MYASSET",
				"assetIssuer":           "ISSUER",
				"numberOfShares":        float64(1000),
				"managingContractIndex": float64(3),
			},
			unexpectedKeys: []string{
				"category", "source", "destination", "amount", "unitOfMeasurement", "numberOfDecimalPlaces",
				"deductedAmount", "remainingAmount", "contractIndex", "contractIndexBurnedFor",
			},
		},
		{
			name: "empty body map",
			body: map[string]any{},
			expectedJSON: map[string]any{
				"epoch":                 float64(100),
				"tickNumber":            float64(200),
				"timestamp":             float64(1234567890),
				"emittingContractIndex": float64(5),
				"transactionHash":       validTxHash,
				"logId":                 float64(400),
				"logDigest":             "abcd1234",
				"type":                  float64(1),
			},
			unexpectedKeys: []string{
				"category", "source", "destination", "amount", "assetName", "assetIssuer", "numberOfShares",
				"managingContractIndex", "unitOfMeasurement", "numberOfDecimalPlaces",
				"deductedAmount", "remainingAmount", "contractIndex", "contractIndexBurnedFor",
			},
		},
		{
			name: "contract related fields",
			body: map[string]any{
				"contractIndex":          uint32(7),
				"contractIndexBurnedFor": uint32(9),
				"deductedAmount":         uint64(250),
				"remainingAmount":        int64(750),
			},
			expectedJSON: map[string]any{
				"epoch":                  float64(100),
				"tickNumber":             float64(200),
				"timestamp":              float64(1234567890),
				"emittingContractIndex":  float64(5),
				"transactionHash":        validTxHash,
				"logId":                  float64(400),
				"logDigest":              "abcd1234",
				"type":                   float64(1),
				"contractIndex":          float64(7),
				"contractIndexBurnedFor": float64(9),
				"deductedAmount":         float64(250),
				"remainingAmount":        float64(750),
			},
			unexpectedKeys: []string{
				"category", "source", "destination", "amount", "assetName", "assetIssuer", "numberOfShares",
				"managingContractIndex", "unitOfMeasurement", "numberOfDecimalPlaces",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logEvent := LogEvent{
				Epoch:                 100,
				TickNumber:            200,
				Index:                 300,
				Type:                  1,
				EmittingContractIndex: 5,
				LogId:                 400,
				LogDigest:             "abcd1234",
				TransactionHash:       validTxHash,
				Timestamp:             1234567890,
				BodySize:              50,
				Body:                  tt.body,
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

			// Check that expected keys are present
			for key, expectedValue := range tt.expectedJSON {
				actualValue, exists := jsonMap[key]
				if !exists {
					t.Errorf("expected key '%s' to be present in JSON, but it was missing", key)
					continue
				}
				if actualValue != expectedValue {
					t.Errorf("key '%s': expected value %v, got %v", key, expectedValue, actualValue)
				}
			}

			// Check that unexpected keys are NOT present (omitempty working)
			for _, key := range tt.unexpectedKeys {
				if _, exists := jsonMap[key]; exists {
					t.Errorf("key '%s' should NOT be present in JSON (omitempty not working), but it was found with value: %v", key, jsonMap[key])
				}
			}
		})
	}
}

func TestLogEventToElastic_NegativeAmountError(t *testing.T) {
	logEvent := LogEvent{
		Epoch:      100,
		TickNumber: 200,
		Body: map[string]any{
			"amount": int64(-100),
		},
	}

	_, err := logEvent.ToLogEventElastic()
	if err == nil {
		t.Fatal("expected error for negative amount, got nil")
	}
	expectedMsg := "amount cannot be negative"
	if !contains(err.Error(), expectedMsg) {
		t.Errorf("expected error message to contain '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestLogEventToElastic_NegativeNumberOfSharesError(t *testing.T) {
	logEvent := LogEvent{
		Epoch:      100,
		TickNumber: 200,
		Body: map[string]any{
			"numberOfShares": int64(-50),
		},
	}

	_, err := logEvent.ToLogEventElastic()
	if err == nil {
		t.Fatal("expected error for negative numberOfShares, got nil")
	}
	expectedMsg := "numberOfShares cannot be negative"
	if !contains(err.Error(), expectedMsg) {
		t.Errorf("expected error message to contain '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestLogEventToElastic_NegativeManagingContractIndexError(t *testing.T) {
	logEvent := LogEvent{
		Epoch:      100,
		TickNumber: 200,
		Body: map[string]any{
			"managingContractIndex": int64(-5),
		},
	}

	_, err := logEvent.ToLogEventElastic()
	if err == nil {
		t.Fatal("expected error for negative managingContractIndex, got nil")
	}
	expectedMsg := "managingContractIndex cannot be negative"
	if !contains(err.Error(), expectedMsg) {
		t.Errorf("expected error message to contain '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestLogEventToElastic_UnitOfMeasurementValidation(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		expectError bool
		errorMsg    string
	}{
		{
			name:        "correct length",
			value:       "1234567",
			expectError: false,
		},
		{
			name:        "too short",
			value:       "123456",
			expectError: true,
			errorMsg:    "must be exactly 7 characters",
		},
		{
			name:        "too long",
			value:       "12345678",
			expectError: true,
			errorMsg:    "must be exactly 7 characters",
		},
		{
			name:        "wrong type",
			value:       123,
			expectError: true,
			errorMsg:    "expected string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logEvent := LogEvent{
				Epoch:      100,
				TickNumber: 200,
				Body: map[string]any{
					"unitOfMeasurement": tt.value,
				},
			}

			_, err := logEvent.ToLogEventElastic()
			if tt.expectError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error message to contain '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestLogEventToElastic_NumberOfDecimalPlacesValidation(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid value 0",
			value:       float64(0),
			expectError: false,
		},
		{
			name:        "valid value 8",
			value:       float64(8),
			expectError: false,
		},
		{
			name:        "valid value 255",
			value:       float64(255),
			expectError: false,
		},
		{
			name:        "negative value",
			value:       float64(-1),
			expectError: true,
			errorMsg:    "must be in range 0-255",
		},
		{
			name:        "too large",
			value:       float64(256),
			expectError: true,
			errorMsg:    "must be in range 0-255",
		},
		{
			name:        "not a whole number",
			value:       float64(3.5),
			expectError: true,
			errorMsg:    "must be a whole number",
		},
		{
			name:        "wrong type",
			value:       "8",
			expectError: true,
			errorMsg:    "expected float64",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logEvent := LogEvent{
				Epoch:      100,
				TickNumber: 200,
				Body: map[string]any{
					"numberOfDecimalPlaces": tt.value,
				},
			}

			_, err := logEvent.ToLogEventElastic()
			if tt.expectError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error message to contain '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestLogEventToElastic_UnknownBodyKey(t *testing.T) {
	logEvent := LogEvent{
		Epoch:      100,
		TickNumber: 200,
		Body: map[string]any{
			"unknownField": "someValue",
		},
	}

	_, err := logEvent.ToLogEventElastic()
	if err == nil {
		t.Fatal("expected error for unknown body key, got nil")
	}
	expectedMsg := "unknown body key 'unknownField'"
	if !contains(err.Error(), expectedMsg) {
		t.Errorf("expected error message to contain '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestLogEventToElastic_WrongDataType(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    any
		errorMsg string
	}{
		{
			name:     "source as number",
			key:      "source",
			value:    123,
			errorMsg: "wrong data type for 'source'",
		},
		{
			name:     "destination as number",
			key:      "destination",
			value:    456,
			errorMsg: "wrong data type for 'destination'",
		},
		{
			name:     "amount as string",
			key:      "amount",
			value:    "1000",
			errorMsg: "wrong data type for 'amount'",
		},
		{
			name:     "assetName as number",
			key:      "assetName",
			value:    789,
			errorMsg: "wrong data type for 'assetName'",
		},
		{
			name:     "numberOfShares as string",
			key:      "numberOfShares",
			value:    "500",
			errorMsg: "wrong data type for 'numberOfShares'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logEvent := LogEvent{
				Epoch:      100,
				TickNumber: 200,
				Body: map[string]any{
					tt.key: tt.value,
				},
			}

			_, err := logEvent.ToLogEventElastic()
			if err == nil {
				t.Fatalf("expected error for wrong data type, got nil")
			}
			if !contains(err.Error(), tt.errorMsg) {
				t.Errorf("expected error message to contain '%s', got '%s'", tt.errorMsg, err.Error())
			}
		})
	}
}

func TestAssignTyped_Success(t *testing.T) {
	var target string
	err := assignTyped("testKey", "testValue", &target)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target != "testValue" {
		t.Errorf("expected target='testValue', got '%s'", target)
	}

	var numTarget int64
	err = assignTyped("numKey", int64(42), &numTarget)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if numTarget != 42 {
		t.Errorf("expected numTarget=42, got %d", numTarget)
	}
}

func TestAssignTyped_TypeMismatch(t *testing.T) {
	var target string
	err := assignTyped("testKey", 123, &target)
	if err == nil {
		t.Fatal("expected error for type mismatch, got nil")
	}
	expectedMsg := "wrong data type for 'testKey'"
	if !contains(err.Error(), expectedMsg) {
		t.Errorf("expected error message to contain '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestAssignTyped_JSONFloat64ToInt64(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		expected    int64
		expectError bool
		errorMsg    string
	}{
		{
			name:     "float64 whole number to int64",
			value:    float64(42),
			expected: 42,
		},
		{
			name:     "float64 negative to int64",
			value:    float64(-100),
			expected: -100,
		},
		{
			name:     "float64 large number to int64",
			value:    float64(1000000),
			expected: 1000000,
		},
		{
			name:        "float64 decimal to int64",
			value:       float64(42.5),
			expectError: true,
			errorMsg:    "must be a whole number",
		},
		{
			name:     "direct int64 to int64",
			value:    int64(99),
			expected: 99,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var target int64
			err := assignTyped("testKey", tt.value, &target)

			if tt.expectError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error message to contain '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if target != tt.expected {
					t.Errorf("expected %d, got %d", tt.expected, target)
				}
			}
		})
	}
}

func TestAssignTyped_JSONFloat64ToUint64(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		expected    uint64
		expectError bool
		errorMsg    string
	}{
		{
			name:     "float64 positive to uint64",
			value:    float64(42),
			expected: 42,
		},
		{
			name:     "float64 zero to uint64",
			value:    float64(0),
			expected: 0,
		},
		{
			name:        "float64 negative to uint64",
			value:       float64(-1),
			expectError: true,
			errorMsg:    "must be a non-negative whole number",
		},
		{
			name:        "float64 decimal to uint64",
			value:       float64(42.5),
			expectError: true,
			errorMsg:    "must be a non-negative whole number",
		},
		{
			name:     "direct uint64 to uint64",
			value:    uint64(99),
			expected: 99,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var target uint64
			err := assignTyped("testKey", tt.value, &target)

			if tt.expectError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error message to contain '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if target != tt.expected {
					t.Errorf("expected %d, got %d", tt.expected, target)
				}
			}
		})
	}
}

func TestAssignTyped_JSONFloat64ToUint32(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		expected    uint32
		expectError bool
		errorMsg    string
	}{
		{
			name:     "float64 positive to uint32",
			value:    float64(42),
			expected: 42,
		},
		{
			name:     "float64 max uint32",
			value:    float64(4294967295),
			expected: 4294967295,
		},
		{
			name:        "float64 exceeds uint32",
			value:       float64(4294967296),
			expectError: true,
			errorMsg:    "must be a valid uint32",
		},
		{
			name:        "float64 negative to uint32",
			value:       float64(-1),
			expectError: true,
			errorMsg:    "must be a valid uint32",
		},
		{
			name:     "direct uint32 to uint32",
			value:    uint32(99),
			expected: 99,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var target uint32
			err := assignTyped("testKey", tt.value, &target)

			if tt.expectError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error message to contain '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if target != tt.expected {
					t.Errorf("expected %d, got %d", tt.expected, target)
				}
			}
		})
	}
}

func TestLogEventToElastic_SpecialSystemTransactions(t *testing.T) {
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
				Epoch:                 100,
				TickNumber:            200,
				Type:                  1,
				EmittingContractIndex: 5,
				LogId:                 400,
				LogDigest:             "abcd1234",
				TransactionHash:       tt.transactionHash,
				Timestamp:             1234567890,
				Body:                  map[string]any{},
			}

			result, err := logEvent.ToLogEventElastic()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Verify category is set correctly
			if result.Category == nil {
				t.Fatalf("expected Category to be set, got nil")
			}
			if *result.Category != tt.expectedCategory {
				t.Errorf("expected Category=%d, got %d", tt.expectedCategory, *result.Category)
			}

			// Verify transaction hash is cleared
			if tt.expectedTxHashEmpty && result.TransactionHash != "" {
				t.Errorf("expected TransactionHash to be empty, got %s", result.TransactionHash)
			}
		})
	}
}

func TestLogEventToElastic_SpecialTransactionJSON(t *testing.T) {
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
				Epoch:                 100,
				TickNumber:            200,
				Type:                  1,
				EmittingContractIndex: 5,
				LogId:                 400,
				LogDigest:             "abcd1234",
				TransactionHash:       tt.transactionHash,
				Timestamp:             1234567890,
				Body:                  map[string]any{},
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

			// Verify category is present in JSON (even if 0)
			categoryValue, exists := jsonMap["category"]
			if !exists {
				t.Errorf("expected 'category' to be present in JSON, but it was missing")
			} else {
				// JSON numbers are float64
				if categoryFloat, ok := categoryValue.(float64); !ok {
					t.Errorf("expected category to be a number, got %T", categoryValue)
				} else if byte(categoryFloat) != tt.expectedCategory {
					t.Errorf("expected category=%d in JSON, got %f", tt.expectedCategory, categoryFloat)
				}
			}

			// Verify transactionHash is NOT present in JSON (omitted because empty)
			if _, exists := jsonMap["transactionHash"]; exists {
				t.Errorf("expected 'transactionHash' to be omitted from JSON for special transactions, but it was present with value: %v", jsonMap["transactionHash"])
			}
		})
	}
}

func TestLogEventToElastic_UnknownSpecialTransaction(t *testing.T) {
	logEvent := LogEvent{
		Epoch:           100,
		TickNumber:      200,
		Type:            1,
		TransactionHash: "SC_UNKNOWN_TX_12345",
		Body:            map[string]any{},
	}

	_, err := logEvent.ToLogEventElastic()
	if err == nil {
		t.Fatal("expected error for unknown special transaction, got nil")
	}
	expectedMsg := "SC_UNKNOWN_TX"
	if !contains(err.Error(), expectedMsg) {
		t.Errorf("expected error message to contain '%s', got '%s'", expectedMsg, err.Error())
	}
}

func TestLogEventToElastic_MalformedSpecialTransaction(t *testing.T) {
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
				TransactionHash: tt.transactionHash,
				Body:            map[string]any{},
			}

			_, err := logEvent.ToLogEventElastic()
			if err == nil {
				t.Fatal("expected error for malformed special transaction, got nil")
			}
			if !contains(err.Error(), tt.expectedErrMsg) {
				t.Errorf("expected error message to contain '%s', got '%s'", tt.expectedErrMsg, err.Error())
			}
		})
	}
}

func TestLogEventToElastic_TypeOverflow(t *testing.T) {
	tests := []struct {
		name        string
		typeValue   uint32
		expectError bool
	}{
		{
			name:        "type within int16 range",
			typeValue:   32767,
			expectError: false,
		},
		{
			name:        "type exceeds int16 max",
			typeValue:   32768,
			expectError: true,
		},
		{
			name:        "type far exceeds int16 max",
			typeValue:   65535,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logEvent := LogEvent{
				Epoch:           100,
				TickNumber:      200,
				Type:            tt.typeValue,
				TransactionHash: validTxHash,
				Body:            map[string]any{},
			}

			_, err := logEvent.ToLogEventElastic()
			if tt.expectError {
				if err == nil {
					t.Fatal("expected error for type overflow, got nil")
				}
				expectedMsg := "exceeds int16 maximum"
				if !contains(err.Error(), expectedMsg) {
					t.Errorf("expected error message to contain '%s', got '%s'", expectedMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestIsSpecialSystemTransaction(t *testing.T) {
	tests := []struct {
		name string

		transactionHash   string
		expectedCategory  byte
		expectedIsSpecial bool
		expectError       bool
		errorMsg          string
	}{
		{
			name:              "regular transaction hash",
			transactionHash:   validTxHash,
			expectedCategory:  0x00,
			expectedIsSpecial: false,
			expectError:       false,
		},
		{
			name:              "SC_END_TICK_TX with suffix",
			transactionHash:   "SC_END_TICK_TX_12345",
			expectedCategory:  4,
			expectedIsSpecial: true,
			expectError:       false,
		},
		{
			name:              "SC_INITIALIZE_TX with suffix",
			transactionHash:   "SC_INITIALIZE_TX_67890",
			expectedCategory:  1,
			expectedIsSpecial: true,
			expectError:       false,
		},
		{
			name:             "unknown SC_ transaction",
			transactionHash:  "SC_INVALID_TX_12345",
			expectedCategory: 1,
			expectError:      true,
			errorMsg:         "unexpected special event log type [SC_INVALID_TX_12345]",
		},
		{
			name:             "SC_ with no underscore after prefix",
			transactionHash:  "SC_ENDTICK",
			expectedCategory: 1,
			expectError:      true,
			errorMsg:         "unexpected special event log type [SC_ENDTICK]",
		},
		{
			name:             "SC_ only",
			transactionHash:  "SC_",
			expectedCategory: 1,
			expectError:      true,
			errorMsg:         "unexpected special event log type [SC_]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			category, isSpecial, err := inferCategoryFromTransactionHash(tt.transactionHash)

			if tt.expectError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error message to contain '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if isSpecial != tt.expectedIsSpecial {
					t.Errorf("expected isSpecial=%v, got %v", tt.expectedIsSpecial, isSpecial)
				}
				if tt.expectedIsSpecial && category != tt.expectedCategory {
					t.Errorf("expected category=%d, got %d", tt.expectedCategory, category)
				}
			}
		})
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || indexOf(s, substr) >= 0)
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
