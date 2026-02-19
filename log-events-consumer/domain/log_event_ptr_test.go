package domain

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogEventPtr_ToLogEvent_Validation(t *testing.T) {
	tests := []struct {
		name          string
		jsonInput     string
		expectError   bool
		errorContains string
	}{
		{
			name: "Success - all required fields present",
			jsonInput: `{
				"epoch": 1,
				"tickNumber": 100,
				"logId": 123,
				"timestamp": 123456789,
				"logDigest": "digest",
				"index": 1,
				"type": 0,
				"emittingContractIndex": 5
			}`,
			expectError: false,
		},
		{
			name: "Missing epoch",
			jsonInput: `{
				"tickNumber": 100,
				"logId": 123,
				"timestamp": 123456789,
				"logDigest": "digest",
				"transactionHash": "hash",
				"index": 1,
				"type": 0,
				"emittingContractIndex": 5
			}`,
			expectError:   true,
			errorContains: "epoch",
		},
		{
			name: "Missing multiple fields",
			jsonInput: `{
				"logId": 123,
				"timestamp": 123456789,
				"logDigest": "digest"
			}`,
			expectError:   true,
			errorContains: "epoch",
		},
		{
			name: "Optional fields missing (success)",
			jsonInput: `{
				"epoch": 1,
				"tickNumber": 100,
				"logId": 123,
				"timestamp": 123456789,
				"logDigest": "digest",
				"index": 1,
				"type": 0,
				"emittingContractIndex": 5
			}`,
			expectError: false,
		},
		{
			name: "Missing logDigest",
			jsonInput: `{
				"epoch": 1,
				"tickNumber": 100,
				"logId": 123,
				"timestamp": 123456789,
				"index": 1,
				"type": 0,
				"emittingContractIndex": 5
			}`,
			expectError:   true,
			errorContains: "logDigest",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lep LogEventPtr
			err := json.Unmarshal([]byte(tt.jsonInput), &lep)
			if err != nil {
				t.Fatalf("Failed to unmarshal JSON: %v", err)
			}

			le, err := lep.ToLogEvent()
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got nil")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error containing %q, got %q", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				// Basic check on values for success case
				if tt.name == "Success - all required fields present" {
					if le.Epoch != 1 || le.TickNumber != 100 || le.LogId != 123 {
						t.Errorf("Converted values mismatch: %+v", le)
					}
				}
			}
		})
	}
}

func TestLogEventPtr_ToLogEvent_OptionalFields(t *testing.T) {
	jsonInput := `{
		"epoch": 1,
		"tickNumber": 100,
		"logId": 123,
		"timestamp": 123456789,
		"logDigest": "digest",
		"transactionHash": "hash",
		"index": 1,
		"type": 0,
		"emittingContractIndex": 5,
		"bodySize": 10,
		"body": {"foo": "bar"}
	}`

	var lep LogEventPtr
	err := json.Unmarshal([]byte(jsonInput), &lep)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	le, err := lep.ToLogEvent()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if le.BodySize != 10 {
		t.Errorf("Expected bodySize 10, got %d", le.BodySize)
	}
	if le.Body["foo"] != "bar" {
		t.Errorf("Expected body['foo'] = 'bar', got %v", le.Body["foo"])
	}
}

func TestLogEventPtr_ToLogEvent_InvalidZeroFields(t *testing.T) {
	jsonInput := `{
				"epoch": 0,
				"tickNumber": 0,
				"logId": 0,
				"timestamp": 0,
				"logDigest": "",
				"index": 0,
				"type": 0,
				"emittingContractIndex": 0
			}`

	var lep LogEventPtr
	err := json.Unmarshal([]byte(jsonInput), &lep)
	require.NoError(t, err)

	_, err = lep.ToLogEvent()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "zero value field(s)")
	assert.Contains(t, err.Error(), "epoch")
	assert.Contains(t, err.Error(), "tickNumber")
	assert.Contains(t, err.Error(), "logId")
	assert.Contains(t, err.Error(), "logDigest")
	// Disabled for the time being.
	//assert.Contains(t, err.Error(), "timestamp")
}
