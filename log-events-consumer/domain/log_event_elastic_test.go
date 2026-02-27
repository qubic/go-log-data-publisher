package domain

import (
	"encoding/json"
	"log"
	"strings"
	"testing"
)

func TestLogEventElastic_IsSupported(t *testing.T) {
	tests := []struct {
		name      string
		eventType int16
		amount    uint64
		want      bool
	}{
		{
			name:      "Type 0 with positive amount is supported",
			eventType: 0,
			amount:    1,
			want:      true,
		},
		{
			name:      "Type 0 with amount 0 is not supported",
			eventType: 0,
			amount:    0,
			want:      false,
		},
		{
			name:      "Type 8 with positive amount is supported",
			eventType: 8,
			amount:    1,
			want:      true,
		},
		{
			name:      "Type 8 with amount 0 is not supported",
			eventType: 8,
			amount:    0,
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			le := &LogEventElastic{Type: tt.eventType, Amount: &tt.amount}
			if got := le.IsSupported(); got != tt.want {
				t.Errorf("IsSupported() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLogEventElastic_Omitempty(t *testing.T) {
	// Initialize with only required fields (no omitempty fields)
	lee := LogEventElastic{
		Epoch:      1,
		TickNumber: 2,
		Timestamp:  3,
		LogId:      4,
		LogDigest:  "digest",
		Type:       1,
	}

	data, err := json.Marshal(lee)
	if err != nil {
		t.Fatalf("Failed to marshal LogEventElastic: %v", err)
	}

	jsonStr := string(data)
	log.Printf("JSON string without optional fields: %s", jsonStr)

	// List of fields with omitempty
	omitemptyFields := []string{
		"transactionHash",
		"categories",
		"source",
		"destination",
		"amount",
		"assetName",
		"assetIssuer",
		"numberOfShares",
		"managingContractIndex",
		"unitOfMeasurement",
		"numberOfDecimalPlaces",
		"deductedAmount",
		"remainingAmount",
		"contractIndex",
		"contractIndexBurnedFor",
		"emittingContractIndex",
	}

	for _, field := range omitemptyFields {
		if strings.Contains(jsonStr, "\""+field+"\"") {
			t.Errorf("Field %q should be omitted from JSON when empty, but it was found: %s", field, jsonStr)
		}
	}

	// Now set ALL omitempty fields and verify they ARE present
	amount := uint64(100)
	shares := uint64(50)
	managingContract := uint64(10)
	decimalPlaces := byte(2)
	deducted := uint64(20)
	remaining := int64(80)
	contractIdx := uint64(30)
	burnedFor := uint64(40)
	emittingContractIndex := uint64(1)

	lee.TransactionHash = "some-tx-hash"
	lee.Categories = Categories{1, 2, 3}
	lee.Source = "source-addr"
	lee.Destination = "dest-addr"
	lee.Amount = &amount
	lee.AssetName = "QUBIC"
	lee.AssetIssuer = "issuer-addr"
	lee.NumberOfShares = &shares
	lee.ManagingContractIndex = &managingContract
	lee.UnitOfMeasurement = []byte("QU")
	lee.NumberOfDecimalPlaces = &decimalPlaces
	lee.DeductedAmount = &deducted
	lee.RemainingAmount = &remaining
	lee.ContractIndex = &contractIdx
	lee.ContractIndexBurnedFor = &burnedFor
	lee.EmittingContractIndex = &emittingContractIndex

	data, err = json.Marshal(lee)
	if err != nil {
		t.Fatalf("Failed to marshal LogEventElastic: %v", err)
	}

	jsonStr = string(data)
	log.Printf("JSON string with all optional fields: %s", jsonStr)

	presentFields := omitemptyFields

	for _, field := range presentFields {
		if !strings.Contains(jsonStr, "\""+field+"\"") {
			t.Errorf("Field %q should be present in JSON when not empty, but it was NOT found: %s", field, jsonStr)
		}
	}
}
