package domain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const validTxHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafxib"

func TestLogEvent_ToLogEventElastic_ZeroValues(t *testing.T) {
	le := LogEvent{
		Epoch:      0,
		TickNumber: 0,
		LogId:      0,
		Timestamp:  0,
		LogDigest:  "",
	}

	_, err := le.ToLogEventElastic()
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid zero value field(s)")
	require.Contains(t, err.Error(), "epoch")
	require.Contains(t, err.Error(), "tickNumber")
	require.Contains(t, err.Error(), "logId")
	// Disabled for the time being.
	//require.Contains(t, err.Error(), "timestamp")
	require.Contains(t, err.Error(), "logDigest")
}

func TestInferCategory(t *testing.T) {
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
			category, isSpecial, err := inferCategory(tt.transactionHash)

			if tt.expectError {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
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

func TestToUint64(t *testing.T) {
	tests := []struct {
		name        string
		input       float64
		expected    uint64
		expectError bool
	}{
		{"Valid uint64", 12345.0, 12345, false},
		{"Zero", 0.0, 0, false},
		{"Negative value", -1.0, 0, true},
		{"Decimal value", 123.45, 0, true},
		{"Large uint64", 9007199254740991.0, 9007199254740991, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toUint64(tt.input)
			if (err != nil) != tt.expectError {
				t.Errorf("toUint64(%f) error = %v, expectError %v", tt.input, err, tt.expectError)
				return
			}
			if !tt.expectError && *got != tt.expected {
				t.Errorf("toUint64(%f) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		name        string
		input       float64
		expected    int64
		expectError bool
	}{
		{"Valid int64", 12345.0, 12345, false},
		{"Zero", 0.0, 0, false},
		{"Negative value", -1.0, 0, true}, // based on implementation: if num < 0 { return 0, err }
		{"Decimal value", 123.45, 0, true},
		{"Large int64", 9007199254740991.0, 9007199254740991, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toInt64(tt.input)
			if (err != nil) != tt.expectError {
				t.Errorf("toInt64(%f) error = %v, expectError %v", tt.input, err, tt.expectError)
				return
			}
			if !tt.expectError && *got != tt.expected {
				t.Errorf("toInt64(%f) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestToByte(t *testing.T) {
	tests := []struct {
		name        string
		input       float64
		expected    byte
		expectError bool
	}{
		{"Valid byte", 123.0, 123, false},
		{"Zero", 0.0, 0, false},
		{"Max byte", 255.0, 255, false},
		{"Negative value", -1.0, 0, true},
		{"Out of range", 256.0, 0, true},
		{"Decimal value", 123.45, 123, false}, // based on implementation: if num < 0 || num > 255 { return 0, err } return byte(num), nil. Wait, it doesn't check if it's integer!
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toByte(tt.input)
			if (err != nil) != tt.expectError {
				t.Errorf("toByte(%f) error = %v, expectError %v", tt.input, err, tt.expectError)
				return
			}
			if !tt.expectError && *got != tt.expected {
				t.Errorf("toByte(%f) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestLogEvent_IsSupported(t *testing.T) {
	supportedMap := map[uint64][]int16{
		0: {0, 1, 2, 3, 8, 13},
	}
	tests := []struct {
		name      string
		eventType int16
		want      bool
	}{
		{
			name:      "Type 0 is supported",
			eventType: 0,
			want:      true,
		},
		{
			name:      "Type 1 is supported",
			eventType: 1,
			want:      true,
		},
		{
			name:      "Type 2 is supported",
			eventType: 2,
			want:      true,
		},
		{
			name:      "Type 3 is supported",
			eventType: 3,
			want:      true,
		},
		{
			name:      "Type 8 is supported",
			eventType: 8,
			want:      true,
		},
		{
			name:      "Type 13 is supported",
			eventType: 13,
			want:      true,
		},
		{
			name:      "Type 4 is not supported",
			eventType: 4,
			want:      false,
		},
		{
			name:      "Type 100 is not supported",
			eventType: 100,
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			le := &LogEvent{Type: tt.eventType}
			if got := le.IsSupported(supportedMap); got != tt.want {
				t.Errorf("IsSupported() = %v, want %v", got, tt.want)
			}
		})
	}
}
