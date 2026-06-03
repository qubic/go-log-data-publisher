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
	require.NotContains(t, err.Error(), "logId")
	require.Contains(t, err.Error(), "timestamp")
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
			category, isSpecial, err := inferTransactionHashCategory(tt.transactionHash)

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

func TestGetOracleQueryStatusValue(t *testing.T) {
	tests := []struct {
		input       string
		expected    int16
		shouldError bool
	}{
		{"pending", 1, false},
		{"committed", 2, false},
		{"success", 3, false},
		{"timeout", 4, false},
		{"unresolvable", 5, false},
		{"unknown", 0, true},
		{"", 0, true},
		{"PENDING", 0, true},
		{"otherstatus", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := getOracleQueryStatusValue(tt.input)
			if tt.shouldError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if got != tt.expected {
				t.Errorf("getOracleQueryStatusValue(%q) = %d, want %d", tt.input, got, tt.expected)
			}
		})
	}
}

func TestStringToDateAndTime(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected uint64
		wantErr  bool
	}{
		{
			name:     "valid timestamp",
			input:    "2025-04-09 14:30:45.123'456",
			expected: 1744209045123,
			wantErr:  false,
		},
		{
			name:     "real world example",
			input:    "2026-04-09 18:33:30.001'000",
			expected: 1775759610001,
			wantErr:  false,
		},
		{
			name:     "epoch zero",
			input:    "1970-01-01 00:00:00.000'000",
			expected: 0,
			wantErr:  false,
		},
		{
			name:     "midnight no sub-seconds",
			input:    "2025-01-01 00:00:00.000'000",
			expected: 1735689600000,
			wantErr:  false,
		},
		{
			name:    "invalid format",
			input:   "not-a-date",
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "missing microseconds",
			input:   "2025-04-09 14:30:45.123",
			wantErr: true,
		},
		{
			name:    "wrong separator",
			input:   "2025-04-09 14:30:45.123.456",
			wantErr: true,
		},
		{
			name:    "missing time",
			input:   "2025-04-09",
			wantErr: true,
		},
		{
			name:    "missing date",
			input:   "14:30:45.123'456",
			wantErr: true,
		},
		{
			name:    "letters in fields",
			input:   "abcd-ef-gh ij:kl:mn.opq'rst",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stringToDateAndTime(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.expected {
				t.Errorf("stringToDateAndTime(%q) = %d, want %d", tt.input, got, tt.expected)
			}
		})
	}
}
