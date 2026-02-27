package domain

import (
	"encoding/json"
	"testing"

	"github.com/qubic/log-events-consumer/testutils"
	"github.com/stretchr/testify/require"
)

func TestLogEvent_Integration(t *testing.T) {
	tests := []struct {
		filename string
		expected string
	}{
		{
			filename: "testdata/kafka/asset-ownership-change.json",
			expected: "testdata/elastic/asset-ownership-change.json",
		},
		{
			filename: "testdata/kafka/asset-possession-change.json",
			expected: "testdata/elastic/asset-possession-change.json",
		},
		{
			filename: "testdata/kafka/burn-end-tick.json",
			expected: "testdata/elastic/burn-end-tick.json",
		},
		{
			filename: "testdata/kafka/burn-with-transaction.json",
			expected: "testdata/elastic/burn-with-transaction.json",
		},
		{
			filename: "testdata/kafka/contract-reserve-deduction-begin-tick.json",
			expected: "testdata/elastic/contract-reserve-deduction-begin-tick.json",
		},
		{
			filename: "testdata/kafka/contract-reserve-deduction-with-transaction.json",
			expected: "testdata/elastic/contract-reserve-deduction-with-transaction.json",
		},
		{
			filename: "testdata/kafka/qu-transfer-positive-amount.json",
			expected: "testdata/elastic/qu-transfer-positive-amount.json",
		},
		{
			filename: "testdata/kafka/asset-issuance.json",
			expected: "testdata/elastic/asset-issuance.json",
		},
		{
			filename: "testdata/kafka/contract-information-message.json",
			expected: "testdata/elastic/contract-information-message.json",
		},
		{
			filename: "testdata/kafka/custom-message-start-dividends.json",
			expected: "testdata/elastic/custom-message-start-dividends.json",
		},
		{
			filename: "testdata/kafka/custom-message-end-dividends.json",
			expected: "testdata/elastic/custom-message-end-dividends.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			// 1. Read the input file
			data, err := testutils.ReadTestFile(tt.filename)
			require.NoError(t, err)

			// 2. unmarshallLogEvent(record, &raw)
			var raw LogEventPtr
			err = json.Unmarshal(data, &raw)
			require.NoError(t, err)

			// 3. raw.ToLogEvent()
			logEvent, err := raw.ToLogEvent()
			require.NoError(t, err)

			// 4. logEvent.ToLogEventElastic()
			logEventElastic, err := logEvent.ToLogEventElastic()
			require.NoError(t, err)

			// 5. json.Marshal(logEventElastic)
			outputJSON, err := json.Marshal(logEventElastic)
			require.NoError(t, err)

			// 6. Read the expected output file
			expectedJSON, err := testutils.ReadTestFile(tt.expected)
			require.NoError(t, err)

			// 7. verify the output against json
			require.JSONEq(t, string(expectedJSON), string(outputJSON))
		})
	}
}
