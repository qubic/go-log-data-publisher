package domain

import (
	"encoding/json"
	"testing"

	"github.com/qubic/log-events-consumer/testutils"
	"github.com/stretchr/testify/require"
)

func TestLogEvent_Integration(t *testing.T) {
	kafkaEntries, err := testutils.ReadDir("testdata/kafka")
	require.NoError(t, err)

	matched := 0
	for _, entry := range kafkaEntries {
		name := entry.Name()
		elasticPath := "testdata/elastic/" + name
		if _, readErr := testutils.ReadTestFile(elasticPath); readErr != nil {
			continue // no matching elastic file — skip
		}
		matched++

		t.Run(name, func(t *testing.T) {
			data, err := testutils.ReadTestFile("testdata/kafka/" + name)
			require.NoError(t, err)

			var raw LogEventPtr
			err = json.Unmarshal(data, &raw)
			require.NoError(t, err)

			logEvent, err := raw.ToLogEvent()
			require.NoError(t, err)

			logEventElastic, err := logEvent.ToLogEventElastic()
			require.NoError(t, err)

			outputJSON, err := json.Marshal(logEventElastic)
			require.NoError(t, err)

			expectedJSON, err := testutils.ReadTestFile(elasticPath)
			require.NoError(t, err)

			require.JSONEq(t, string(expectedJSON), string(outputJSON))
		})
	}

	require.NotZero(t, matched, "no kafka/elastic file pairs found in testdata")
}
