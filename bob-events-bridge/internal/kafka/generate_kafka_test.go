//go:build log_reconstruction

// This file is only meant as a utility to transform bob JSON-RPC to the kafka format for manually ingesting log-events

package kafka

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/qubic/bob-events-bridge/internal/bob"
)

// rpcResponse mirrors the bob JSON-RPC 2.0 response envelope (toIngest2.json).
type rpcResponse struct {
	Result []bob.LogPayload `json:"result"`
}

// TestGenerateKafkaMessages reads the real bob producer output (JSON-RPC response
// with a result array of log payloads), runs the REAL producer transform
// (bob.ParseEventBody + kafka.BuildEventMessage), and writes the resulting Kafka
// EventMessages as NDJSON for the consumer stage to ingest.
//
//	IN=/home/linckode/asdasdasdasd/toIngest2.json \
//	OUT=/home/linckode/asdasdasdasd/kafkaMessages.ndjson \
//	  go test ./internal/kafka/ -run TestGenerateKafkaMessages -v
func TestGenerateKafkaMessages(t *testing.T) {
	inPath := os.Getenv("IN")
	outPath := os.Getenv("OUT")
	if inPath == "" {
		t.Skip("set IN=<path to toIngest2.json> to run")
	}

	raw, err := os.ReadFile(inPath)
	if err != nil {
		t.Fatalf("reading input: %v", err)
	}

	var resp rpcResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("unmarshalling rpc response: %v", err)
	}

	var out []byte
	for i, payload := range resp.Result {
		parsed, err := bob.ParseEventBody(payload.Type, payload.Body)
		if err != nil {
			t.Fatalf("record %d: ParseEventBody: %v", i, err)
		}

		// Isolated slice: no dividend-section state, index is positional.
		// These flags do not affect the elastic document for QU_TRANSFER.
		msg, err := BuildEventMessage(parsed, uint32(i), false, &resp.Result[i], false)
		if err != nil {
			t.Fatalf("record %d: BuildEventMessage: %v", i, err)
		}

		line, err := json.Marshal(msg)
		if err != nil {
			t.Fatalf("record %d: marshalling kafka message: %v", i, err)
		}
		out = append(out, line...)
		out = append(out, '\n')
	}

	t.Logf("produced %d kafka messages", len(resp.Result))
	if outPath != "" {
		if err := os.WriteFile(outPath, out, 0o644); err != nil {
			t.Fatalf("writing output: %v", err)
		}
		t.Logf("wrote kafka messages to %s", outPath)
	} else {
		t.Logf("\n%s", string(out))
	}
}
