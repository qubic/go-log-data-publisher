package domain

import (
	"encoding/json"
	"testing"
)

func TestLogEvent_JSONMarshaling(t *testing.T) {
	logEvent := LogEvent{
		Ok:        true,
		Epoch:     100,
		Tick:      1000,
		Id:        12345,
		Hash:      "test-hash",
		Type:      1,
		TypeName:  "TestEvent",
		Timestamp: "2024-01-01T00:00:00Z",
		TxHash:    "tx-hash",
		BodySize:  100,
		Body:      "test-body",
	}

	jsonData, err := json.Marshal(logEvent)
	if err != nil {
		t.Fatalf("Failed to marshal LogEvent: %v", err)
	}

	var unmarshaled LogEvent
	err = json.Unmarshal(jsonData, &unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal LogEvent: %v", err)
	}

	if unmarshaled.Ok != logEvent.Ok {
		t.Errorf("Expected Ok %v, got %v", logEvent.Ok, unmarshaled.Ok)
	}
	if unmarshaled.Epoch != logEvent.Epoch {
		t.Errorf("Expected Epoch %d, got %d", logEvent.Epoch, unmarshaled.Epoch)
	}
	if unmarshaled.Tick != logEvent.Tick {
		t.Errorf("Expected Tick %d, got %d", logEvent.Tick, unmarshaled.Tick)
	}
	if unmarshaled.Id != logEvent.Id {
		t.Errorf("Expected Id %d, got %d", logEvent.Id, unmarshaled.Id)
	}
	if unmarshaled.Hash != logEvent.Hash {
		t.Errorf("Expected Hash %s, got %s", logEvent.Hash, unmarshaled.Hash)
	}
	if unmarshaled.Type != logEvent.Type {
		t.Errorf("Expected Type %d, got %d", logEvent.Type, unmarshaled.Type)
	}
	if unmarshaled.TypeName != logEvent.TypeName {
		t.Errorf("Expected TypeName %s, got %s", logEvent.TypeName, unmarshaled.TypeName)
	}
	if unmarshaled.Timestamp != logEvent.Timestamp {
		t.Errorf("Expected Timestamp %s, got %s", logEvent.Timestamp, unmarshaled.Timestamp)
	}
	if unmarshaled.TxHash != logEvent.TxHash {
		t.Errorf("Expected TxHash %s, got %s", logEvent.TxHash, unmarshaled.TxHash)
	}
	if unmarshaled.BodySize != logEvent.BodySize {
		t.Errorf("Expected BodySize %d, got %d", logEvent.BodySize, unmarshaled.BodySize)
	}
	if unmarshaled.Body != logEvent.Body {
		t.Errorf("Expected Body %s, got %s", logEvent.Body, unmarshaled.Body)
	}
}

func TestLogEvent_JSONUnmarshalPartialData(t *testing.T) {
	jsonData := []byte(`{
		"ok": true,
		"epoch": 100,
		"tick": 1000,
		"id": 12345
	}`)

	var logEvent LogEvent
	err := json.Unmarshal(jsonData, &logEvent)
	if err != nil {
		t.Fatalf("Failed to unmarshal partial LogEvent: %v", err)
	}

	if !logEvent.Ok {
		t.Error("Expected Ok to be true")
	}
	if logEvent.Epoch != 100 {
		t.Errorf("Expected Epoch 100, got %d", logEvent.Epoch)
	}
	if logEvent.Tick != 1000 {
		t.Errorf("Expected Tick 1000, got %d", logEvent.Tick)
	}
	if logEvent.Id != 12345 {
		t.Errorf("Expected Id 12345, got %d", logEvent.Id)
	}
	if logEvent.Hash != "" {
		t.Errorf("Expected Hash to be empty, got %s", logEvent.Hash)
	}
}

func TestLogEvent_JSONUnmarshalInvalidData(t *testing.T) {
	jsonData := []byte(`{
		"ok": "not a boolean",
		"epoch": "not a number"
	}`)

	var logEvent LogEvent
	err := json.Unmarshal(jsonData, &logEvent)
	if err == nil {
		t.Fatal("Expected error for invalid JSON data, got nil")
	}
}

func TestLogEvent_JSONMarshalEmptyStruct(t *testing.T) {
	logEvent := LogEvent{}

	jsonData, err := json.Marshal(logEvent)
	if err != nil {
		t.Fatalf("Failed to marshal empty LogEvent: %v", err)
	}

	var unmarshaled LogEvent
	err = json.Unmarshal(jsonData, &unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal empty LogEvent: %v", err)
	}

	if unmarshaled.Ok != false {
		t.Error("Expected Ok to be false for empty struct")
	}
	if unmarshaled.Epoch != 0 {
		t.Errorf("Expected Epoch 0, got %d", unmarshaled.Epoch)
	}
	if unmarshaled.Tick != 0 {
		t.Errorf("Expected Tick 0, got %d", unmarshaled.Tick)
	}
}
