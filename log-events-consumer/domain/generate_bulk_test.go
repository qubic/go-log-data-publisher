//go:build log_reconstruction

// This file is only meant as a utility to transform the kafka to elastic format for manually ingesting log-events

package domain

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestGenerateBulk reads Kafka EventMessages (NDJSON produced by the producer
// stage), runs the REAL consumer transform pipeline (the same steps as
// consume.consumeBatch), and writes an Elastic _bulk NDJSON body.
//
//	IN=/home/linckode/asdasdasdasd/kafkaMessages.ndjson \
//	OUT=/home/linckode/asdasdasdasd/bulk.ndjson \
//	INDEX=qubic-event-logs-write \
//	  go test ./domain/ -run TestGenerateBulk -v
func TestGenerateBulk(t *testing.T) {
	inPath := "/path/to/file.json"
	outPath := "/path/to/file.ndjson"
	indexName := os.Getenv("INDEX")

	if indexName == "" {
		indexName = "qubic-event-logs-write"
	}

	f, err := os.Open(inPath)
	if err != nil {
		t.Fatalf("opening input: %v", err)
	}
	defer f.Close()

	// supported map: only type present in this dataset is QU_TRANSFER (0)
	supported := ParseSupportedTypes(`{"0":[0,1,2,3,4,5,6,8,9,10,11,12,13,14,15,255]}`)

	var bulk strings.Builder
	indexed, total := 0, 0
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		total++

		var ptr LogEventPtr
		if err := json.Unmarshal([]byte(line), &ptr); err != nil {
			t.Fatalf("record %d: unmarshalling LogEventPtr: %v", total, err)
		}
		logEvent, err := ptr.ToLogEvent()
		if err != nil {
			t.Fatalf("record %d: ToLogEvent: %v", total, err)
		}
		if !logEvent.IsSupported(supported) {
			t.Logf("record %d (logId %d): skipped (unsupported type)", total, logEvent.LogId)
			continue
		}
		lee, err := logEvent.ToLogEventElastic()
		if err != nil {
			t.Fatalf("record %d: ToLogEventElastic: %v", total, err)
		}
		if !lee.IsSupported() {
			t.Logf("record %d (logId %d): skipped (elastic filter)", total, logEvent.LogId)
			continue
		}
		val, err := json.Marshal(lee)
		if err != nil {
			t.Fatalf("record %d: marshalling elastic doc: %v", total, err)
		}

		// doc _id = <epoch>-<logId>, exactly as consume.consumeBatch builds it
		docID := strconv.FormatUint(uint64(logEvent.Epoch), 10) + "-" + strconv.FormatUint(logEvent.LogId, 10)
		action, _ := json.Marshal(map[string]any{
			"index": map[string]any{"_index": indexName, "_id": docID},
		})

		bulk.Write(action)
		bulk.WriteByte('\n')
		bulk.Write(val)
		bulk.WriteByte('\n')
		indexed++
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanning input: %v", err)
	}

	t.Logf("indexed %d / %d records", indexed, total)
	if outPath != "" {
		if err := os.WriteFile(outPath, []byte(bulk.String()), 0o644); err != nil {
			t.Fatalf("writing output: %v", err)
		}
		t.Logf("wrote bulk body to %s", outPath)
	} else {
		t.Logf("\n%s", bulk.String())
	}
}

func ParseSupportedTypes(mapStr string) map[uint64][]int16 {
	fmt.Printf("main: supported log types input: %s\n", mapStr)
	var logTypes map[uint64][]int16
	if err := json.Unmarshal([]byte(mapStr), &logTypes); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("main: parsed supported log types: %v\n", logTypes)
	return logTypes
}
