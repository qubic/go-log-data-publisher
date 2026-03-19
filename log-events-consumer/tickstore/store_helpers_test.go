package tickstore

import (
	"testing"
)

func TestIsCompletedTick(t *testing.T) {
	tests := []struct {
		name      string
		total     uint64
		processed uint64
		skipped   uint64
		want      bool
	}{
		{"not started", 0, 0, 0, false},
		{"partially processed", 10, 5, 0, false},
		{"partially skipped", 10, 0, 5, false},
		{"partially processed and skipped", 10, 5, 4, false},
		{"completed by processed", 10, 10, 0, true},
		{"completed by skipped", 10, 0, 10, true},
		{"completed by both", 10, 5, 5, true},
		{"over completed", 10, 6, 5, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isCompletedTick(tt.total, tt.processed, tt.skipped); got != tt.want {
				t.Errorf("isCompletedTick(%v, %v, %v) = %v, want %v", tt.total, tt.processed, tt.skipped, got, tt.want)
			}
		})
	}
}

func TestIsNewHighestTick(t *testing.T) {
	tests := []struct {
		name                 string
		tickNumber           uint64
		newHighestTickNumber uint64
		storedTickNumber     uint64
		want                 bool
	}{
		{"first tick", 100, 0, 0, true},
		{"greater than both", 100, 90, 80, true},
		{"less than new highest", 80, 90, 70, false},
		{"equal to new highest", 90, 90, 70, false},
		{"equal to stored", 90, 0, 90, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isNewHighestTick(tt.tickNumber, tt.newHighestTickNumber, tt.storedTickNumber); got != tt.want {
				t.Errorf("isNewHighestTick(%v, %v, %v) = %v, want %v", tt.tickNumber, tt.newHighestTickNumber, tt.storedTickNumber, got, tt.want)
			}
		})
	}
}

func TestParseNumericField(t *testing.T) {
	tests := []struct {
		name    string
		val     any
		want    uint64
		wantErr bool
	}{
		{"nil value", nil, 0, false},
		{"valid number string", "123", 123, false},
		{"zero string", "0", 0, false},
		{"negative number string", "-123", 0, true},
		{"invalid string", "abc", 0, true},
		{"not a string", 123, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePositiveNumericField(tt.val)
			if (err != nil) != tt.wantErr {
				t.Errorf("parsePositiveNumericField(%v) error = %v, wantErr %v", tt.val, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parsePositiveNumericField(%v) = %v, want %v", tt.val, got, tt.want)
			}
		})
	}
}
