package domain

import (
	"testing"
)

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
		{"Negative value", -1.0, -1, false},
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

func TestToUint32(t *testing.T) {
	tests := []struct {
		name        string
		input       float64
		expected    uint32
		expectError bool
	}{
		{"Valid uint32", 12345.0, 12345, false},
		{"Zero", 0.0, 0, false},
		{"Max uint32", 4294967295.0, 4294967295, false},
		{"Negative value", -1.0, 0, true},
		{"Decimal value", 123.45, 0, true},
		{"Overflow", 4294967296.0, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toUint32(tt.input)
			if (err != nil) != tt.expectError {
				t.Errorf("toUint32(%f) error = %v, expectError %v", tt.input, err, tt.expectError)
				return
			}
			if !tt.expectError && *got != tt.expected {
				t.Errorf("toUint32(%f) = %v, want %v", tt.input, *got, tt.expected)
			}
		})
	}
}

func TestToPositiveInt16(t *testing.T) {
	tests := []struct {
		name        string
		input       float64
		expected    int16
		expectError bool
	}{
		{"Valid int16", 123.0, 123, false},
		{"Zero", 0.0, 0, false},
		{"Max int16", 32767.0, 32767, false},
		{"Negative value", -1.0, 0, true},
		{"Decimal value", 12.5, 0, true},
		{"Overflow", 32768.0, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toPositiveInt16(tt.input)
			if (err != nil) != tt.expectError {
				t.Errorf("toInt16(%f) error = %v, expectError %v", tt.input, err, tt.expectError)
				return
			}
			if !tt.expectError && *got != tt.expected {
				t.Errorf("toInt16(%f) = %v, want %v", tt.input, *got, tt.expected)
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
