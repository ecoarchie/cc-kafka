package api

import (
	"testing"
)

func TestFromBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected NullableString
	}{
		{
			name:     "Valid string",
			input:    []byte{0, 5, 'H', 'e', 'l', 'l', 'o'},
			expected: "Hello",
		},
		{
			name:     "Null string (-1 size)",
			input:    []byte{0, 0},
			expected: "",
		},
		{
			name:     "kafka-cli",
			input:    []byte{0, 9, 'k', 'a', 'f', 'k', 'a', '-', 'c', 'l', 'i'},
			expected: "kafka-cli",
		},
		{
			name:     "Empty string",
			input:    []byte{0, 0, 0},
			expected: "",
		},
		{
			name:     "String with size 1",
			input:    []byte{0, 1, 'A'},
			expected: "A",
		},
		{
			name:     "String with size 2",
			input:    []byte{0, 2, 'A', 'B'},
			expected: "AB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ns NullableString
			result := ns.FromBytes(tt.input)

			if result != tt.expected {
				t.Errorf("FromBytes() = %v, want %v", result, tt.expected)
			}
		})
	}
}
