package serial

import "testing"

func TestParseResponse(t *testing.T) {
	tests := []struct {
		name            string
		response        string
		expectedCommand string
		expectedValue   string
		expectedOk      bool
	}{
		{
			name:            "valid response",
			response:        "130 1067 3",
			expectedCommand: "130 1067",
			expectedValue:   "3",
			expectedOk:      true,
		},
		{
			name:            "valid response with different values",
			response:        "130 5003 2",
			expectedCommand: "130 5003",
			expectedValue:   "2",
			expectedOk:      true,
		},
		{
			name:            "invalid command",
			response:        "130 1067 3",
			expectedCommand: "130 2000",
			expectedValue:   "",
			expectedOk:      false,
		},
		{
			name:            "response too short",
			response:        "130 1067",
			expectedCommand: "130 1067",
			expectedValue:   "",
			expectedOk:      false,
		},
		{
			name:            "empty response",
			response:        "",
			expectedCommand: "130 1067",
			expectedValue:   "",
			expectedOk:      false,
		},
		{
			name:            "temperature response",
			response:        "130 201 21500",
			expectedCommand: "130 201",
			expectedValue:   "21500",
			expectedOk:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, ok := ParseResponse(tt.response, tt.expectedCommand)
			if ok != tt.expectedOk {
				t.Errorf("ParseResponse(%q, %q) ok = %v, want %v",
					tt.response, tt.expectedCommand, ok, tt.expectedOk)
			}
			if value != tt.expectedValue {
				t.Errorf("ParseResponse(%q, %q) = %q, want %q",
					tt.response, tt.expectedCommand, value, tt.expectedValue)
			}
		})
	}
}

func TestParseResponseEdgeCases(t *testing.T) {
	// Test with more than 3 parts - should get the last value
	value, ok := ParseResponse("130 1067 3 extra", "130 1067")
	if !ok {
		t.Error("Expected true for response with extra parts")
	}
	if value != "3" {
		t.Errorf("Expected value 3, got %s", value)
	}
}
