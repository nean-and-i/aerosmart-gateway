package serial

import (
	"testing"
	"time"
)

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

// The bus is shared with another device and carries no checksum, so a
// collision can splice two frames or corrupt bytes while the prefix still
// matches. Only a frame of exactly "<node> <register> <integer>" is trusted.
func TestParseResponseRejectsCorruptedFrames(t *testing.T) {
	tests := []struct {
		name      string
		response  string
		wantValue string
		wantOk    bool
	}{
		{"two frames spliced by a lost newline", "130 1067 3 130 5003 4", "", false},
		{"trailing garbage field", "130 1067 3 extra", "", false},
		{"garbage glued to the value", "130 1067 3x", "", false},
		{"sign that the unit never sends", "130 1067 +3", "", false},
		{"control byte inside the value", "130 1067 2\x001", "", false},
		{"non-ASCII byte inside the value", "130 1067 2\xff1", "", false},
		{"decimal point", "130 1067 2.5", "", false},
		{"negative value", "130 1067 -1250", "-1250", true},
		{"line noise before the frame", "\x00\xfe130 1067 3", "3", true},
		{"line noise after the frame", "130 1067 3\x00", "3", true},
		{"leading zero", "130 1067 03", "03", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, ok := ParseResponse(tt.response, "130 1067")
			if ok != tt.wantOk || value != tt.wantValue {
				t.Errorf("ParseResponse(%q) = (%q, %v), want (%q, %v)",
					tt.response, value, ok, tt.wantValue, tt.wantOk)
			}
		})
	}
}

// A ReopenBackoff configured above the 30s default must not make the
// supervisor's doubling loop shrink on its first step: applyDefaults must
// never leave MaxReopenBackoff below ReopenBackoff.
func TestApplyDefaultsKeepsMaxReopenBackoffAboveReopenBackoff(t *testing.T) {
	cfg := &Config{ReopenBackoff: time.Minute}
	cfg.applyDefaults(nopLogger{})

	if cfg.MaxReopenBackoff < cfg.ReopenBackoff {
		t.Errorf("MaxReopenBackoff = %v, want at least ReopenBackoff (%v)", cfg.MaxReopenBackoff, cfg.ReopenBackoff)
	}
}
