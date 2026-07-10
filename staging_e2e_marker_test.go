package s3mock

import "testing"

func TestStagingE2EMarker(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "empty", value: "", want: ""},
		{name: "no whitespace", value: "hello", want: "hello"},
		{name: "leading and trailing spaces", value: "  hello  ", want: "hello"},
		{name: "tabs and newlines", value: "\t\nhello\n\t", want: "hello"},
		{name: "inner whitespace preserved", value: "  a b  ", want: "a b"},
		{name: "only whitespace", value: "   \t\n ", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StagingE2EMarker(tt.value); got != tt.want {
				t.Errorf("StagingE2EMarker(%q) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}
