package s3mock

import "testing"

// etagQuoteB is a fixture helper for the Dev Auto-fix E2E flow.
// It should wrap a raw ETag hex digest in double quotes, matching the
// way S3 returns ETags (e.g. `"d41d8cd98f00b204e9800998ecf8427e"`).
//
// The current implementation only adds the leading quote and omits the
// trailing one, which makes TestETagQuoteB fail deterministically. The
// intended repair is limited to this file: append the trailing `"`.
func etagQuoteB(digest string) string {
	return `"` + digest
}

func TestETagQuoteB(t *testing.T) {
	got := etagQuoteB("d41d8cd98f00b204e9800998ecf8427e")
	want := `"d41d8cd98f00b204e9800998ecf8427e"`
	if got != want {
		t.Fatalf("etagQuoteB(%q) = %q, want %q", "d41d8cd98f00b204e9800998ecf8427e", got, want)
	}
}
