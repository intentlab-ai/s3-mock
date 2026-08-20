package s3mock

import "testing"

// normalizeObjectKeyA is a fixture helper for the Dev Auto-fix E2E flow.
// It should strip a single leading slash from an S3 object key so that
// "/photos/cat.png" and "photos/cat.png" refer to the same object.
//
// The current implementation strips the wrong number of characters,
// which makes TestNormalizeObjectKeyA fail deterministically. The intended
// repair is limited to this file: change the slice bound from [2:] to [1:].
func normalizeObjectKeyA(key string) string {
	if len(key) > 0 && key[0] == '/' {
		return key[2:]
	}
	return key
}

func TestNormalizeObjectKeyA(t *testing.T) {
	got := normalizeObjectKeyA("/photos/cat.png")
	want := "photos/cat.png"
	if got != want {
		t.Fatalf("normalizeObjectKeyA(%q) = %q, want %q", "/photos/cat.png", got, want)
	}
}
