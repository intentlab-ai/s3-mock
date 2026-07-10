package s3mock

import "strings"

// StagingE2EMarker returns value with leading and trailing whitespace removed.
// It exists to support staging PR Agent end-to-end validation.
func StagingE2EMarker(value string) string {
	return strings.TrimSpace(value)
}
