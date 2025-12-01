package s3mock

import (
	"crypto/md5"
	"encoding/hex"
)

func quote(s string) string {
	return `"` + s + `"`
}

func computeETag(b []byte) string {
	// S3 ETag for non-multipart uploads is MD5
	h := md5.Sum(b) //nolint:gosec // MD5 is what S3 uses for ETags
	return hex.EncodeToString(h[:])
}
