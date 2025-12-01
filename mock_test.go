package s3mock

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertHTTPStatusCode checks that the error contains the expected HTTP status code
func assertHTTPStatusCode(t *testing.T, err error, expectedStatus int) {
	t.Helper()

	var httpErr interface{ HTTPStatusCode() int }
	require.True(t, errors.As(err, &httpErr), "error should contain HTTP status code")

	assert.Equal(t, expectedStatus, httpErr.HTTPStatusCode(),
		"expected HTTP status %d (%s), got %d",
		expectedStatus, http.StatusText(expectedStatus), httpErr.HTTPStatusCode())
}

// assertS3Error validates that the error is properly formatted as S3 XML error with expected code
func assertS3Error(t *testing.T, err error, expectedCode string, expectedStatus int) {
	t.Helper()

	// First verify HTTP status code
	assertHTTPStatusCode(t, err, expectedStatus)

	// Then check the error code if the SDK can parse it
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		// The SDK successfully parsed the error, validate the code
		assert.Equal(t, expectedCode, apiErr.ErrorCode(),
			"Expected error code %q but got %q - XML may not be parsed correctly",
			expectedCode, apiErr.ErrorCode())
	} else {
		t.Errorf("Expected error to be parseable as smithy.APIError to validate error code %q", expectedCode)
	}
}

func TestConditionalPut(t *testing.T) {
	bucket := "test-bucket"
	key := "foo.txt"
	contentV1 := []byte("v1")
	etagV1 := computeETag(contentV1)

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket and initial object
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(contentV1),
	})
	require.NoError(t, err, "PutObject v1 should succeed")

	// Conditional update with If-Match (should succeed)
	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket:  &bucket,
		Key:     &key,
		Body:    bytes.NewReader([]byte("v2")),
		IfMatch: aws.String(quote(etagV1)),
	})
	require.NoError(t, err, "PutObject v2 with If-Match should succeed")

	// Conditional update with wrong If-Match (should fail)
	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket:  &bucket,
		Key:     &key,
		Body:    bytes.NewReader([]byte("v3")),
		IfMatch: aws.String(quote(etagV1)),
	})
	assertS3Error(t, err, "PreconditionFailed", http.StatusPreconditionFailed)
}

func TestHeadObject(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"
	content := []byte("hello world")

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket and object
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err, "PutObject should succeed")

	// Get expected ETag
	eTag := quote(computeETag(content))

	// Head object
	headResp, err := client.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	require.NoError(t, err, "HeadObject should succeed")

	// Verify response metadata
	assert.Equal(t, eTag, aws.ToString(headResp.ETag), "ETag should match")
	assert.NotNil(t, headResp.LastModified, "LastModified should be set")
	assert.False(t, headResp.LastModified.IsZero(), "LastModified should not be zero")
}

func TestGetIfNoneMatch(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"
	content := []byte("content")

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket and object
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err, "PutObject should succeed")

	etag := quote(computeETag(content))

	// Get with If-None-Match matching the current ETag (should return 304)
	_, err = client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		IfNoneMatch: aws.String(etag),
	})

	// 304 Not Modified is a special case - it's not an error code in S3's XML sense
	assertHTTPStatusCode(t, err, http.StatusNotModified)

	// Get with If-None-Match with different ETag (should succeed)
	getResp, err := client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		IfNoneMatch: aws.String(`"different-etag"`),
	})
	require.NoError(t, err, "GetObject with different If-None-Match should succeed")
	defer getResp.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(getResp.Body)
	require.NoError(t, err, "reading response body should succeed")
	assert.Equal(t, content, body, "content should match")
}

func TestGetIfMatch(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"
	content := []byte("content")

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket and object
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err, "PutObject should succeed")

	eTag := quote(computeETag(content))

	// Get with If-Match matching the current ETag (should succeed)
	getResp, err := client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket:  &bucket,
		Key:     &key,
		IfMatch: aws.String(eTag),
	})
	require.NoError(t, err, "GetObject with matching If-Match should succeed")
	defer getResp.Body.Close() //nolint:errcheck

	// Get with If-Match with wrong ETag (should fail)
	_, err = client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket:  &bucket,
		Key:     &key,
		IfMatch: aws.String(`"wrong-etag"`),
	})
	assertS3Error(t, err, "PreconditionFailed", http.StatusPreconditionFailed)
}

func TestDeleteObject(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket and object
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader([]byte("content")),
	})
	require.NoError(t, err, "PutObject should succeed")

	// Delete object
	_, err = client.DeleteObject(t.Context(), &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	require.NoError(t, err, "DeleteObject should succeed")

	// Verify object no longer exists
	// HEAD errors return generic HTTP status codes without error details
	_, err = client.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	assertHTTPStatusCode(t, err, http.StatusNotFound)

	// Delete again (should still succeed - idempotent)
	_, err = client.DeleteObject(t.Context(), &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	require.NoError(t, err, "DeleteObject second time should succeed (idempotent)")
}

func TestDeleteNotExisting(t *testing.T) {
	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	bucket := "test-bucket"
	key := "test.txt"

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Delete non-existent object (should succeed)
	_, err = client.DeleteObject(t.Context(), &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	require.NoError(t, err, "DeleteObject for non-existent object should succeed")
}

func TestDeleteIfMatch(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"
	content := []byte("content")

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket and object
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err, "PutObject should succeed")

	// Get with If-Match with wrong ETag (should fail)
	_, err = client.DeleteObject(t.Context(), &s3.DeleteObjectInput{
		Bucket:  &bucket,
		Key:     &key,
		IfMatch: aws.String(`"wrong-etag"`),
	})
	assertS3Error(t, err, "PreconditionFailed", http.StatusPreconditionFailed)

	// Get with If-Match matching the current ETag (should succeed)
	eTag := quote(computeETag(content))
	_, err = client.DeleteObject(t.Context(), &s3.DeleteObjectInput{
		Bucket:  &bucket,
		Key:     &key,
		IfMatch: aws.String(eTag),
	})
	require.NoError(t, err, "DeleteObject with matching If-Match should succeed")
}

func TestPutIfNoneMatch(t *testing.T) {
	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	bucket := "test-bucket"
	key := "test.txt"
	contentV1 := []byte("v1")

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Put with If-None-Match="*" should succeed for new object
	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		Body:        bytes.NewReader(contentV1),
		IfNoneMatch: aws.String("*"),
	})
	require.NoError(t, err, "PutObject with If-None-Match=* for new object should succeed")

	// Put with If-None-Match matching current ETag should fail
	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		Body:        bytes.NewReader([]byte("v2")),
		IfNoneMatch: aws.String("*"),
	})
	assertS3Error(t, err, "PreconditionFailed", http.StatusPreconditionFailed)
}

func TestGetNonExistent(t *testing.T) {
	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	bucket := "test-bucket"
	key := "nonexistent.txt"

	// Create bucket (but no object)
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Get non-existent object
	_, err = client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	assertS3Error(t, err, "NoSuchKey", http.StatusNotFound)
}

func TestGetObjectAttributes(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"
	content := []byte("hello world")
	hash := sha256.Sum256(content)
	checksumSHA256 := base64.StdEncoding.EncodeToString(hash[:])

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket:         &bucket,
		Key:            &key,
		Body:           bytes.NewReader(content),
		ChecksumSHA256: aws.String(checksumSHA256),
	})
	require.NoError(t, err, "PutObject should succeed")

	// Compute expected ETag
	eTag := quote(computeETag(content))

	// Get object attributes (including LastModified)
	attrResp, err := client.GetObjectAttributes(t.Context(), &s3.GetObjectAttributesInput{
		Bucket: &bucket,
		Key:    &key,
		ObjectAttributes: []types.ObjectAttributes{
			types.ObjectAttributesEtag,
			types.ObjectAttributesObjectSize,
			types.ObjectAttributesStorageClass,
			types.ObjectAttributesChecksum,
		},
	})
	require.NoError(t, err, "GetObjectAttributes should succeed")

	// Verify ETag matches expected
	assert.Equal(t, eTag, aws.ToString(attrResp.ETag), "ETag should match")

	// Verify ObjectSize matches content length
	require.NotNil(t, attrResp.ObjectSize, "ObjectSize should be set")
	assert.Equal(t, int64(len(content)), *attrResp.ObjectSize, "ObjectSize should match content length")

	// Verify LastModified is set (returned as header, not in XML body)
	require.NotNil(t, attrResp.LastModified, "LastModified should be set from response header")
	assert.False(t, attrResp.LastModified.IsZero(), "LastModified should not be zero")

	// Verify checksum is returned
	require.NotNil(t, attrResp.Checksum, "Checksum should be present")
	require.NotNil(t, attrResp.Checksum.ChecksumSHA256, "ChecksumSHA256 should be present")
	assert.Equal(t, checksumSHA256, aws.ToString(attrResp.Checksum.ChecksumSHA256), "ChecksumSHA256 should match")
}

func TestGetObjectAttributesSingleAttribute(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"
	content := []byte("hello world")

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err, "PutObject should succeed")

	// Compute expected ETag
	eTag := quote(computeETag(content))

	// Get object attributes with only ETag attribute
	attrResp, err := client.GetObjectAttributes(t.Context(), &s3.GetObjectAttributesInput{
		Bucket: &bucket,
		Key:    &key,
		ObjectAttributes: []types.ObjectAttributes{
			types.ObjectAttributesEtag,
		},
	})
	require.NoError(t, err, "GetObjectAttributes should succeed")

	// Verify ETag matches expected
	assert.Equal(t, eTag, aws.ToString(attrResp.ETag), "ETag should match")

	// Verify only ETag is returned, other attributes should be nil/empty
	assert.Nil(t, attrResp.ObjectSize, "ObjectSize should not be set when not requested")
	assert.Nil(t, attrResp.Checksum, "Checksum should not be set when not requested")
	assert.Empty(t, attrResp.StorageClass, "StorageClass should not be set when not requested")
}

func TestGetObjectAttributesNonExistent(t *testing.T) {
	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	bucket := "test-bucket"
	key := "nonexistent.txt"

	// Create bucket (but no object)
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Get attributes for non-existent object
	_, err = client.GetObjectAttributes(t.Context(), &s3.GetObjectAttributesInput{
		Bucket: &bucket,
		Key:    &key,
		ObjectAttributes: []types.ObjectAttributes{
			types.ObjectAttributesEtag,
		},
	})
	assertS3Error(t, err, "NoSuchKey", http.StatusNotFound)
}

func TestListObjectsV2(t *testing.T) {
	bucket := "test-bucket"
	testFiles := map[string][]byte{
		"file1.txt":        []byte("content1"),
		"file2.txt":        []byte("content2"),
		"subdir/file3.txt": []byte("content3"),
	}

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Put multiple objects
	for key, content := range testFiles {
		_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &key,
			Body:   bytes.NewReader(content),
		})
		require.NoError(t, err, "PutObject %s should succeed", key)
	}

	// List all objects
	listResp, err := client.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
		Bucket: &bucket,
	})
	require.NoError(t, err, "ListObjectsV2 should succeed")

	assert.Equal(t, bucket, aws.ToString(listResp.Name), "Bucket name should match")
	require.NotNil(t, listResp.KeyCount, "KeyCount should be set")
	assert.Equal(t, int32(len(testFiles)), *listResp.KeyCount, "Should have %d objects", len(testFiles))
	assert.Len(t, listResp.Contents, len(testFiles), "Should have %d contents", len(testFiles))

	// Verify all keys are present and metadata matches
	for _, obj := range listResp.Contents {
		key := aws.ToString(obj.Key)
		content, exists := testFiles[key]
		require.True(t, exists, "Key %s should be in test files", key)

		assert.NotNil(t, obj.ETag, "ETag should be set")
		assert.NotNil(t, obj.LastModified, "LastModified should be set")
		assert.NotNil(t, obj.Size, "Size should be set")

		// Verify object metadata
		assert.Equal(t, quote(computeETag(content)), aws.ToString(obj.ETag), "ETag should match for %s", key)
		assert.Equal(t, int64(len(content)), aws.ToInt64(obj.Size), "Size should match for %s", key)
	}
}

func TestListObjectsV2WithPrefix(t *testing.T) {
	bucket := "test-bucket"
	testFiles := map[string][]byte{
		"file1.txt":        []byte("content1"),
		"file2.txt":        []byte("content2"),
		"subdir/file3.txt": []byte("content3"),
	}

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket and objects
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	for key, content := range testFiles {
		_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &key,
			Body:   bytes.NewReader(content),
		})
		require.NoError(t, err, "PutObject %s should succeed", key)
	}

	// List objects with prefix
	prefix := "subdir/"
	listResp, err := client.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})
	require.NoError(t, err, "ListObjectsV2 with prefix should succeed")

	require.NotNil(t, listResp.KeyCount, "KeyCount should be set")
	assert.Equal(t, int32(1), *listResp.KeyCount, "Should have 1 object with prefix")
	assert.Len(t, listResp.Contents, 1, "Should have 1 content")

	// Verify the returned key matches expected key with prefix
	expectedKey := "subdir/file3.txt"
	assert.Equal(t, expectedKey, aws.ToString(listResp.Contents[0].Key), "Should match prefix")

	// Verify the key exists in testFiles
	_, exists := testFiles[expectedKey]
	assert.True(t, exists, "Key should be in test files")
}

func TestListObjectsV2EmptyBucket(t *testing.T) {
	bucket := "test-bucket"

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create empty bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// List objects in empty bucket
	listResp, err := client.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
		Bucket: &bucket,
	})
	require.NoError(t, err, "ListObjectsV2 should succeed")

	require.NotNil(t, listResp.KeyCount, "KeyCount should be set")
	assert.Equal(t, int32(0), *listResp.KeyCount, "Should have 0 objects")
	assert.Len(t, listResp.Contents, 0, "Should have no contents")
}

func TestListObjectsV2NonExistentBucket(t *testing.T) {
	bucket := "nonexistent-bucket"

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// List objects in non-existent bucket
	_, err = client.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
		Bucket: &bucket,
	})
	assertS3Error(t, err, "NoSuchBucket", http.StatusNotFound)
}

func TestGetObject(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"
	content := []byte("hello world")

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket and object
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err, "PutObject should succeed")

	// Get object
	getResp, err := client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	require.NoError(t, err, "GetObject should succeed")
	defer getResp.Body.Close() //nolint:errcheck

	// Verify content
	body, err := io.ReadAll(getResp.Body)
	require.NoError(t, err, "reading response body should succeed")
	assert.Equal(t, content, body, "content should match")

	// Verify response metadata
	eTag := quote(computeETag(content))
	assert.Equal(t, eTag, aws.ToString(getResp.ETag), "ETag should match")
	assert.NotNil(t, getResp.LastModified, "LastModified should be set")
	assert.False(t, getResp.LastModified.IsZero(), "LastModified should not be zero")
	assert.NotNil(t, getResp.ContentLength, "ContentLength should be set")
	assert.Equal(t, int64(len(content)), *getResp.ContentLength, "ContentLength should match")
}

func TestGetObjectNonExistent(t *testing.T) {
	bucket := "test-bucket"
	key := "nonexistent.txt"

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket (but no object)
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	_, err = client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	assertS3Error(t, err, "NoSuchKey", http.StatusNotFound)
}

func TestGetObjectIfMatch(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"
	content := []byte("content")

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket and object
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err, "PutObject should succeed")

	eTag := quote(computeETag(content))

	// Get with If-Match matching the current ETag (should succeed)
	headResp, err := client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket:  &bucket,
		Key:     &key,
		IfMatch: aws.String(eTag),
	})
	require.NoError(t, err, "GetObject with matching If-Match should succeed")
	assert.Equal(t, eTag, aws.ToString(headResp.ETag), "ETag should match")

	// Get If-Match not matching the current ETag (should fail)
	_, err = client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket:  &bucket,
		Key:     &key,
		IfMatch: aws.String("wrong-etag"),
	})
	assertS3Error(t, err, "PreconditionFailed", http.StatusPreconditionFailed)
}

func TestGetObjectIfNoneMatch(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"
	content := []byte("content")

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket and object
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err, "PutObject should succeed")

	etag := quote(computeETag(content))

	// Get with If-None-Match matching the current ETag (should return 304)
	_, err = client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		IfNoneMatch: aws.String(etag),
	})
	assertHTTPStatusCode(t, err, http.StatusNotModified)

	// Head with If-None-Match with different ETag (should succeed)
	headResp, err := client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		IfNoneMatch: aws.String(`"different-etag"`),
	})
	require.NoError(t, err, "GetObject with different If-None-Match should succeed")
	assert.Equal(t, etag, aws.ToString(headResp.ETag), "ETag should match")
}

func TestHeadObjectNonExistent(t *testing.T) {
	bucket := "test-bucket"
	key := "nonexistent.txt"

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket (but no object)
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Head non-existent object
	// HEAD errors return generic HTTP status codes without error details
	_, err = client.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	assertHTTPStatusCode(t, err, http.StatusNotFound)
}

func TestHeadObjectIfMatch(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"
	content := []byte("content")

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket and object
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err, "PutObject should succeed")

	eTag := quote(computeETag(content))

	// Head with If-Match matching the current ETag (should succeed)
	headResp, err := client.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket:  &bucket,
		Key:     &key,
		IfMatch: aws.String(eTag),
	})
	require.NoError(t, err, "HeadObject with matching If-Match should succeed")
	assert.Equal(t, eTag, aws.ToString(headResp.ETag), "ETag should match")

	// Head with If-Match with wrong ETag (should fail)
	// HEAD errors return generic HTTP status codes without error details
	_, err = client.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket:  &bucket,
		Key:     &key,
		IfMatch: aws.String(`"different-etag"`),
	})
	assertHTTPStatusCode(t, err, http.StatusPreconditionFailed)
}

func TestHeadObjectIfNoneMatch(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"
	content := []byte("content")

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket and object
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err, "PutObject should succeed")

	etag := quote(computeETag(content))

	// Head with If-None-Match matching the current ETag (should return 304)
	_, err = client.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		IfNoneMatch: aws.String(etag),
	})
	assertHTTPStatusCode(t, err, http.StatusNotModified)

	// Head with If-None-Match with different ETag (should succeed)
	headResp, err := client.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		IfNoneMatch: aws.String(`"different-etag"`),
	})
	require.NoError(t, err, "HeadObject with different If-None-Match should succeed")
	assert.Equal(t, etag, aws.ToString(headResp.ETag), "ETag should match")
}

func TestPutObject(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"
	content := []byte("hello world")

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Put object (basic, no conditionals)
	putResp, err := client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err, "PutObject should succeed")

	// Verify response metadata
	expectedETag := quote(computeETag(content))
	assert.Equal(t, expectedETag, aws.ToString(putResp.ETag), "ETag should match")
	// Note: PutObjectOutput includes ETag; LastModified is available via GetObject/HeadObject

	// Verify object was stored correctly by getting it
	getResp, err := client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	require.NoError(t, err, "GetObject should succeed")
	defer getResp.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(getResp.Body)
	require.NoError(t, err, "reading response body should succeed")
	assert.Equal(t, content, body, "content should match")
}

func TestPutObjectNonExistentBucket(t *testing.T) {
	bucket := "nonexistent-bucket"
	key := "test.txt"

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Put object to non-existent bucket
	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader([]byte("content")),
	})
	assertS3Error(t, err, "NoSuchBucket", http.StatusNotFound)
}

func TestPutObjectWithChecksum(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"
	content := []byte("hello world")
	hash := sha256.Sum256(content)
	checksumSHA256 := base64.StdEncoding.EncodeToString(hash[:])

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Put object with checksum
	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket:         &bucket,
		Key:            &key,
		Body:           bytes.NewReader(content),
		ChecksumSHA256: aws.String(checksumSHA256),
	})
	require.NoError(t, err, "PutObject with checksum should succeed")

	// Verify checksum was stored by getting object attributes
	attrResp, err := client.GetObjectAttributes(t.Context(), &s3.GetObjectAttributesInput{
		Bucket: &bucket,
		Key:    &key,
		ObjectAttributes: []types.ObjectAttributes{
			types.ObjectAttributesChecksum,
		},
	})
	require.NoError(t, err, "GetObjectAttributes should succeed")
	require.NotNil(t, attrResp.Checksum, "Checksum should be present")
	require.NotNil(t, attrResp.Checksum.ChecksumSHA256, "ChecksumSHA256 should be present")
	assert.Equal(t, checksumSHA256, aws.ToString(attrResp.Checksum.ChecksumSHA256), "ChecksumSHA256 should match")
}

func TestListObjectsV2MaxKeys(t *testing.T) {
	bucket := "test-bucket"
	testFiles := map[string][]byte{
		"file1.txt": []byte("content"),
		"file2.txt": []byte("content"),
		"file3.txt": []byte("content"),
		"file4.txt": []byte("content"),
		"file5.txt": []byte("content"),
	}

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Put multiple objects
	for key, content := range testFiles {
		_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &key,
			Body:   bytes.NewReader(content),
		})
		require.NoError(t, err, "PutObject %s should succeed", key)
	}

	// List with max-keys=2
	maxKeys := int32(2)
	listResp, err := client.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
		Bucket:  &bucket,
		MaxKeys: &maxKeys,
	})
	require.NoError(t, err, "ListObjectsV2 should succeed")

	assert.Equal(t, int32(2), *listResp.KeyCount, "Should have 2 objects")
	assert.Len(t, listResp.Contents, 2, "Should have 2 contents")
	assert.True(t, aws.ToBool(listResp.IsTruncated), "Should be truncated")
	assert.NotEmpty(t, aws.ToString(listResp.NextContinuationToken), "Should have next continuation token")
}

func TestListObjectsV2StartAfter(t *testing.T) {
	bucket := "test-bucket"
	testFiles := map[string][]byte{
		"file1.txt": []byte("content"),
		"file2.txt": []byte("content"),
		"file3.txt": []byte("content"),
		"file4.txt": []byte("content"),
	}

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Put multiple objects
	for key, content := range testFiles {
		_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &key,
			Body:   bytes.NewReader(content),
		})
		require.NoError(t, err, "PutObject %s should succeed", key)
	}

	// List with start-after="file2.txt" (should return file3.txt and file4.txt)
	startAfter := "file2.txt"
	listResp, err := client.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
		Bucket:     &bucket,
		StartAfter: &startAfter,
	})
	require.NoError(t, err, "ListObjectsV2 with start-after should succeed")

	assert.Equal(t, int32(2), *listResp.KeyCount, "Should have 2 objects after start-after")
	assert.Len(t, listResp.Contents, 2, "Should have 2 contents")

	// Verify keys are file3.txt and file4.txt
	returnedKeys := make([]string, len(listResp.Contents))
	for i, obj := range listResp.Contents {
		returnedKeys[i] = aws.ToString(obj.Key)
		// Verify each returned key exists in testFiles
		_, exists := testFiles[returnedKeys[i]]
		assert.True(t, exists, "Returned key %s should be in test files", returnedKeys[i])
	}
	assert.Equal(t, []string{"file3.txt", "file4.txt"}, returnedKeys, "Should return keys after start-after")
}

func TestListObjectsV2Paginated(t *testing.T) {
	bucket := "test-bucket"
	// Create 10 objects to test pagination across multiple pages
	testFiles := map[string][]byte{
		"file01.txt": []byte("content1"),
		"file02.txt": []byte("content2"),
		"file03.txt": []byte("content3"),
		"file04.txt": []byte("content4"),
		"file05.txt": []byte("content5"),
		"file06.txt": []byte("content6"),
		"file07.txt": []byte("content7"),
		"file08.txt": []byte("content8"),
		"file09.txt": []byte("content9"),
		"file10.txt": []byte("content10"),
	}

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Put objects
	for key, content := range testFiles {
		_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &key,
			Body:   bytes.NewReader(content),
		})
		require.NoError(t, err, "PutObject %s should succeed", key)
	}

	// Test pagination with MaxKeys=3 (should result in 4 pages: 3, 3, 3, 1)
	maxKeys := int32(3)
	allKeys := make(map[string]bool)
	var continuationToken *string

	pageNum := 0
	for {
		pageNum++
		input := &s3.ListObjectsV2Input{
			Bucket:  &bucket,
			MaxKeys: &maxKeys,
		}
		if continuationToken != nil {
			input.ContinuationToken = continuationToken
		}

		listResp, err := client.ListObjectsV2(t.Context(), input)
		require.NoError(t, err, "ListObjectsV2 page %d should succeed", pageNum)

		// Verify page metadata
		require.NotNil(t, listResp.KeyCount, "KeyCount should be set on page %d", pageNum)
		assert.Equal(t, *listResp.KeyCount, int32(len(listResp.Contents)), "KeyCount should match Contents length on page %d", pageNum)

		// Collect all keys from this page
		for _, obj := range listResp.Contents {
			allKeys[aws.ToString(obj.Key)] = true
		}

		// Verify pagination state
		if pageNum < 4 {
			// First 3 pages should be truncated
			assert.True(t, aws.ToBool(listResp.IsTruncated), "Page %d should be truncated", pageNum)
			require.NotNil(t, listResp.NextContinuationToken, "Page %d should have NextContinuationToken", pageNum)
			assert.NotEmpty(t, aws.ToString(listResp.NextContinuationToken), "Page %d NextContinuationToken should not be empty", pageNum)
			continuationToken = listResp.NextContinuationToken
			continue
		}
		// Last page should not be truncated
		assert.False(t, aws.ToBool(listResp.IsTruncated), "Last page should not be truncated")
		assert.Nil(t, listResp.NextContinuationToken, "Last page should not have NextContinuationToken")
		break
	}

	// Verify we got exactly 4 pages
	assert.Equal(t, 4, pageNum, "Should have exactly 4 pages")

	// Verify all keys were returned across all pages
	assert.Len(t, allKeys, len(testFiles), "Should have all %d keys", len(testFiles))
	for key := range testFiles {
		assert.True(t, allKeys[key], "Key %s should be present in paginated results", key)
	}
}

func TestListObjectsV1(t *testing.T) {
	bucket := "test-bucket"
	testFiles := map[string][]byte{
		"file1.txt":        []byte("content1"),
		"file2.txt":        []byte("content2"),
		"subdir/file3.txt": []byte("content3"),
	}

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Put multiple objects
	for key, content := range testFiles {
		_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &key,
			Body:   bytes.NewReader(content),
		})
		require.NoError(t, err, "PutObject %s should succeed", key)
	}

	// List all objects using ListObjects v1
	listResp, err := client.ListObjects(t.Context(), &s3.ListObjectsInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "ListObjects should succeed")

	assert.Equal(t, bucket, aws.ToString(listResp.Name), "Bucket name should match")
	assert.Len(t, listResp.Contents, len(testFiles), "Should have %d contents", len(testFiles))

	// Verify all keys are present and metadata matches
	for _, obj := range listResp.Contents {
		key := aws.ToString(obj.Key)
		content, exists := testFiles[key]
		require.True(t, exists, "Key %s should be in test files", key)

		assert.NotNil(t, obj.ETag, "ETag should be set")
		assert.NotNil(t, obj.LastModified, "LastModified should be set")
		assert.NotNil(t, obj.Size, "Size should be set")
		assert.NotNil(t, obj.Owner, "Owner should be set (v1 always includes owner)")

		// Verify object metadata
		assert.Equal(t, quote(computeETag(content)), aws.ToString(obj.ETag), "ETag should match for %s", key)
		assert.Equal(t, int64(len(content)), aws.ToInt64(obj.Size), "Size should match for %s", key)
	}
}

func TestListObjectsV1WithPrefix(t *testing.T) {
	bucket := "test-bucket"
	testFiles := map[string][]byte{
		"file1.txt":        []byte("content1"),
		"file2.txt":        []byte("content2"),
		"subdir/file3.txt": []byte("content3"),
	}

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Put multiple objects
	for key, content := range testFiles {
		_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &key,
			Body:   bytes.NewReader(content),
		})
		require.NoError(t, err, "PutObject %s should succeed", key)
	}

	// List objects with prefix using ListObjects v1
	prefix := "subdir/"
	listResp, err := client.ListObjects(t.Context(), &s3.ListObjectsInput{
		Bucket: &bucket,
		Prefix: &prefix,
	})
	require.NoError(t, err, "ListObjects with prefix should succeed")

	assert.Len(t, listResp.Contents, 1, "Should have 1 content")
	assert.Equal(t, prefix, aws.ToString(listResp.Prefix), "Prefix should match")

	// Verify the returned key matches expected key with prefix
	expectedKey := "subdir/file3.txt"
	assert.Equal(t, expectedKey, aws.ToString(listResp.Contents[0].Key), "Should match prefix")

	// Verify the key exists in testFiles
	_, exists := testFiles[expectedKey]
	assert.True(t, exists, "Key should be in test files")
}

func TestListObjectsV1MaxKeys(t *testing.T) {
	bucket := "test-bucket"
	testFiles := map[string][]byte{
		"file1.txt": []byte("content"),
		"file2.txt": []byte("content"),
		"file3.txt": []byte("content"),
		"file4.txt": []byte("content"),
		"file5.txt": []byte("content"),
	}

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Put multiple objects
	for key, content := range testFiles {
		_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &key,
			Body:   bytes.NewReader(content),
		})
		require.NoError(t, err, "PutObject %s should succeed", key)
	}

	// List with max-keys=2 using ListObjects v1
	maxKeys := int32(2)
	listResp, err := client.ListObjects(t.Context(), &s3.ListObjectsInput{
		Bucket:  &bucket,
		MaxKeys: &maxKeys,
	})
	require.NoError(t, err, "ListObjects should succeed")

	assert.Equal(t, int32(2), *listResp.MaxKeys, "Should have maxKeys=2")
	assert.Len(t, listResp.Contents, 2, "Should have 2 contents")
	assert.True(t, aws.ToBool(listResp.IsTruncated), "Should be truncated")
	assert.NotEmpty(t, aws.ToString(listResp.NextMarker), "Should have next marker (v1 pagination)")
}

func TestListObjectsV1Paginated(t *testing.T) {
	bucket := "test-bucket"
	// Create 10 objects to test pagination across multiple pages
	testFiles := map[string][]byte{
		"file01.txt": []byte("content1"),
		"file02.txt": []byte("content2"),
		"file03.txt": []byte("content3"),
		"file04.txt": []byte("content4"),
		"file05.txt": []byte("content5"),
		"file06.txt": []byte("content6"),
		"file07.txt": []byte("content7"),
		"file08.txt": []byte("content8"),
		"file09.txt": []byte("content9"),
		"file10.txt": []byte("content10"),
	}

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Put objects
	for key, content := range testFiles {
		_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &key,
			Body:   bytes.NewReader(content),
		})
		require.NoError(t, err, "PutObject %s should succeed", key)
	}

	// Test pagination with MaxKeys=3 (should result in 4 pages: 3, 3, 3, 1)
	maxKeys := int32(3)
	allKeys := make(map[string]bool)
	var marker *string
	pageNum := 0

	for {
		pageNum++
		input := &s3.ListObjectsInput{
			Bucket:  &bucket,
			MaxKeys: &maxKeys,
		}
		if marker != nil {
			input.Marker = marker
		}

		listResp, err := client.ListObjects(t.Context(), input)
		require.NoError(t, err, "ListObjects page %d should succeed", pageNum)

		// Collect all keys from this page
		for _, obj := range listResp.Contents {
			allKeys[aws.ToString(obj.Key)] = true
		}

		// Verify pagination state
		if pageNum < 4 {
			// First 3 pages should be truncated
			assert.True(t, aws.ToBool(listResp.IsTruncated), "Page %d should be truncated", pageNum)
			require.NotNil(t, listResp.NextMarker, "Page %d should have NextMarker", pageNum)
			assert.NotEmpty(t, aws.ToString(listResp.NextMarker), "Page %d NextMarker should not be empty", pageNum)
			marker = listResp.NextMarker
			continue
		}
		// Last page should not be truncated
		assert.False(t, aws.ToBool(listResp.IsTruncated), "Last page should not be truncated")
		assert.Nil(t, listResp.NextMarker, "Last page should not have NextMarker")
		break
	}

	// Verify we got exactly 4 pages
	assert.Equal(t, 4, pageNum, "Should have exactly 4 pages")

	// Verify all keys were returned across all pages
	assert.Len(t, allKeys, len(testFiles), "Should have all %d keys", len(testFiles))
	for key := range testFiles {
		assert.True(t, allKeys[key], "Key %s should be present in paginated results", key)
	}
}

func TestListObjectsV1NonExistentBucket(t *testing.T) {
	bucket := "nonexistent-bucket"

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// List objects in non-existent bucket using ListObjects v1
	_, err = client.ListObjects(t.Context(), &s3.ListObjectsInput{
		Bucket: &bucket,
	})
	assertS3Error(t, err, "NoSuchBucket", http.StatusNotFound)
}

func TestCreateBucketDuplicate(t *testing.T) {
	bucket := "test-bucket"

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket first time
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Try to create bucket again (should fail)
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	assertS3Error(t, err, "BucketAlreadyExists", http.StatusConflict)
}

func TestDeleteObjectNonExistentBucket(t *testing.T) {
	bucket := "nonexistent-bucket"
	key := "test.txt"

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Delete object from non-existent bucket
	_, err = client.DeleteObject(t.Context(), &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	assertS3Error(t, err, "NoSuchBucket", http.StatusNotFound)
}

func TestDeleteBucket(t *testing.T) {
	bucket := "test-bucket"

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Delete empty bucket
	_, err = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "DeleteBucket should succeed")

	// Verify bucket no longer exists by trying to list objects
	_, err = client.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
		Bucket: &bucket,
	})
	assertS3Error(t, err, "NoSuchBucket", http.StatusNotFound)
}

func TestDeleteBucketNonExistent(t *testing.T) {
	bucket := "nonexistent-bucket"

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Try to delete non-existent bucket
	_, err = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{
		Bucket: &bucket,
	})
	assertS3Error(t, err, "NoSuchBucket", http.StatusNotFound)
}

func TestDeleteBucketNotEmpty(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Put an object in the bucket
	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader([]byte("content")),
	})
	require.NoError(t, err, "PutObject should succeed")

	// Try to delete bucket with objects (should fail)
	_, err = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{
		Bucket: &bucket,
	})
	assertS3Error(t, err, "BucketNotEmpty", http.StatusConflict)

	// Verify bucket still exists
	listResp, err := client.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
		Bucket: &bucket,
	})
	require.NoError(t, err, "ListObjectsV2 should succeed")
	assert.Equal(t, int32(1), *listResp.KeyCount, "Bucket should still have 1 object")
}

func TestDeleteBucketAfterEmptying(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Put an object in the bucket
	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader([]byte("content")),
	})
	require.NoError(t, err, "PutObject should succeed")

	// Try to delete bucket with objects (should fail)
	_, err = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{
		Bucket: &bucket,
	})
	assertS3Error(t, err, "BucketNotEmpty", http.StatusConflict)

	// Delete the object
	_, err = client.DeleteObject(t.Context(), &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	require.NoError(t, err, "DeleteObject should succeed")

	// Now delete the empty bucket (should succeed)
	_, err = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "DeleteBucket should succeed after emptying")

	// Verify bucket no longer exists
	_, err = client.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
		Bucket: &bucket,
	})
	assertS3Error(t, err, "NoSuchBucket", http.StatusNotFound)
}

func TestPutObjectIfMatchNonExistent(t *testing.T) {
	bucket := "test-bucket"
	key := "nonexistent.txt"

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Put object with If-Match on non-existent object (should fail)
	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket:  &bucket,
		Key:     &key,
		Body:    bytes.NewReader([]byte("content")),
		IfMatch: aws.String(`"some-etag"`),
	})
	assertS3Error(t, err, "PreconditionFailed", http.StatusPreconditionFailed)
}

func TestPutObjectIfNoneMatch(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"
	contentV1 := []byte("test text")

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Put initial object
	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		Body:        bytes.NewReader(contentV1),
		IfNoneMatch: aws.String("*"),
	})
	require.NoError(t, err, "PutObject should succeed")

	// Put again if-none-match, should fail
	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		Body:        bytes.NewReader([]byte("v2")),
		IfNoneMatch: aws.String("*"),
	})
	assertS3Error(t, err, "PreconditionFailed", http.StatusPreconditionFailed)

	// Put with If-None-Match with invalid value
	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		Body:        bytes.NewReader([]byte("v2")),
		IfNoneMatch: aws.String(`"different-etag"`),
	})
	assertS3Error(t, err, "BadRequest", http.StatusBadRequest)
}

func TestHeadObjectNonExistentBucket(t *testing.T) {
	bucket := "nonexistent-bucket"
	key := "test.txt"

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Head object in non-existent bucket
	// HEAD errors return generic HTTP status codes without error details
	_, err = client.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	assertHTTPStatusCode(t, err, http.StatusNotFound)
}

func TestGetObjectIfMatchNonExistent(t *testing.T) {
	bucket := "test-bucket"
	key := "nonexistent.txt"

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Get object with If-Match on non-existent object (should fail)
	_, err = client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket:  &bucket,
		Key:     &key,
		IfMatch: aws.String(`"some-etag"`),
	})
	assertS3Error(t, err, "NoSuchKey", http.StatusNotFound)
}

func TestGetObjectIfModified(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"
	content := []byte("original content")
	changed := []byte("modified content")

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket and object
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err, "PutObject should succeed")

	// Get object
	getResp, err := client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	require.NoError(t, err, "GetObject should succeed")
	defer getResp.Body.Close()

	// Get if modified, should fail
	_, err = client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket:          &bucket,
		Key:             &key,
		IfModifiedSince: getResp.LastModified,
	})
	assertS3Error(t, err, "NotModified", http.StatusNotModified)

	// update object
	time.Sleep(time.Second)
	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(changed),
	})
	require.NoError(t, err, "PutObject should succeed")

	// Get if modified should success
	_, err = client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket:          &bucket,
		Key:             &key,
		IfModifiedSince: getResp.LastModified,
	})
	require.NoError(t, err, "GetObject if modified should succeed")
}

func TestGetObjectIfUnmodified(t *testing.T) {
	bucket := "test-bucket"
	key := "test.txt"
	content := []byte("original content")
	changed := []byte("modified content")

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket and object
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(content),
	})
	require.NoError(t, err, "PutObject should succeed")

	// Get object
	getResp, err := client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	require.NoError(t, err, "GetObject should succeed")
	defer getResp.Body.Close()

	// Get if unmodified, should fail
	_, err = client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket:            &bucket,
		Key:               &key,
		IfUnmodifiedSince: getResp.LastModified,
	})
	require.NoError(t, err, "GetObject if unmodified should succeed")

	// update object
	time.Sleep(time.Second)
	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(changed),
	})
	require.NoError(t, err, "PutObject should succeed")

	// Get if unmodified should success
	_, err = client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket:            &bucket,
		Key:               &key,
		IfUnmodifiedSince: getResp.LastModified,
	})
	assertS3Error(t, err, "PreconditionFailed", http.StatusPreconditionFailed)
}

func TestHeadObjectIfMatchNonExistent(t *testing.T) {
	bucket := "test-bucket"
	key := "nonexistent.txt"

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create bucket
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	require.NoError(t, err, "CreateBucket should succeed")

	// Head object with If-Match on non-existent object (should fail)
	// HEAD errors return generic HTTP status codes without error details
	_, err = client.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket:  &bucket,
		Key:     &key,
		IfMatch: aws.String(`"some-etag"`),
	})
	assertHTTPStatusCode(t, err, http.StatusNotFound)
}

func TestListBuckets(t *testing.T) {
	bucket1 := "bucket-1"
	bucket2 := "bucket-2"
	bucket3 := "bucket-3"

	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// Create multiple buckets
	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket1,
	})
	require.NoError(t, err, "CreateBucket bucket1 should succeed")

	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket2,
	})
	require.NoError(t, err, "CreateBucket bucket2 should succeed")

	_, err = client.CreateBucket(t.Context(), &s3.CreateBucketInput{
		Bucket: &bucket3,
	})
	require.NoError(t, err, "CreateBucket bucket3 should succeed")

	// List all buckets
	listResp, err := client.ListBuckets(t.Context(), &s3.ListBucketsInput{})
	require.NoError(t, err, "ListBuckets should succeed")

	// Verify we got all buckets
	require.NotNil(t, listResp.Buckets, "Buckets should not be nil")
	assert.Len(t, listResp.Buckets, 3, "Should have 3 buckets")

	// Collect bucket names
	bucketNames := make(map[string]bool)
	for _, bucket := range listResp.Buckets {
		bucketNames[aws.ToString(bucket.Name)] = true
		assert.NotNil(t, bucket.CreationDate, "CreationDate should be set")
		assert.False(t, bucket.CreationDate.IsZero(), "CreationDate should not be zero")
	}

	// Verify all expected buckets are present
	assert.True(t, bucketNames[bucket1], "bucket1 should be present")
	assert.True(t, bucketNames[bucket2], "bucket2 should be present")
	assert.True(t, bucketNames[bucket3], "bucket3 should be present")
}

func TestListBucketsEmpty(t *testing.T) {
	client, close, err := New()
	if err != nil {
		t.Fatalf("setup %v", err)
	}
	t.Cleanup(func() {
		_ = close(t.Context())
	})

	// List buckets when no buckets exist
	listResp, err := client.ListBuckets(t.Context(), &s3.ListBucketsInput{})
	require.NoError(t, err, "ListBuckets should succeed")

	// Verify empty result
	require.NotNil(t, listResp.Buckets, "Buckets should not be nil")
	assert.Len(t, listResp.Buckets, 0, "Should have 0 buckets")
}
