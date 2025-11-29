// Package mock provides a simple in-memory S3 mock server for testing.
package mock

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type object struct {
	Body           []byte
	ETag           string
	ModTime        time.Time
	ChecksumSHA256 string
}

type fakeS3 struct {
	mu      sync.Mutex
	buckets map[string]map[string]object // bucket -> key -> object
}

// New creates a new mock S3 client for testing.
func New() (*s3.Client, func(context.Context) error, error) {
	fake := &fakeS3{
		buckets: make(map[string]map[string]object),
	}

	ts := httptest.NewServer(fake)

	client, err := newTestS3Client(ts.URL)
	if err != nil {
		return nil, nil, err
	}

	closeFunc := func(_ context.Context) error {
		ts.Close()
		return nil
	}

	return client, closeFunc, nil
}

func newTestS3Client(serverURL string) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("AKID", "SECRET", "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(serverURL)
		o.UsePathStyle = true
	}), nil
}

// writeS3Error writes an S3 error response in XML format
func writeS3Error(w http.ResponseWriter, code string, message string, resource string, statusCode int) {
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-amz-request-id", "mock-request-id")
	w.Header().Set("x-amz-id-2", "mock-id-2")
	w.WriteHeader(statusCode)

	errorResp := ErrorResponse{
		Code:      code,
		Message:   message,
		Resource:  resource,
		RequestID: "mock-request-id",
	}

	_ = writeXML(w, errorResp)
}

// writeHeadError writes a generic HTTP error response for HEAD requests (no XML body)
// According to S3 spec, HEAD errors return generic HTTP status codes without error details
func writeHeadError(w http.ResponseWriter, statusCode int) {
	w.Header().Set("x-amz-request-id", "mock-request-id")
	w.Header().Set("x-amz-id-2", "mock-id-2")
	w.WriteHeader(statusCode)
}

// writes common headers for object requests
func writeObjectHeaders(w http.ResponseWriter, obj object) {
	w.Header().Set("ETag", quote(obj.ETag))
	w.Header().Set("Last-Modified", obj.ModTime.Format(http.TimeFormat))
	w.Header().Set("x-amz-request-id", "mock-request-id")
	if obj.ChecksumSHA256 != "" {
		w.Header().Set("x-amz-checksum-sha256", obj.ChecksumSHA256)
	}
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(path, "/")

	// Handle service-level operations (root path)
	if bucket == "" {
		switch r.Method {
		case http.MethodGet:
			f.handleListBuckets(w, r)
		default:
			writeS3Error(
				w,
				"NotImplemented",
				"The requested operation is not implemented",
				r.URL.Path,
				http.StatusNotImplemented,
			)
		}
		return
	}

	// Handle bucket-level operations)
	if key == "" {
		switch r.Method {
		case http.MethodGet:
			f.handleListObjects(w, r, bucket)
		case http.MethodPut:
			f.handleCreateBucket(w, r, bucket)
		case http.MethodDelete:
			f.handleDeleteBucket(w, r, bucket)
		default:
			writeS3Error(
				w,
				"NotImplemented",
				"The requested operation is not implemented",
				r.URL.Path,
				http.StatusNotImplemented,
			)
		}
		return
	}

	// Handle object-level operations
	switch r.Method {
	case http.MethodPut:
		f.handlePutObject(w, r, bucket, key)
	case http.MethodGet:
		f.handleGetObject(w, r, bucket, key)
	case http.MethodHead:
		f.handleHeadObject(w, r, bucket, key)
	case http.MethodDelete:
		f.handleDeleteObject(w, r, bucket, key)
	default:
		writeS3Error(
			w,
			"NotImplemented",
			"The requested operation is not implemented",
			r.URL.Path,
			http.StatusNotImplemented,
		)
	}
}

// handleListObjects returns a list of objects in the bucket as XML (supports both ListObjects v1 and ListObjectsV2)
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html
func (f *fakeS3) handleListObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	query := r.URL.Query()

	// handle V2
	if query.Get("list-type") == "2" {
		f.handleListObjectsV2(w, r, bucket)
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	objects, bucketExists := f.buckets[bucket]
	if !bucketExists {
		writeS3Error(
			w,
			"NoSuchBucket",
			"The specified bucket does not exist",
			"/"+bucket, http.StatusNotFound,
		)
		return
	}

	prefix := query.Get("prefix")
	maxKeys := 1000 // default
	if maxKeysStr := query.Get("max-keys"); maxKeysStr != "" {
		if parsed, err := strconv.Atoi(maxKeysStr); err == nil && parsed > 0 {
			maxKeys = parsed
		}
	}

	marker := query.Get("marker")
	keys, isTruncated, nextToken := filterList(
		slices.Collect(maps.Keys(objects)),
		prefix,
		"", // v1 does not have a startAfter parameter
		marker,
		maxKeys,
	)

	// Default owner for v1 responses
	defaultOwner := &Owner{
		ID:          "mock-owner-id",
		DisplayName: "Mock Owner",
	}

	var contents []ContentsV1
	for _, key := range keys {
		obj := objects[key]
		contents = append(contents, ContentsV1{
			Key:          key,
			LastModified: obj.ModTime.UTC().Format(time.RFC3339),
			ETag:         quote(obj.ETag),
			Size:         int64(len(obj.Body)),
			StorageClass: "STANDARD",
			Owner:        defaultOwner,
		})
	}

	result := ListBucketResultV1{
		XMLNS:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:        bucket,
		Prefix:      prefix,
		Marker:      marker,
		MaxKeys:     maxKeys,
		IsTruncated: isTruncated,
		NextMarker:  nextToken,
		Contents:    contents,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = writeXML(w, result)
}

// handleListObjectsV2 returns a list of objects in the bucket as XML
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
func (f *fakeS3) handleListObjectsV2(w http.ResponseWriter, r *http.Request, bucket string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	objects, bucketExists := f.buckets[bucket]
	if !bucketExists {
		writeS3Error(
			w,
			"NoSuchBucket",
			"The specified bucket does not exist",
			"/"+bucket, http.StatusNotFound,
		)
		return
	}

	// Parse query parameters
	query := r.URL.Query()

	prefix := query.Get("prefix")
	maxKeys := 1000 // default
	if maxKeysStr := query.Get("max-keys"); maxKeysStr != "" {
		if parsed, err := strconv.Atoi(maxKeysStr); err == nil && parsed > 0 {
			maxKeys = parsed
		}
	}

	continuationToken := query.Get("continuation-token")
	startAfter := query.Get("start-after")

	keys, isTruncated, nextToken := filterList(
		slices.Collect(maps.Keys(objects)),
		prefix,
		startAfter,
		continuationToken,
		maxKeys,
	)

	var contents []Contents
	for _, key := range keys {
		obj := objects[key]
		contents = append(contents, Contents{
			Key:          key,
			LastModified: obj.ModTime.UTC().Format(time.RFC3339),
			ETag:         quote(obj.ETag),
			Size:         int64(len(obj.Body)),
			StorageClass: "STANDARD",
		})
	}

	result := ListBucketResult{
		XMLNS:                 "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:                  bucket,
		Prefix:                prefix,
		MaxKeys:               maxKeys,
		IsTruncated:           isTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: nextToken,
		StartAfter:            startAfter,
		KeyCount:              len(contents),
		Contents:              contents,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = writeXML(w, result)
}

// handleListBuckets returns a list of all buckets as XML (ListBuckets)
func (f *fakeS3) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	bucketNames := make([]string, 0, len(f.buckets))
	for bucketName := range f.buckets {
		bucketNames = append(bucketNames, bucketName)
	}

	// Parse query parameters
	query := r.URL.Query()
	prefix := query.Get("prefix")
	maxKeys := 1000 // default
	if maxKeysStr := query.Get("max-keys"); maxKeysStr != "" {
		if parsed, err := strconv.Atoi(maxKeysStr); err == nil && parsed > 0 {
			maxKeys = parsed
		}
	}
	continuationToken := query.Get("continuation-token")
	startAfter := query.Get("start-after")

	// Apply filtering and pagination
	filteredBucketNames, isTruncated, nextContinuationToken := filterList(
		bucketNames,
		prefix,
		startAfter,
		continuationToken,
		maxKeys,
	)

	// Build bucket list with creation date
	// For mock purposes, use a default creation date
	defaultCreationDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
	var buckets []Bucket
	for _, bucketName := range filteredBucketNames {
		buckets = append(buckets, Bucket{
			Name:         bucketName,
			CreationDate: defaultCreationDate,
		})
	}

	result := ListAllMyBucketsResult{
		XMLNS:                 "http://s3.amazonaws.com/doc/2006-03-01/",
		Prefix:                prefix,
		MaxKeys:               maxKeys,
		IsTruncated:           isTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: nextContinuationToken,
		StartAfter:            startAfter,
		BucketCount:           len(buckets),
		Buckets:               buckets,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = writeXML(w, result)
}

func filterList(
	keys []string,
	prefix string,
	startAfter string,
	continuationToken string,
	maxKeys int,
) ([]string, bool, string) {
	sort.Strings(keys)

	// Apply prefix filter
	if prefix != "" {
		var filtered []string
		for _, key := range keys {
			if strings.HasPrefix(key, prefix) {
				filtered = append(filtered, key)
			}
		}
		keys = filtered
	}

	// Apply start-after filter
	if startAfter != "" {
		var filtered []string
		for _, key := range keys {
			if key > startAfter {
				filtered = append(filtered, key)
			}
		}
		keys = filtered
	}

	// Apply continuation token (simple implementation: skip keys before token)
	if continuationToken != "" {
		var filtered []string
		found := false
		for _, key := range keys {
			if found {
				filtered = append(filtered, key)
			} else if key == continuationToken {
				found = true
			}
		}
		keys = filtered
	}

	// Limit to maxKeys
	isTruncated := false
	nextContinuationToken := ""
	if len(keys) > maxKeys {
		isTruncated = true
		nextContinuationToken = keys[maxKeys-1]
		keys = keys[:maxKeys]
	}

	return keys, isTruncated, nextContinuationToken
}

// writeXML encodes value as XML to w
func writeXML(w http.ResponseWriter, value any) error {
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	return enc.Encode(value)
}

// parseObjectAttributesHeader parses the x-amz-object-attributes header value
// which contains comma-separated attribute names (e.g., "ETag,ObjectSize,StorageClass").
// Returns a map with attribute names as keys and true as values for requested attributes.
func parseObjectAttributesHeader(r *http.Request) map[string]bool {
	requested := make(map[string]bool)

	// we use the canonical form of the header even when the specification uses
	// x-amz-object-attributes
	headerValues := r.Header["X-Amz-Object-Attributes"]
	if len(headerValues) == 0 {
		return requested
	}

	for _, attr := range headerValues {
		attr = strings.TrimSpace(attr)
		if attr != "" {
			requested[attr] = true
		}
	}

	return requested
}

func (f *fakeS3) handleCreateBucket(w http.ResponseWriter, _ *http.Request, bucket string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.buckets[bucket]; exists {
		writeS3Error(
			w,
			"BucketAlreadyExists",
			"The requested bucket name already exists",
			"/"+bucket, http.StatusConflict,
		)
		return
	}

	f.buckets[bucket] = make(map[string]object)
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(
			w,
			"InternalError",
			"Failed to read request body",
			"/"+bucket+"/"+key, http.StatusInternalServerError,
		)
		return
	}

	// Extract SHA256 checksum if provided
	checksumSHA256 := r.Header.Get("x-amz-checksum-sha256")

	f.mu.Lock()
	defer f.mu.Unlock()

	// Validate bucket exists
	objects, bucketExists := f.buckets[bucket]
	if !bucketExists {
		writeS3Error(
			w,
			"NoSuchBucket",
			"The specified bucket does not exist",
			"/"+bucket, http.StatusNotFound,
		)
		return
	}

	existing, exists := objects[key]

	// Conditional headers for PutObject
	// See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
	ifMatch := strings.Trim(r.Header.Get("If-Match"), `"`)
	ifNoneMatch := strings.Trim(r.Header.Get("If-None-Match"), `"`)

	// If-Match: Upload only if ETag matches. Returns 412 if ETag doesn't match.
	// AWS returns 412 even if object doesn't exist.
	if ifMatch != "" {
		if !exists || ifMatch != existing.ETag {
			writeS3Error(
				w,
				"PreconditionFailed",
				"At least one of the pre-conditions you specified did not hold",
				"/"+bucket+"/"+key, http.StatusPreconditionFailed,
			)
			return
		}
	}

	// If-None-Match: Expects "*" (asterisk). Upload only if object doesn't exist.
	// Returns 412 if object already exists.
	if ifNoneMatch != "" && ifNoneMatch != "*" {
		// TODO check what code does s3 return if if-none-match is not '*'
		writeS3Error(
			w,
			"BadRequest",
			"invalid 'if-none-match' header. Expected '*'",
			"/"+bucket+"/"+key,
			http.StatusBadRequest,
		)
		return
	}

	if ifNoneMatch == "*" && exists {
		writeS3Error(
			w,
			"PreconditionFailed",
			"At least one of the pre-conditions you specified did not hold",
			"/"+bucket+"/"+key, http.StatusPreconditionFailed,
		)
		return
	}

	etag := computeETag(body)
	now := time.Now().UTC()

	objects[key] = object{
		Body:           body,
		ETag:           etag,
		ModTime:        now,
		ChecksumSHA256: checksumSHA256,
	}

	w.Header().Set("ETag", quote(etag))
	w.Header().Set("Last-Modified", now.Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

// returns objects attributes in headers
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
func (f *fakeS3) handleHeadObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	objects, bucketExists := f.buckets[bucket]
	if !bucketExists {
		writeHeadError(w, http.StatusNotFound)
		return
	}
	obj, exists := objects[key]

	if !exists {
		writeHeadError(w, http.StatusNotFound)
		return
	}

	// Conditional headers for HeadObject
	if !checkConditionalRequest(w, r, bucket, key, obj) {
		return
	}

	// Write common headers
	writeObjectHeaders(w, obj)

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}

// handles object get requests. If 'attributes' query parameter is specified, returns the attrinutes
// (see handleGetObjectAttributes). Otherwise, returns the object's content
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
func (f *fakeS3) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	// If GetObjectAttributes request
	_, isGetAttributes := r.URL.Query()["attributes"]
	if isGetAttributes {
		f.handleGetObjectAttributes(w, r, bucket, key)
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	objects, bucketExists := f.buckets[bucket]
	if !bucketExists {
		writeS3Error(
			w,
			"NoSuchBucket",
			"The specified bucket does not exist",
			"/"+bucket, http.StatusNotFound)
		return
	}
	obj, exists := objects[key]

	if !exists {
		writeS3Error(
			w,
			"NoSuchKey",
			"The specified key does not exist",
			"/"+bucket+"/"+key, http.StatusNotFound,
		)

		return
	}

	// Write common headers
	writeObjectHeaders(w, obj)

	// Conditional headers for GetObject
	// See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
	if !checkConditionalRequest(w, r, bucket, key, obj) {
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(obj.Body)
}

// returns object attributes
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html
func (f *fakeS3) handleGetObjectAttributes(w http.ResponseWriter, r *http.Request, bucket, key string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	objects, bucketExists := f.buckets[bucket]
	if !bucketExists {
		writeS3Error(
			w,
			"NoSuchBucket",
			"The specified bucket does not exist",
			"/"+bucket, http.StatusNotFound)
		return
	}
	obj, exists := objects[key]

	if !exists {
		writeS3Error(
			w,
			"NoSuchKey",
			"The specified key does not exist",
			"/"+bucket+"/"+key, http.StatusNotFound,
		)

		return
	}

	requestedAttributes := parseObjectAttributesHeader(r)
	if len(requestedAttributes) == 0 {
		writeS3Error(
			w,
			"InvalidRequest",
			"At least one attribute must be specified in 'x-amz-object-attributes' header",
			"/"+bucket+"/"+key,
			http.StatusBadRequest,
		)
		return
	}

	// Write common headers
	writeObjectHeaders(w, obj)

	w.Header().Set("Content-Type", "application/xml")
	response := GetObjectAttributesOutput{
		XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/",
	}

	// Only include requested attributes (or all if header not provided)
	if requestedAttributes["ETag"] {
		response.ETag = quote(obj.ETag)
	}
	if requestedAttributes["ObjectSize"] {
		response.ObjectSize = int64(len(obj.Body))
	}
	if requestedAttributes["Checksum"] && obj.ChecksumSHA256 != "" {
		response.Checksum = &Checksum{
			ChecksumSHA256: obj.ChecksumSHA256,
		}
	}
	if requestedAttributes["StorageClass"] {
		response.StorageClass = "STANDARD"
	}

	w.WriteHeader(http.StatusOK)
	_ = writeXML(w, response)
}

func checkConditionalRequest(w http.ResponseWriter, r *http.Request, bucket string, key string, obj object) bool {
	// Conditional headers for GetObject
	// See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
	ifMatch := strings.Trim(r.Header.Get("If-Match"), `"`)
	ifNoneMatch := strings.Trim(r.Header.Get("If-None-Match"), `"`)

	// If-Match: Return object only if ETag matches, otherwise 412
	if ifMatch != "" && ifMatch != obj.ETag {
		writeS3Error(
			w,
			"PreconditionFailed",
			"At least one of the pre-conditions you specified did not hold",
			"/"+bucket+"/"+key, http.StatusPreconditionFailed,
		)
		return false
	}

	// If-None-Match: Return object only if ETag differs, otherwise 304
	if ifNoneMatch != "" && ifNoneMatch == obj.ETag {
		// Set ETag header even for 304 responses
		w.Header().Set("ETag", quote(obj.ETag))
		w.WriteHeader(http.StatusNotModified)
		return false
	}

	return true
}

func (f *fakeS3) handleDeleteBucket(w http.ResponseWriter, _ *http.Request, bucket string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	objects, bucketExists := f.buckets[bucket]
	if !bucketExists {
		writeS3Error(
			w,
			"NoSuchBucket",
			"The specified bucket does not exist",
			"/"+bucket, http.StatusNotFound,
		)
		return
	}

	// Check if bucket is empty
	if len(objects) > 0 {
		writeS3Error(
			w,
			"BucketNotEmpty",
			"The bucket you tried to delete is not empty",
			"/"+bucket, http.StatusConflict,
		)
		return
	}

	// Delete the bucket
	delete(f.buckets, bucket)
	w.WriteHeader(http.StatusNoContent)
}

func (f *fakeS3) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	objects, bucketExists := f.buckets[bucket]
	if !bucketExists {
		writeS3Error(
			w,
			"NoSuchBucket",
			"The specified bucket does not exist",
			"/"+bucket, http.StatusNotFound,
		)
		return
	}

	obj, exists := objects[key]
	if !exists {
		// S3 DELETE is idempotent and usually returns 204 even if missing
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Conditional headers for DeleteObject
	// See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html
	ifMatch := r.Header.Get("If-Match")

	// If-Match: Deletes the object only if ETag matches, otherwise 412
	if ifMatch != "" && strings.Trim(ifMatch, `"`) != obj.ETag {
		writeS3Error(
			w,
			"PreconditionFailed",
			"At least one of the pre-conditions you specified did not hold",
			"/"+bucket+"/"+key, http.StatusPreconditionFailed,
		)
		return
	}

	delete(objects, key)
	w.WriteHeader(http.StatusNoContent)
}
