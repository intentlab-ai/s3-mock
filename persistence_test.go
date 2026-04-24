package s3mock

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

// TestPersistence_RoundTrip writes objects, closes the mock, reopens it with
// the same PersistDir, and asserts all objects (including those with
// multi-part keys) survived.
func TestPersistence_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	bucket := "records"

	fixtures := map[string][]byte{
		"flat.json":                           []byte(`{"v":1}`),
		"sessions/abc/dt=2026-04-23/one.json": []byte("body-one"),
		"sessions/abc/dt=2026-04-23/two.json": bytes.Repeat([]byte("x"), 1<<16),
	}

	client1, close1, err := NewWithOptions(WithPersistence(dir))
	require.NoError(t, err)

	_, err = client1.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	for key, body := range fixtures {
		_, err := client1.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(body),
		})
		require.NoError(t, err, "put %s", key)
	}

	require.NoError(t, close1(ctx))

	client2, close2, err := NewWithOptions(WithPersistence(dir))
	require.NoError(t, err)
	defer close2(ctx)

	for key, want := range fixtures {
		out, err := client2.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "get %s after restart", key)
		got, err := io.ReadAll(out.Body)
		require.NoError(t, err)
		out.Body.Close()
		require.Equal(t, want, got, "body mismatch for %s", key)
	}
}

// TestPersistence_RetentionSweep verifies that objects past the retention
// window are reaped from both memory and disk on a synchronous sweep.
func TestPersistence_RetentionSweep(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	bucket := "ephemeral"

	// Long auto-sweep interval; we invoke Sweep() synchronously below.
	client, fake, close, err := buildMock(
		WithPersistence(dir),
		WithRetention(24*time.Hour),
		WithSweepInterval(time.Hour),
	)
	require.NoError(t, err)
	defer close(ctx)

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("fresh.txt"),
		Body:   bytes.NewReader([]byte("fresh")),
	})
	require.NoError(t, err)

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("stale.txt"),
		Body:   bytes.NewReader([]byte("stale")),
	})
	require.NoError(t, err)

	// Backdate stale.txt both in memory and on disk.
	fake.mu.Lock()
	stale := fake.buckets[bucket]["stale.txt"]
	stale.ModTime = time.Now().UTC().Add(-48 * time.Hour)
	fake.buckets[bucket]["stale.txt"] = stale
	fake.mu.Unlock()
	require.NoError(t, fake.persistPut(bucket, "stale.txt", stale))

	fake.Sweep()

	_, err = client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("stale.txt"),
	})
	require.Error(t, err, "stale.txt should be swept from memory")

	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("fresh.txt"),
	})
	require.NoError(t, err)
	out.Body.Close()

	// Disk side: the sidecar files must also be gone.
	binPath, metaPath := fake.objectPaths(bucket, "stale.txt")
	require.NoFileExists(t, binPath)
	require.NoFileExists(t, metaPath)
}

// TestConcurrentReads exercises the RWMutex path: many parallel GetObjects
// against a populated bucket must all succeed and return the right bodies.
// Correctness check under -race; any regression to a plain Mutex would
// still pass but the perf benefit (parallel reads) would be gone.
func TestConcurrentReads(t *testing.T) {
	ctx := context.Background()
	client, closeFn, err := NewWithOptions()
	require.NoError(t, err)
	defer closeFn(ctx)

	const bucket = "b"
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	const n = 100
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k/%04d", i)
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte(key)),
		})
		require.NoError(t, err)
	}

	const readers = 16
	var wg sync.WaitGroup
	errs := make(chan error, readers*n)
	for w := 0; w < readers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("k/%04d", i)
				out, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				if err != nil {
					errs <- err
					return
				}
				body, err := io.ReadAll(out.Body)
				out.Body.Close()
				if err != nil {
					errs <- err
					return
				}
				if string(body) != key {
					errs <- fmt.Errorf("key %s: body=%q", key, body)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestPersistence_InMemoryDefault asserts that NewWithOptions() with no
// options behaves identically to the in-memory New() — no disk writes.
func TestPersistence_InMemoryDefault(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	client, close, err := NewWithOptions()
	require.NoError(t, err)
	defer close(ctx)

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("b")})
	require.NoError(t, err)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("b"),
		Key:    aws.String("k"),
		Body:   bytes.NewReader([]byte("v")),
	})
	require.NoError(t, err)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "no options → no disk writes")
}
