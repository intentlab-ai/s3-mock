package s3mock

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Options configures a mock constructed via NewWithOptions.
type Options struct {
	// PersistDir enables on-disk persistence under the given directory.
	// Empty = fully in-memory (original behaviour).
	PersistDir string

	// Retention is the max age of an object; older objects are reaped by the
	// sweeper. Zero = disabled (objects live forever).
	Retention time.Duration

	// SweepInterval is how often the retention sweeper wakes. Zero = 5 min.
	SweepInterval time.Duration
}

// Option mutates Options; pass to NewWithOptions.
type Option func(*Options)

// WithPersistence enables disk persistence under dir.
func WithPersistence(dir string) Option {
	return func(o *Options) { o.PersistDir = dir }
}

// WithRetention sets the TTL applied by the sweeper.
func WithRetention(d time.Duration) Option {
	return func(o *Options) { o.Retention = d }
}

// WithSweepInterval overrides the default sweep interval.
func WithSweepInterval(d time.Duration) Option {
	return func(o *Options) { o.SweepInterval = d }
}

// NewWithOptions creates a mock S3 client with the given options. Passing
// no options is equivalent to New().
func NewWithOptions(opts ...Option) (*s3.Client, func(context.Context) error, error) {
	client, _, closeFn, err := buildMock(opts...)
	return client, closeFn, err
}

// buildMock is the shared constructor used by NewWithOptions and by package
// tests that need direct access to the fakeS3 (e.g. to force-sweep).
func buildMock(opts ...Option) (*s3.Client, *fakeS3, func(context.Context) error, error) {
	cfg := Options{}
	for _, apply := range opts {
		apply(&cfg)
	}
	if cfg.SweepInterval == 0 {
		cfg.SweepInterval = 5 * time.Minute
	}

	fake := &fakeS3{
		buckets:    make(map[string]map[string]object),
		persistDir: cfg.PersistDir,
		retention:  cfg.Retention,
	}

	if cfg.PersistDir != "" {
		if err := os.MkdirAll(cfg.PersistDir, 0o700); err != nil {
			return nil, nil, nil, fmt.Errorf("create persist dir: %w", err)
		}
		if err := fake.loadFromDisk(); err != nil {
			return nil, nil, nil, fmt.Errorf("load persisted state: %w", err)
		}
	}

	ts := httptest.NewServer(fake)

	if cfg.Retention > 0 {
		fake.sweepStop = make(chan struct{})
		fake.sweepDone = make(chan struct{})
		go fake.sweepLoop(cfg.SweepInterval)
	}

	client, err := newTestS3Client(ts.URL)
	if err != nil {
		ts.Close()
		return nil, nil, nil, err
	}

	closeFunc := func(_ context.Context) error {
		if fake.sweepStop != nil {
			close(fake.sweepStop)
			<-fake.sweepDone
		}
		ts.Close()
		return nil
	}

	return client, fake, closeFunc, nil
}

// ----- on-disk layout -----
//
//   <persistDir>/
//     <bucket>/
//       <hex(sha256(key))>.bin        raw object bytes
//       <hex(sha256(key))>.meta.json  key + ETag + ModTime + checksum
//
// Keys are hashed so multi-part keys (with slashes) can't produce nested
// directories or escape the bucket directory via "..".

type objectMeta struct {
	Key            string    `json:"key"`
	ETag           string    `json:"etag"`
	ModTime        time.Time `json:"mod_time"`
	ChecksumSHA256 string    `json:"checksum_sha256,omitempty"`
}

func hashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func (f *fakeS3) bucketDir(bucket string) string {
	return filepath.Join(f.persistDir, bucket)
}

func (f *fakeS3) objectPaths(bucket, key string) (binPath, metaPath string) {
	h := hashKey(key)
	dir := f.bucketDir(bucket)
	return filepath.Join(dir, h+".bin"), filepath.Join(dir, h+".meta.json")
}

// persistPut writes body + sidecar atomically via tmp + rename.
func (f *fakeS3) persistPut(bucket, key string, obj object) error {
	if f.persistDir == "" {
		return nil
	}
	dir := f.bucketDir(bucket)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}

	binPath, metaPath := f.objectPaths(bucket, key)

	if err := writeAtomic(binPath, obj.Body); err != nil {
		return err
	}
	meta := objectMeta{
		Key:            key,
		ETag:           obj.ETag,
		ModTime:        obj.ModTime,
		ChecksumSHA256: obj.ChecksumSHA256,
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return writeAtomic(metaPath, metaBytes)
}

func (f *fakeS3) persistDelete(bucket, key string) error {
	if f.persistDir == "" {
		return nil
	}
	binPath, metaPath := f.objectPaths(bucket, key)
	_ = os.Remove(binPath)
	_ = os.Remove(metaPath)
	return nil
}

func (f *fakeS3) persistCreateBucket(bucket string) error {
	if f.persistDir == "" {
		return nil
	}
	return os.MkdirAll(f.bucketDir(bucket), 0o700)
}

func (f *fakeS3) persistDeleteBucket(bucket string) error {
	if f.persistDir == "" {
		return nil
	}
	return os.RemoveAll(f.bucketDir(bucket))
}

func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, path)
}

// loadFromDisk rebuilds f.buckets from persistDir. Any malformed sidecar or
// missing body file is skipped with a silent warning — this is a local cache,
// not authoritative storage.
func (f *fakeS3) loadFromDisk() error {
	entries, err := os.ReadDir(f.persistDir)
	if err != nil {
		return err
	}
	for _, bucketEntry := range entries {
		if !bucketEntry.IsDir() {
			continue
		}
		bucket := bucketEntry.Name()
		bucketPath := filepath.Join(f.persistDir, bucket)

		objs, err := os.ReadDir(bucketPath)
		if err != nil {
			continue
		}
		f.buckets[bucket] = make(map[string]object)

		for _, entry := range objs {
			name := entry.Name()
			if filepath.Ext(name) != ".json" {
				continue
			}
			// .meta.json sidecar
			metaPath := filepath.Join(bucketPath, name)
			metaData, err := os.ReadFile(metaPath)
			if err != nil {
				continue
			}
			var meta objectMeta
			if err := json.Unmarshal(metaData, &meta); err != nil {
				continue
			}
			binPath := metaPath[:len(metaPath)-len(".meta.json")] + ".bin"
			body, err := os.ReadFile(binPath)
			if err != nil {
				continue
			}
			f.buckets[bucket][meta.Key] = object{
				Body:           body,
				ETag:           meta.ETag,
				ModTime:        meta.ModTime,
				ChecksumSHA256: meta.ChecksumSHA256,
			}
		}
	}
	return nil
}

// sweepLoop reaps objects older than f.retention at each tick.
func (f *fakeS3) sweepLoop(interval time.Duration) {
	defer close(f.sweepDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-f.sweepStop:
			return
		case <-ticker.C:
			f.sweepOnce()
		}
	}
}

func (f *fakeS3) sweepOnce() {
	cutoff := time.Now().UTC().Add(-f.retention)

	f.mu.Lock()
	type victim struct{ bucket, key string }
	var victims []victim
	for bucket, objs := range f.buckets {
		for key, obj := range objs {
			if obj.ModTime.Before(cutoff) {
				victims = append(victims, victim{bucket, key})
			}
		}
	}
	for _, v := range victims {
		delete(f.buckets[v.bucket], v.key)
	}
	f.mu.Unlock()

	// Disk removal outside the lock — no in-memory reader can see these
	// objects anymore, so leaking files briefly is safe.
	for _, v := range victims {
		_ = f.persistDelete(v.bucket, v.key)
	}
}

// Sweep triggers a single sweep pass synchronously. Exposed for tests that
// need deterministic reaping rather than waiting for the ticker.
func (f *fakeS3) Sweep() { f.sweepOnce() }

