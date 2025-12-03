# s3-mock

An in-memory S3 mock server for testing Go applications that use AWS S3.

## Overview

This package provides a simple mock implementation of AWS S3 that runs in-memory, making it ideal for unit and integration tests. It implements common S3 operations without requiring a real S3 service or network connection.

## Motivation

Commonly used tools such as [localstack](https://github.com/localstack/localstack) and [minio](https://github.com/minio/minio) lack support for [conditional requests](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html), which are important when implementing distributed applications that update or delete objects concurrently.

## Installation

```bash
go get github.com/grafana/s3-mock
```

## Usage

```go
import "github.com/grafana/s3-mock"

// Create a mock S3 client
client, closeFn, err := s3mock.New()
if err != nil {
    // handle error
}
defer closeFn(context.Background())

// Use the client as you would with a real S3 client
// The client implements the standard AWS SDK v2 S3 interface
```

## Requirements

- Go 1.24.2 or later
- AWS SDK Go v2

## API Coverage

The mock implement common API methods for creating, retrieving and deleting buckets and objects. It supports regular and directory buckets (multi-part object keys, e.g. '/path/to/object').

Important implementation gaps:
- versioned buckets
- multipart uploads

This project does not attempt to implement:
- Policies
- ACL
- Life cycle management
- Tagging
- Locks

The following table documents the current implementation status of S3 API operations within the scope of this project:

| Operation | Status | Notes |
|-----------|--------|-------|
| **Bucket Operations** |
| `CreateBucket` | ✅ Implemented | bucket configuration in the request body is ignored |
| `DeleteBucket` | ✅ Implemented | |
| `ListBuckets` | ✅  Implemented | Supports prefix, maxKeys, continuationToken, startAfter parameters |
| `HeadBucket` | ❌ Not Implemented | - |
| **Object Operations** |
| `PutObject` | ✅ Implemented | Supports conditional headers (If-Match, If-None-Match), SHA256 checksum storage |
| `GetObject` | ✅ Implemented | Supports conditional headers (If-Match, If-None-Match, if-,modified, if-unmodified) |
| `HeadObject` | ✅ Implemented | Returns object metadata without body supports conditional headers |
| `DeleteObject` | ✅ Implemented | Supports conditional headers (If-Match, If-None-Match, if-,modified, if-unmodified) |
| `CopyObject` | ❌ Not Implemented | - |
| `DeleteObjects` | ❌ Not Implemented | Batch delete operation |
| `GetObjectAttributes` | ✅ Implemented | Supports ETag, ObjectSize, Checksum (SHA256), StorageClass |
| **Listing Operations** |
| `ListObjectsV2` | ✅ Implemented | Supports prefix, maxKeys, continuationToken, startAfter parameters |
| `ListObjects` | ✅ Implemented | Legacy ListObjects v1 API - supports prefix, maxKeys, marker parameters |
| `ListObjectVersions` | ❌ Not Implemented | - |
| **Multipart Upload Operations** |
| `CreateMultipartUpload` | ❌ Not Implemented | - |
| `UploadPart` | ❌ Not Implemented | - |
| `UploadPartCopy` | ❌ Not Implemented | - |
| `CompleteMultipartUpload` | ❌ Not Implemented | - |
| `AbortMultipartUpload` | ❌ Not Implemented | - |
| `ListMultipartUploads` | ❌ Not Implemented | - |
| `ListParts` | ❌ Not Implemented | - |

## License

See LICENSE file for details.

