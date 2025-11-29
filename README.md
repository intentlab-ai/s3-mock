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
client, closeFn, err := mock.New()
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

## License

See LICENSE file for details.

