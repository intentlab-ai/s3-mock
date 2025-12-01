package s3mock

// ErrorResponse represents an S3 error response in XML format.
type ErrorResponse struct {
	XMLName   struct{} `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

// Checksum represents checksum information in GetObjectAttributes response.
type Checksum struct {
	ChecksumSHA256 string `xml:"ChecksumSHA256,omitempty"`
}

// GetObjectAttributesOutput represents the response for GetObjectAttributes operation.
// Note: LastModified is returned as an HTTP header (Last-Modified), not in the XML body.
// Only attributes specified in the x-amz-object-attributes header are included.
type GetObjectAttributesOutput struct {
	XMLName      struct{}     `xml:"GetObjectAttributesOutput"`
	XMLNS        string       `xml:"xmlns,attr"`
	ETag         string       `xml:"ETag,omitempty"`
	ObjectSize   int64        `xml:"ObjectSize,omitempty"`
	Checksum     *Checksum    `xml:"Checksum,omitempty"`
	StorageClass string       `xml:"StorageClass,omitempty"`
	ObjectParts  *ObjectParts `xml:"ObjectParts,omitempty"`
}

// ObjectParts represents information about object parts in GetObjectAttributes response.
type ObjectParts struct {
	TotalPartsCount int64 `xml:"TotalPartsCount,omitempty"`
}

// Contents represents an object entry in ListObjectsV2 response.
type Contents struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

// ContentsV1 represents an object entry in ListObjects v1 response (includes Owner).
type ContentsV1 struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
	Owner        *Owner `xml:"Owner"`
}

// ListBucketResult represents the response for ListObjectsV2 operation.
type ListBucketResult struct {
	XMLName               struct{}   `xml:"ListBucketResult"`
	XMLNS                 string     `xml:"xmlns,attr"`
	Name                  string     `xml:"Name"`
	Prefix                string     `xml:"Prefix,omitempty"`
	MaxKeys               int        `xml:"MaxKeys"`
	IsTruncated           bool       `xml:"IsTruncated"`
	ContinuationToken     string     `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string     `xml:"NextContinuationToken,omitempty"`
	StartAfter            string     `xml:"StartAfter,omitempty"`
	KeyCount              int        `xml:"KeyCount"`
	Contents              []Contents `xml:"Contents"`
}

// ListBucketResultV1 represents the response for ListObjects v1 operation.
type ListBucketResultV1 struct {
	XMLName     struct{}     `xml:"ListBucketResult"`
	XMLNS       string       `xml:"xmlns,attr"`
	Name        string       `xml:"Name"`
	Prefix      string       `xml:"Prefix,omitempty"`
	Marker      string       `xml:"Marker,omitempty"`
	MaxKeys     int          `xml:"MaxKeys"`
	IsTruncated bool         `xml:"IsTruncated"`
	NextMarker  string       `xml:"NextMarker,omitempty"`
	Contents    []ContentsV1 `xml:"Contents"`
}

// Bucket represents a bucket entry in ListAllMyBucketsResult response.
type Bucket struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

// ListAllMyBucketsResult represents the response for ListBuckets operation.
type ListAllMyBucketsResult struct {
	XMLName               struct{} `xml:"ListAllMyBucketsResult"`
	XMLNS                 string   `xml:"xmlns,attr"`
	Owner                 *Owner   `xml:"Owner,omitempty"`
	Prefix                string   `xml:"Prefix,omitempty"`
	MaxKeys               int      `xml:"MaxKeys"`
	IsTruncated           bool     `xml:"IsTruncated"`
	ContinuationToken     string   `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string   `xml:"NextContinuationToken,omitempty"`
	StartAfter            string   `xml:"StartAfter,omitempty"`
	BucketCount           int      `xml:"BucketCount"`
	Buckets               []Bucket `xml:"Buckets>Bucket"`
}

// Owner represents the bucket owner in ListAllMyBucketsResult response.
type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName,omitempty"`
}
