/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package trainer

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	. "github.com/opendatahub-io/distributed-workloads/tests/common/support"
)

// CloudURI represents a parsed cloud storage URI
type CloudURI struct {
	Scheme string // e.g., "s3", "azure", "gs"
	Bucket string
	Prefix string
}

// ParseCloudURI parses a cloud storage URI into scheme, bucket, and prefix.
// Excludes PVC URIs (pvc://) and local filesystem paths (no scheme).
// Returns nil if not a valid cloud storage URI.
func ParseCloudURI(uri string) *CloudURI {
	if idx := strings.Index(uri, "://"); idx <= 0 {
		return nil // Local filesystem path (no scheme)
	}

	scheme := uri[:strings.Index(uri, "://")]
	// Exclude PVC URIs - they are not cloud storage
	if scheme == "pvc" {
		return nil
	}

	rest := uri[strings.Index(uri, "://")+3:]
	parts := strings.SplitN(rest, "/", 2)
	bucket := parts[0]
	prefix := ""
	if len(parts) > 1 {
		prefix = parts[1]
	}

	return &CloudURI{
		Scheme: scheme,
		Bucket: bucket,
		Prefix: prefix,
	}
}

// CloudStorageProvider defines the interface for cloud storage operations.
// Easy to extend: implement for Azure, GCS, etc.
type CloudStorageProvider interface {
	// PrepareStorage deletes old objects and creates a .keep file to ensure prefix exists
	PrepareStorage(ctx context.Context, uri *CloudURI) error
	// CleanupStorage deletes all objects under the prefix
	CleanupStorage(ctx context.Context, uri *CloudURI) (int, error)
	// CreateBucket creates a new bucket if it doesn't exist
	CreateBucket(ctx context.Context, bucketName string) error
	// DeleteBucket deletes a bucket and all its contents
	DeleteBucket(ctx context.Context, bucketName string) error
	// BucketExists checks if a bucket exists
	BucketExists(ctx context.Context, bucketName string) (bool, error)
}

// S3Provider implements CloudStorageProvider for S3-compatible storage
type S3Provider struct {
	endpoint  string
	accessKey string
	secretKey string
}

// NewS3Provider creates a new S3 provider using environment credentials.
// Credentials are retrieved from environment variables (AWS_DEFAULT_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY).
// This pattern can be followed for other providers:
//   - Azure: Get Azure credentials from AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY, etc.
//   - GCS: Get GCS credentials from GOOGLE_APPLICATION_CREDENTIALS or GCS_* env vars
func NewS3Provider() (*S3Provider, error) {
	endpoint, _ := GetStorageBucketDefaultEndpoint()
	accessKey, _ := GetStorageBucketAccessKeyId()
	secretKey, _ := GetStorageBucketSecretKey()

	if endpoint == "" || accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("S3 credentials not configured")
	}

	return &S3Provider{
		endpoint:  endpoint,
		accessKey: accessKey,
		secretKey: secretKey,
	}, nil
}

// GetS3Client creates an S3 client with the provider's credentials
func (p *S3Provider) GetS3Client() (*minio.Client, error) {
	endpointURL := p.endpoint
	secure := true
	if !strings.HasPrefix(endpointURL, "http") {
		endpointURL = "https://" + endpointURL
	} else if strings.HasPrefix(endpointURL, "http://") {
		secure = false
	}

	// Extract host:port from URL (remove http:// or https://)
	endpoint := strings.TrimPrefix(strings.TrimPrefix(endpointURL, "https://"), "http://")

	// Configure TLS verification based on environment variable
	// CHECKPOINT_VERIFY_SSL can be set to "true" to enable certificate verification
	// Defaults to false to support test environments with self-signed certificates
	verifySSL := false
	if verifySSLEnv := os.Getenv("CHECKPOINT_VERIFY_SSL"); verifySSLEnv != "" {
		if val, err := strconv.ParseBool(verifySSLEnv); err == nil {
			verifySSL = val
		}
	}

	var tr *http.Transport
	if !verifySSL {
		// Skip TLS verification for test environments with self-signed certificates
		tr = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(p.accessKey, p.secretKey, ""),
		Secure:    secure,
		Transport: tr,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	return client, nil
}

// PrepareStorage deletes old objects and creates a .keep file for S3
func (p *S3Provider) PrepareStorage(ctx context.Context, uri *CloudURI) error {
	if uri.Bucket == "" || uri.Prefix == "" {
		return fmt.Errorf("invalid URI: bucket and prefix required")
	}

	client, err := p.GetS3Client()
	if err != nil {
		return err
	}

	// Delete all existing objects under the prefix
	fullPrefix := strings.TrimSuffix(uri.Prefix, "/") + "/"
	objectsCh := client.ListObjects(ctx, uri.Bucket, minio.ListObjectsOptions{
		Prefix:    fullPrefix,
		Recursive: true,
	})

	deleted := 0
	for object := range objectsCh {
		if object.Err != nil {
			return fmt.Errorf("failed to list objects: %w", object.Err)
		}
		if err := client.RemoveObject(ctx, uri.Bucket, object.Key, minio.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("failed to delete object %s: %w", object.Key, err)
		}
		deleted++
	}

	// Create .keep file to ensure prefix exists
	keepKey := strings.TrimSuffix(uri.Prefix, "/") + "/.keep"
	_, err = client.PutObject(ctx, uri.Bucket, keepKey, strings.NewReader("placeholder"), int64(len("placeholder")), minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to create .keep file: %w", err)
	}

	return nil
}

// CleanupStorage deletes all objects under the prefix for S3
func (p *S3Provider) CleanupStorage(ctx context.Context, uri *CloudURI) (int, error) {
	if uri.Bucket == "" || uri.Prefix == "" {
		return 0, fmt.Errorf("invalid URI: bucket and prefix required")
	}

	client, err := p.GetS3Client()
	if err != nil {
		return 0, err
	}

	fullPrefix := strings.TrimSuffix(uri.Prefix, "/") + "/"
	objectsCh := client.ListObjects(ctx, uri.Bucket, minio.ListObjectsOptions{
		Prefix:    fullPrefix,
		Recursive: true,
	})

	deleted := 0
	for object := range objectsCh {
		if object.Err != nil {
			return deleted, fmt.Errorf("failed to list objects: %w", object.Err)
		}
		if err := client.RemoveObject(ctx, uri.Bucket, object.Key, minio.RemoveObjectOptions{}); err != nil {
			return deleted, fmt.Errorf("failed to delete object %s: %w", object.Key, err)
		}
		deleted++
	}

	return deleted, nil
}

// CreateBucket creates a new S3 bucket if it doesn't exist
func (p *S3Provider) CreateBucket(ctx context.Context, bucketName string) error {
	if bucketName == "" {
		return fmt.Errorf("bucket name cannot be empty")
	}

	client, err := p.GetS3Client()
	if err != nil {
		return err
	}

	// Check if bucket already exists
	exists, err := client.BucketExists(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("failed to check if bucket exists: %w", err)
	}

	if exists {
		return nil // Bucket already exists, no need to create
	}

	// Create the bucket
	err = client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
	if err != nil {
		return fmt.Errorf("failed to create bucket %s: %w", bucketName, err)
	}

	return nil
}

// DeleteBucket deletes an S3 bucket and all its contents
func (p *S3Provider) DeleteBucket(ctx context.Context, bucketName string) error {
	if bucketName == "" {
		return fmt.Errorf("bucket name cannot be empty")
	}

	client, err := p.GetS3Client()
	if err != nil {
		return err
	}

	// Check if bucket exists
	exists, err := client.BucketExists(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("failed to check if bucket exists: %w", err)
	}

	if !exists {
		return nil // Bucket doesn't exist, nothing to delete
	}

	// Delete all objects in the bucket first
	objectsCh := client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Recursive: true,
	})

	for object := range objectsCh {
		if object.Err != nil {
			return fmt.Errorf("failed to list objects: %w", object.Err)
		}
		if err := client.RemoveObject(ctx, bucketName, object.Key, minio.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("failed to delete object %s: %w", object.Key, err)
		}
	}

	// Delete the bucket
	err = client.RemoveBucket(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("failed to delete bucket %s: %w", bucketName, err)
	}

	return nil
}

// BucketExists checks if an S3 bucket exists
func (p *S3Provider) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	if bucketName == "" {
		return false, fmt.Errorf("bucket name cannot be empty")
	}

	client, err := p.GetS3Client()
	if err != nil {
		return false, err
	}

	exists, err := client.BucketExists(ctx, bucketName)
	if err != nil {
		return false, fmt.Errorf("failed to check if bucket exists: %w", err)
	}

	return exists, nil
}

// getCloudStorageProvider returns the appropriate provider for the given URI scheme.
// Easy to extend: add cases for "azure", "gs" (GCS), etc.
func getCloudStorageProvider(scheme string) (CloudStorageProvider, error) {
	switch scheme {
	case "s3":
		return NewS3Provider()
	// Future: add cases for other cloud providers
	default:
		return nil, fmt.Errorf("unsupported cloud storage scheme: %s", scheme)
	}
}

// PrepareCloudCheckpointStorage prepares cloud checkpoint storage by deleting old objects
// and creating a .keep file to ensure the prefix exists.
// Returns nil on success or if the URI is not cloud storage. Errors are logged but
// do not cause test failures, allowing tests to continue even if storage operations fail.
func PrepareCloudCheckpointStorage(test Test, checkpointURI string) error {
	test.T().Helper()

	uri := ParseCloudURI(checkpointURI)
	if uri == nil {
		return nil // Not a cloud storage URI, skip
	}

	provider, err := getCloudStorageProvider(uri.Scheme)
	if err != nil {
		test.T().Logf("Cloud checkpoint prep: skip (%v)", err)
		return nil
	}

	ctx := test.Ctx()
	if err := provider.PrepareStorage(ctx, uri); err != nil {
		test.T().Logf("Cloud checkpoint prep warning: %v", err)
		return nil
	}

	test.T().Logf("Cloud checkpoint prep complete: cleaned and created %s://%s/%s/.keep", uri.Scheme, uri.Bucket, uri.Prefix)
	return nil
}

// CleanupCloudCheckpointStorage cleans up cloud checkpoint storage by deleting all objects
// under the specified prefix. Returns the number of objects deleted.
// Errors are logged but do not cause test failures, allowing tests to continue even if
// cleanup operations fail.
func CleanupCloudCheckpointStorage(test Test, checkpointURI string) int {
	test.T().Helper()

	uri := ParseCloudURI(checkpointURI)
	if uri == nil {
		test.T().Logf("Cloud checkpoint cleanup: skip (not cloud storage)")
		return 0
	}

	provider, err := getCloudStorageProvider(uri.Scheme)
	if err != nil {
		test.T().Logf("Cloud checkpoint cleanup: skip (%v)", err)
		return 0
	}

	ctx := test.Ctx()
	deleted, err := provider.CleanupStorage(ctx, uri)
	if err != nil {
		test.T().Logf("Cloud checkpoint cleanup warning: %v", err)
		return deleted
	}

	test.T().Logf("Cloud checkpoint cleanup: deleted %d objects from %s://%s/%s", deleted, uri.Scheme, uri.Bucket, uri.Prefix)
	return deleted
}

// Package-level variables to track dynamically created buckets
var (
	dynamicallyCreatedBuckets = make(map[string]bool)
	bucketCreationMutex       sync.Mutex
)

// GetOrCreateS3Bucket returns a shared S3 bucket for all S3 checkpoint tests.
// The first call creates a new bucket and stores it in sharedS3BucketName.
// Subsequent calls reuse the same bucket across all test namespaces.
// The bucket is cleaned up after all tests complete via TestMain cleanup.
var sharedS3BucketName string

func GetOrCreateS3Bucket(test Test) (string, error) {
	test.T().Helper()

	bucketCreationMutex.Lock()
	defer bucketCreationMutex.Unlock()

	// Reuse the same bucket if it already exists
	if sharedS3BucketName != "" {
		// Verify the bucket still exists
		provider, err := NewS3Provider()
		if err == nil {
			ctx := test.Ctx()
			exists, err := provider.BucketExists(ctx, sharedS3BucketName)
			if err == nil && exists {
				test.T().Logf("Reusing existing shared S3 bucket: %s", sharedS3BucketName)
				return sharedS3BucketName, nil
			}
		}
	}

	// Create a new shared bucket for all S3 tests
	timestamp := time.Now().Unix()
	bucketName := fmt.Sprintf("test-checkpoints-%d", timestamp)

	// Create S3 provider
	provider, err := NewS3Provider()
	if err != nil {
		return "", fmt.Errorf("failed to create S3 provider: %w", err)
	}

	// Create the bucket
	ctx := test.Ctx()
	if err := provider.CreateBucket(ctx, bucketName); err != nil {
		return "", fmt.Errorf("failed to create bucket %s: %w", bucketName, err)
	}

	// Track this bucket for cleanup and store as shared bucket
	dynamicallyCreatedBuckets[bucketName] = true
	sharedS3BucketName = bucketName
	test.T().Logf("Created shared S3 bucket for all S3 tests: %s (will be cleaned up after all tests)", bucketName)

	return bucketName, nil
}

// isS3Configured checks if all required S3 credentials are configured
func isS3Configured() bool {
	endpoint, _ := GetStorageBucketDefaultEndpoint()
	accessKey, _ := GetStorageBucketAccessKeyId()
	secretKey, _ := GetStorageBucketSecretKey()
	return endpoint != "" && accessKey != "" && secretKey != ""
}

// CheckpointExistsInS3 verifies that at least one checkpoint object exists in S3 storage.
// Returns true if checkpoints are found, false otherwise. Errors are logged but do not cause failures.
func CheckpointExistsInS3(test Test, checkpointURI string) bool {
	test.T().Helper()

	uri := ParseCloudURI(checkpointURI)
	if uri == nil || uri.Scheme != "s3" {
		return false // Not an S3 URI
	}

	provider, err := NewS3Provider()
	if err != nil {
		test.T().Logf("Failed to create S3 provider to verify checkpoints: %v", err)
		return false
	}

	client, err := provider.GetS3Client()
	if err != nil {
		test.T().Logf("Failed to create S3 client to verify checkpoints: %v", err)
		return false
	}

	// List objects under the checkpoint prefix (exclude .keep file)
	fullPrefix := strings.TrimSuffix(uri.Prefix, "/") + "/"
	objectsCh := client.ListObjects(test.Ctx(), uri.Bucket, minio.ListObjectsOptions{
		Prefix:    fullPrefix,
		Recursive: true,
	})

	objectCount := 0
	for object := range objectsCh {
		if object.Err != nil {
			test.T().Logf("Error listing objects in S3: %v", object.Err)
			return false
		}
		// Skip .keep file and .incomplete markers
		if !strings.HasSuffix(object.Key, "/.keep") && !strings.Contains(object.Key, ".incomplete") {
			objectCount++
		}
	}

	return objectCount > 0
}

// CleanupDynamicallyCreatedBuckets deletes all buckets that were created dynamically during tests.
// This should be called after all S3 tests complete.
// Only attempts cleanup if S3 credentials are fully configured (endpoint, accessKey, secretKey).
// This function can be called from TestMain where a Test interface may not be available.
func CleanupDynamicallyCreatedBuckets() {
	bucketCreationMutex.Lock()
	defer bucketCreationMutex.Unlock()

	if len(dynamicallyCreatedBuckets) == 0 {
		return
	}

	// Only cleanup if S3 credentials are fully configured
	// If credentials aren't configured, tests wouldn't have run anyway, so no buckets to clean
	if !isS3Configured() {
		fmt.Printf("S3 credentials not configured, skipping bucket cleanup\n")
		return
	}

	provider, err := NewS3Provider()
	if err != nil {
		fmt.Printf("Failed to create S3 provider for bucket cleanup: %v\n", err)
		return
	}

	ctx := context.Background()
	for bucketName := range dynamicallyCreatedBuckets {
		fmt.Printf("Cleaning up dynamically created bucket: %s\n", bucketName)
		if err := provider.DeleteBucket(ctx, bucketName); err != nil {
			fmt.Printf("Warning: failed to delete bucket %s: %v\n", bucketName, err)
		} else {
			fmt.Printf("Successfully deleted bucket: %s\n", bucketName)
		}
		delete(dynamicallyCreatedBuckets, bucketName)
	}
}
