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

// parseCloudURI parses a cloud storage URI into scheme, bucket, and prefix.
// Excludes PVC URIs (pvc://) and local filesystem paths (no scheme).
// Returns nil if not a valid cloud storage URI.
func parseCloudURI(uri string) *CloudURI {
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

// getS3Client creates an S3 client with the provider's credentials
func (p *S3Provider) getS3Client() (*minio.Client, error) {
	endpointURL := p.endpoint
	secure := true
	if !strings.HasPrefix(endpointURL, "http") {
		endpointURL = "https://" + endpointURL
	} else if strings.HasPrefix(endpointURL, "http://") {
		secure = false
	}

	// Extract host:port from URL (remove http:// or https://)
	endpoint := strings.TrimPrefix(strings.TrimPrefix(endpointURL, "https://"), "http://")

	// Configure TLS verification based on environment variables
	// CHECKPOINT_VERIFY_SSL or VERIFY_SSL can be set to "true" to enable certificate verification
	// Defaults to false to support test environments with self-signed certificates
	verifySSL := false
	if verifySSLEnv := os.Getenv("CHECKPOINT_VERIFY_SSL"); verifySSLEnv != "" {
		if val, err := strconv.ParseBool(verifySSLEnv); err == nil {
			verifySSL = val
		}
	} else if verifySSLEnv := os.Getenv("VERIFY_SSL"); verifySSLEnv != "" {
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

	client, err := p.getS3Client()
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

	client, err := p.getS3Client()
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

	uri := parseCloudURI(checkpointURI)
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

	uri := parseCloudURI(checkpointURI)
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
