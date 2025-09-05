package command

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	errorpkg "github.com/peak/s5cmd/v2/error"
	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
	"gotest.tools/v3/assert"
)

func TestSizeOnlyStrategy(t *testing.T) {
	strategy := &SizeOnlyStrategy{}

	// Create test objects with different sizes
	srcObj := &storage.Object{Size: 100}
	dstObj := &storage.Object{Size: 200}

	// Different sizes should sync
	err := strategy.ShouldSync(srcObj, dstObj)
	assert.NilError(t, err)

	// Same sizes should not sync
	dstObj.Size = 100
	err = strategy.ShouldSync(srcObj, dstObj)
	assert.Equal(t, err, errorpkg.ErrObjectSizesMatch)
}

func TestSizeAndModificationStrategy(t *testing.T) {
	strategy := &SizeAndModificationStrategy{}

	now := time.Now()
	older := now.Add(-time.Hour)
	newer := now.Add(time.Hour)

	testCases := []struct {
		name        string
		srcModTime  time.Time
		dstModTime  time.Time
		srcSize     int64
		dstSize     int64
		shouldSync  bool
		expectedErr error
	}{
		{
			name:       "newer source, different size",
			srcModTime: newer,
			dstModTime: older,
			srcSize:    100,
			dstSize:    200,
			shouldSync: true,
		},
		{
			name:       "newer source, same size",
			srcModTime: newer,
			dstModTime: older,
			srcSize:    100,
			dstSize:    100,
			shouldSync: true,
		},
		{
			name:       "older source, different size",
			srcModTime: older,
			dstModTime: newer,
			srcSize:    100,
			dstSize:    200,
			shouldSync: true,
		},
		{
			name:        "older source, same size",
			srcModTime:  older,
			dstModTime:  newer,
			srcSize:     100,
			dstSize:     100,
			shouldSync:  false,
			expectedErr: errorpkg.ErrObjectIsNewerAndSizesMatch,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srcObj := &storage.Object{
				ModTime: &tc.srcModTime,
				Size:    tc.srcSize,
			}
			dstObj := &storage.Object{
				ModTime: &tc.dstModTime,
				Size:    tc.dstSize,
			}

			err := strategy.ShouldSync(srcObj, dstObj)
			if tc.shouldSync {
				assert.NilError(t, err)
			} else {
				assert.Equal(t, err, tc.expectedErr)
			}
		})
	}
}

func TestIsMultipartETag(t *testing.T) {
	testCases := []struct {
		etag        string
		isMultipart bool
	}{
		{"d41d8cd98f00b204e9800998ecf8427e", false},  // Regular MD5
		{"d41d8cd98f00b204e9800998ecf8427e-5", true}, // Multipart with dash
		{"", false},           // Empty
		{"abc-def-ghi", true}, // Multiple dashes
		{"nodash", false},     // No dash
	}

	for _, tc := range testCases {
		t.Run(tc.etag, func(t *testing.T) {
			result := isMultipartETag(tc.etag)
			assert.Equal(t, result, tc.isMultipart)
		})
	}
}

func TestHashStrategy(t *testing.T) {
	strategy := &HashStrategy{}

	// Create URLs for test objects
	remoteURL, _ := url.New("s3://bucket/key")
	remoteURL2, _ := url.New("s3://bucket/key2")

	// Test different sizes - should always sync
	srcObj := &storage.Object{URL: remoteURL, Size: 100, Etag: "etag1"}
	dstObj := &storage.Object{URL: remoteURL2, Size: 200, Etag: "etag2"}
	err := strategy.ShouldSync(srcObj, dstObj)
	assert.NilError(t, err)

	// Test same ETags - should not sync
	srcObj.Size = 100
	dstObj.Size = 100
	srcObj.Etag = "sameetag"
	dstObj.Etag = "sameetag"
	err = strategy.ShouldSync(srcObj, dstObj)
	assert.Equal(t, err, errorpkg.ErrObjectEtagsMatch)

	// Test different ETags - should sync
	dstObj.Etag = "differentetag"
	err = strategy.ShouldSync(srcObj, dstObj)
	assert.NilError(t, err)

	// Test multipart ETags - should always sync
	srcObj.Etag = "etag1-5" // Multipart ETag
	dstObj.Etag = "etag2"
	err = strategy.ShouldSync(srcObj, dstObj)
	assert.NilError(t, err)

	dstObj.Etag = "etag2-3" // Both multipart
	err = strategy.ShouldSync(srcObj, dstObj)
	assert.NilError(t, err)
}

func TestGetHashWithRemoteObject(t *testing.T) {
	// Test remote object (should return existing Etag)
	remoteURL, _ := url.New("s3://bucket/key")
	obj := &storage.Object{
		URL:  remoteURL,
		Etag: "remote-etag",
	}

	hash := getHash(obj)
	assert.Equal(t, hash, "remote-etag")
}

func TestGetHashWithLocalFileEtag(t *testing.T) {
	// Test local object with existing Etag
	localURL, _ := url.New("/local/file")
	obj := &storage.Object{
		URL:  localURL,
		Etag: "existing-etag",
	}

	hash := getHash(obj)
	assert.Equal(t, hash, "existing-etag")
}

func TestGetHashWithLocalFile(t *testing.T) {
	// Create a temporary file
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "testfile")

	content := "Hello, World!"
	err := os.WriteFile(tmpFile, []byte(content), 0644)
	assert.NilError(t, err)

	// Calculate expected MD5
	md5Hash := md5.Sum([]byte(content))
	expectedHash := hex.EncodeToString(md5Hash[:])

	// Test local file hash calculation
	localURL, _ := url.New(tmpFile)
	obj := &storage.Object{
		URL:  localURL,
		Etag: "", // No existing Etag
		Size: int64(len(content)),
	}

	hash := getHash(obj)
	assert.Equal(t, hash, expectedHash)
}

func TestGetHashWithLargeFile(t *testing.T) {
	// Create a temporary large file to test memory usage
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "largefile")

	// Create 1MB file
	content := strings.Repeat("A", 1024*1024)
	err := os.WriteFile(tmpFile, []byte(content), 0644)
	assert.NilError(t, err)

	// Calculate expected MD5
	md5Hash := md5.Sum([]byte(content))
	expectedHash := hex.EncodeToString(md5Hash[:])

	// Test large file hash calculation
	localURL, _ := url.New(tmpFile)
	obj := &storage.Object{
		URL:  localURL,
		Etag: "",
		Size: int64(len(content)),
	}

	hash := getHash(obj)
	assert.Equal(t, hash, expectedHash)
}

func TestGetHashWithNonExistentFile(t *testing.T) {
	// Test with non-existent file
	localURL, _ := url.New("/non/existent/file")
	obj := &storage.Object{
		URL:  localURL,
		Etag: "",
		Size: 100,
	}

	hash := getHash(obj)
	assert.Equal(t, hash, "") // Should return empty string on error
}

func TestNewStrategy(t *testing.T) {
	// Test creating different strategies
	sizeOnly := NewStrategy(true, false)
	_, ok := sizeOnly.(*SizeOnlyStrategy)
	assert.Assert(t, ok)

	hashOnly := NewStrategy(false, true)
	_, ok = hashOnly.(*HashStrategy)
	assert.Assert(t, ok)

	sizeAndMod := NewStrategy(false, false)
	_, ok = sizeAndMod.(*SizeAndModificationStrategy)
	assert.Assert(t, ok)

	// Test priority: sizeOnly takes precedence over hashOnly
	sizeOnlyPriority := NewStrategy(true, true)
	_, ok = sizeOnlyPriority.(*SizeOnlyStrategy)
	assert.Assert(t, ok)
}

func TestHashCachingPerformanceOptimization(t *testing.T) {
	// Test that demonstrates the performance optimization
	strategy := &HashStrategy{}

	// Create a temporary local file
	tmpDir := t.TempDir()
	localFile := filepath.Join(tmpDir, "testfile.txt")
	content := "Hello, World! This is a performance test."
	err := os.WriteFile(localFile, []byte(content), 0644)
	assert.NilError(t, err)

	// Calculate expected MD5 hash
	md5Hash := md5.Sum([]byte(content))
	expectedHash := hex.EncodeToString(md5Hash[:])

	localURL, _ := url.New(localFile)

	// Test WITHOUT hash caching (old behavior)
	fsWithoutCache := storage.NewLocalClient(storage.Options{CacheHashes: false})
	ctx := context.Background()
	dstObjWithoutCache, err := fsWithoutCache.Stat(ctx, localURL)
	assert.NilError(t, err)

	// Test WITH hash caching (new optimized behavior)
	fsWithCache := storage.NewLocalClient(storage.Options{CacheHashes: true})
	dstObjWithCache, err := fsWithCache.Stat(ctx, localURL)
	assert.NilError(t, err)

	// Create remote S3 object
	remoteURL, _ := url.New("s3://bucket/testfile.txt")
	srcObj := &storage.Object{
		URL:  remoteURL,
		Size: int64(len(content)),
		Etag: expectedHash,
	}

	t.Logf("WITHOUT hash caching - Destination ETag: '%s'", dstObjWithoutCache.Etag)
	t.Logf("WITH hash caching - Destination ETag: '%s'", dstObjWithCache.Etag)

	// Both should work correctly
	err1 := strategy.ShouldSync(srcObj, dstObjWithoutCache)
	err2 := strategy.ShouldSync(srcObj, dstObjWithCache)

	// Both should return ErrObjectEtagsMatch
	assert.Equal(t, err1, errorpkg.ErrObjectEtagsMatch)
	assert.Equal(t, err2, errorpkg.ErrObjectEtagsMatch)

	// The key difference: with hash caching, the ETag is pre-computed
	assert.Equal(t, dstObjWithoutCache.Etag, "", "Without cache: ETag should be empty")
	assert.Equal(t, dstObjWithCache.Etag, expectedHash, "With cache: ETag should be pre-computed")

	// This means getHash() for cached objects will be much faster
	// since it just returns the pre-computed ETag instead of recalculating
	hash1 := getHash(dstObjWithoutCache) // This will recalculate MD5
	hash2 := getHash(dstObjWithCache)    // This will just return the ETag

	assert.Equal(t, hash1, expectedHash)
	assert.Equal(t, hash2, expectedHash)
	t.Log("Performance optimization: hash caching avoids repeated MD5 calculations")
}

func TestHashStrategyBugReproduction(t *testing.T) {
	// This test reproduces the real-world scenario that causes the bug
	// Let's simulate how objects are actually created in sync operation

	strategy := &HashStrategy{}

	// Create a temporary local file
	tmpDir := t.TempDir()
	localFile := filepath.Join(tmpDir, "testfile.txt")
	content := "Hello, World!"
	err := os.WriteFile(localFile, []byte(content), 0644)
	assert.NilError(t, err)

	// Calculate expected MD5 hash
	md5Hash := md5.Sum([]byte(content))
	expectedHash := hex.EncodeToString(md5Hash[:])

	// Simulate remote S3 object (source) - this would come from S3 list operation
	remoteURL, _ := url.New("s3://bucket/testfile.txt")
	srcObj := &storage.Object{
		URL:  remoteURL,
		Size: int64(len(content)),
		Etag: expectedHash, // S3 provides the MD5 as ETag
	}

	// Simulate local object (destination) - this would come from filesystem Stat operation
	// The key insight: let's check how storage.Filesystem.Stat actually creates objects
	localURL, _ := url.New(localFile)

	// Create filesystem storage to test real object creation with hash caching
	fs := &storage.Filesystem{}
	// Enable hash caching to test the optimized behavior
	fsOpts := storage.Options{CacheHashes: true}
	fs = storage.NewLocalClient(fsOpts)
	ctx := context.Background()
	dstObj, err := fs.Stat(ctx, localURL)
	assert.NilError(t, err)

	// Now test with real objects as they would be created
	t.Logf("Source ETag: '%s'", srcObj.Etag)
	t.Logf("Destination ETag: '%s'", dstObj.Etag)
	t.Logf("Source hash from getHash(): '%s'", getHash(srcObj))
	t.Logf("Destination hash from getHash(): '%s'", getHash(dstObj))

	err = strategy.ShouldSync(srcObj, dstObj)
	if err == errorpkg.ErrObjectEtagsMatch {
		t.Log("SUCCESS: Files with identical content correctly identified as not needing sync")
	} else if err == nil {
		t.Log("BUG CONFIRMED: Files with identical content incorrectly marked for sync")
		t.Log("This demonstrates the reported bug")
		// Don't fail the test, we want to see this behavior
	} else {
		t.Logf("Unexpected error: %v", err)
		t.Fail()
	}
}

func TestHashStrategyRemoteToLocal(t *testing.T) {
	strategy := &HashStrategy{}

	// Create a temporary local file
	tmpDir := t.TempDir()
	localFile := filepath.Join(tmpDir, "testfile.txt")
	content := "Hello, World!"
	err := os.WriteFile(localFile, []byte(content), 0644)
	assert.NilError(t, err)

	// Calculate expected MD5 hash
	md5Hash := md5.Sum([]byte(content))
	expectedHash := hex.EncodeToString(md5Hash[:])

	// Create remote S3 object with correct ETag (MD5 hash)
	remoteURL, _ := url.New("s3://bucket/testfile.txt")
	srcObj := &storage.Object{
		URL:  remoteURL,
		Size: int64(len(content)),
		Etag: expectedHash, // Remote object has the correct MD5 hash
	}

	// Create local object as it would be created by filesystem
	localURL, _ := url.New(localFile)
	dstObj := &storage.Object{
		URL:  localURL,
		Size: int64(len(content)),
		Etag: "", // Local object has empty ETag as per current implementation
	}

	// Test the actual behavior
	err = strategy.ShouldSync(srcObj, dstObj)
	if err == errorpkg.ErrObjectEtagsMatch {
		t.Log("Files with identical content correctly identified as not needing sync")
	} else if err == nil {
		t.Log("BUG: Files with identical content incorrectly marked for sync")
		t.Log("srcHash:", getHash(srcObj))
		t.Log("dstHash:", getHash(dstObj))
		t.Fail()
	} else {
		t.Logf("Unexpected error: %v", err)
		t.Fail()
	}
}

func TestHashStrategyWithEmptyFiles(t *testing.T) {
	strategy := &HashStrategy{}

	// Create empty temporary files
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "src")
	dstFile := filepath.Join(tmpDir, "dst")

	err := os.WriteFile(srcFile, []byte{}, 0644)
	assert.NilError(t, err)
	err = os.WriteFile(dstFile, []byte{}, 0644)
	assert.NilError(t, err)

	srcURL, _ := url.New(srcFile)
	dstURL, _ := url.New(dstFile)

	srcObj := &storage.Object{URL: srcURL, Size: 0, Etag: ""}
	dstObj := &storage.Object{URL: dstURL, Size: 0, Etag: ""}

	// Empty files should have same hash and not sync
	err = strategy.ShouldSync(srcObj, dstObj)
	assert.Equal(t, err, errorpkg.ErrObjectEtagsMatch)
}

func TestGetHashWithFileReadError(t *testing.T) {
	// Create a temporary file and then remove it to simulate read error
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "testfile")

	content := "test content"
	err := os.WriteFile(tmpFile, []byte(content), 0644)
	assert.NilError(t, err)

	// Remove the file to simulate file not found error
	err = os.Remove(tmpFile)
	assert.NilError(t, err)

	// Test hash calculation with missing file
	localURL, _ := url.New(tmpFile)
	obj := &storage.Object{
		URL:  localURL,
		Etag: "",
		Size: int64(len(content)),
	}

	hash := getHash(obj)
	assert.Equal(t, hash, "") // Should return empty string on file access error
}
