package command

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
)

// BenchmarkHashCalculationWithoutCache benchmarks the old behavior where
// MD5 is calculated every time getHash() is called
func BenchmarkHashCalculationWithoutCache(b *testing.B) {
	// Create a test file
	tmpDir := b.TempDir()
	testFile := filepath.Join(tmpDir, "benchmark_file.txt")
	content := "This is a benchmark test file with some content to hash."
	err := os.WriteFile(testFile, []byte(content), 0644)
	if err != nil {
		b.Fatal(err)
	}

	// Create filesystem without hash caching
	fs := storage.NewLocalClient(storage.Options{CacheHashes: false})
	testURL, _ := url.New(testFile)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj, err := fs.Stat(ctx, testURL)
		if err != nil {
			b.Fatal(err)
		}
		// This will trigger MD5 calculation every time
		_ = getHash(obj)
	}
}

// BenchmarkHashCalculationWithCache benchmarks the optimized behavior where
// MD5 is calculated once and cached in the ETag field
func BenchmarkHashCalculationWithCache(b *testing.B) {
	// Create a test file
	tmpDir := b.TempDir()
	testFile := filepath.Join(tmpDir, "benchmark_file.txt")
	content := "This is a benchmark test file with some content to hash."
	err := os.WriteFile(testFile, []byte(content), 0644)
	if err != nil {
		b.Fatal(err)
	}

	// Create filesystem with hash caching enabled
	fs := storage.NewLocalClient(storage.Options{CacheHashes: true})
	testURL, _ := url.New(testFile)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj, err := fs.Stat(ctx, testURL)
		if err != nil {
			b.Fatal(err)
		}
		// This will just return the cached ETag
		_ = getHash(obj)
	}
}

// BenchmarkSyncStrategyWithoutCache benchmarks sync strategy performance without caching
func BenchmarkSyncStrategyWithoutCache(b *testing.B) {
	strategy := &HashStrategy{}

	// Create test files
	tmpDir := b.TempDir()
	testFile := filepath.Join(tmpDir, "strategy_benchmark.txt")
	content := "Test content for sync strategy benchmark"
	err := os.WriteFile(testFile, []byte(content), 0644)
	if err != nil {
		b.Fatal(err)
	}

	fs := storage.NewLocalClient(storage.Options{CacheHashes: false})
	testURL, _ := url.New(testFile)
	ctx := context.Background()

	// Create source and destination objects
	srcObj, err := fs.Stat(ctx, testURL)
	if err != nil {
		b.Fatal(err)
	}
	dstObj, err := fs.Stat(ctx, testURL)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = strategy.ShouldSync(srcObj, dstObj)
	}
}

// BenchmarkSyncStrategyWithCache benchmarks sync strategy performance with caching
func BenchmarkSyncStrategyWithCache(b *testing.B) {
	strategy := &HashStrategy{}

	// Create test files
	tmpDir := b.TempDir()
	testFile := filepath.Join(tmpDir, "strategy_benchmark.txt")
	content := "Test content for sync strategy benchmark"
	err := os.WriteFile(testFile, []byte(content), 0644)
	if err != nil {
		b.Fatal(err)
	}

	fs := storage.NewLocalClient(storage.Options{CacheHashes: true})
	testURL, _ := url.New(testFile)
	ctx := context.Background()

	// Create source and destination objects
	srcObj, err := fs.Stat(ctx, testURL)
	if err != nil {
		b.Fatal(err)
	}
	dstObj, err := fs.Stat(ctx, testURL)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = strategy.ShouldSync(srcObj, dstObj)
	}
}
