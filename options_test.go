package mysql

import (
	"testing"
	"time"
)

// TestDefaultOptions verifies the behavior of the defaultOptions function.
// It ensures that default values are correctly applied, custom values are respected,
// and edge cases like zero values are handled properly.
func TestDefaultOptions(t *testing.T) {
	// Test case: verify that default values are set correctly when no options are provided
	t.Run("default values", func(t *testing.T) {
		// Call defaultOptions without arguments to get pure defaults
		opts := defaultOptions()

		// Verify cache-specific default values
		if opts.CacheSize != 10 {
			t.Errorf("Expected CacheSize 10, got %d", opts.CacheSize)
		}

		if opts.CacheTTLCheck != 5*time.Minute {
			t.Errorf("Expected CacheTTLCheck 5m, got %v", opts.CacheTTLCheck)
		}

		// Cache should be disabled by default for safety
		if opts.CacheEnabled != false {
			t.Errorf("Expected CacheEnabled false, got %v", opts.CacheEnabled)
		}
	})

	// Test case: verify that custom cache values override defaults
	t.Run("custom cache values", func(t *testing.T) {
		// Define custom options with specific cache settings
		customOpts := Options{
			CacheSize:     50,               // Custom cache size in MB
			CacheTTLCheck: 10 * time.Minute, // Custom cleanup interval
			CacheEnabled:  true,             // Explicitly enable caching
		}

		// Apply custom options
		opts := defaultOptions(customOpts)

		// Verify custom values are preserved
		if opts.CacheSize != 50 {
			t.Errorf("Expected CacheSize 50, got %d", opts.CacheSize)
		}

		if opts.CacheTTLCheck != 10*time.Minute {
			t.Errorf("Expected CacheTTLCheck 10m, got %v", opts.CacheTTLCheck)
		}

		if opts.CacheEnabled != true {
			t.Errorf("Expected CacheEnabled true, got %v", opts.CacheEnabled)
		}
	})

	// Test case: verify that zero values in custom options use defaults
	// This ensures backward compatibility and safe behavior for unspecified fields
	t.Run("zero values should use defaults", func(t *testing.T) {
		customOpts := Options{
			CacheSize:     0, // Zero value, should use default
			CacheTTLCheck: 0, // Zero value, should use default
		}

		opts := defaultOptions(customOpts)

		// Should fall back to defaults for zero values
		if opts.CacheSize != 10 {
			t.Errorf("Expected CacheSize 10, got %d", opts.CacheSize)
		}

		if opts.CacheTTLCheck != 5*time.Minute {
			t.Errorf("Expected CacheTTLCheck 5m, got %v", opts.CacheTTLCheck)
		}
	})
}

// TestConnectionStringGeneration tests the connection string generation logic.
// It verifies that custom connection strings are preserved and that
// other options are still applied when a connection string is provided.
func TestConnectionStringGeneration(t *testing.T) {
	// Test case: when a custom connection string is provided, it should be used as-is
	// This allows users to bypass the automatic generation for advanced use cases
	t.Run("with custom connection string", func(t *testing.T) {
		customOpts := Options{
			ConnectionString: "custom_connection_string", // Pre-built DSN
			CacheSize:        20,                         // Should still be applied
			CacheTTLCheck:    time.Minute,
		}

		opts := defaultOptions(customOpts)

		// Custom connection string should be preserved
		if opts.ConnectionString != "custom_connection_string" {
			t.Errorf("Expected custom connection string, got %s", opts.ConnectionString)
		}

		// Cache settings should still be applied even with custom connection string
		if opts.CacheSize != 20 {
			t.Errorf("Expected CacheSize 20, got %d", opts.CacheSize)
		}
	})
}

// BenchmarkDefaultOptions measures the performance of the defaultOptions function
// under different usage patterns to ensure it doesn't become a bottleneck.
func BenchmarkDefaultOptions(b *testing.B) {
	// Benchmark: creating options with no arguments (fast path)
	b.Run("empty options", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = defaultOptions()
		}
	})

	// Benchmark: creating options with custom values (typical use case)
	// This measures the merge logic performance
	b.Run("with custom options", func(b *testing.B) {
		customOpts := Options{
			CacheSize:     100,
			CacheTTLCheck: time.Hour,
			CacheEnabled:  true,
			Host:          "example.com",
			Port:          3307,
		}

		for i := 0; i < b.N; i++ {
			_ = defaultOptions(customOpts)
		}
	})

	// Benchmark: creating options with a pre-built connection string
	// This should be faster since it skips DSN generation
	b.Run("with connection string", func(b *testing.B) {
		customOpts := Options{
			ConnectionString: "user:pass@tcp(host:3306)/db",
			CacheSize:        50,
		}

		for i := 0; i < b.N; i++ {
			_ = defaultOptions(customOpts)
		}
	})
}