package mysql

import (
	"testing"
	"time"
)

func TestDefaultOptions(t *testing.T) {
	t.Run("default values", func(t *testing.T) {
		opts := defaultOptions()

		if opts.CacheSize != 10 {
			t.Errorf("Expected CacheSize 10, got %d", opts.CacheSize)
		}

		if opts.CacheTTLCheck != 5*time.Minute {
			t.Errorf("Expected CacheTTLCheck 5m, got %v", opts.CacheTTLCheck)
		}

		if opts.CacheEnabled != false {
			t.Errorf("Expected CacheEnabled false, got %v", opts.CacheEnabled)
		}
	})

	t.Run("custom cache values", func(t *testing.T) {
		customOpts := Options{
			CacheSize:     50,
			CacheTTLCheck: 10 * time.Minute,
			CacheEnabled:  true,
		}

		opts := defaultOptions(customOpts)

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

	t.Run("zero values should use defaults", func(t *testing.T) {
		customOpts := Options{
			CacheSize:     0, // Should use default
			CacheTTLCheck: 0, // Should use default
		}

		opts := defaultOptions(customOpts)

		if opts.CacheSize != 10 {
			t.Errorf("Expected CacheSize 10, got %d", opts.CacheSize)
		}

		if opts.CacheTTLCheck != 5*time.Minute {
			t.Errorf("Expected CacheTTLCheck 5m, got %v", opts.CacheTTLCheck)
		}
	})
}

func TestConnectionStringGeneration(t *testing.T) {
	t.Run("with custom connection string", func(t *testing.T) {
		customOpts := Options{
			ConnectionString: "custom_connection_string",
			CacheSize:        20,
			CacheTTLCheck:    time.Minute,
		}

		opts := defaultOptions(customOpts)

		if opts.ConnectionString != "custom_connection_string" {
			t.Errorf("Expected custom connection string, got %s", opts.ConnectionString)
		}

		// Cache settings should still be applied
		if opts.CacheSize != 20 {
			t.Errorf("Expected CacheSize 20, got %d", opts.CacheSize)
		}
	})
}

func BenchmarkDefaultOptions(b *testing.B) {
	b.Run("empty options", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = defaultOptions()
		}
	})

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
