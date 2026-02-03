package mysql

import (
	"fmt"
	"time"
)

// Storage defines the interface for key-value storage with expiration support.
// Implementations can be used for caching, persistence, or other storage needs.
type Storage interface {
	// Get retrieves a value by its key. Returns an error if key doesn't exist or has expired.
	Get(key string) ([]byte, error)

	// Set stores a key-value pair with optional expiration.
	// If exp is 0, the entry never expires. If negative, behavior is implementation-defined.
	Set(key string, val []byte, exp time.Duration) error

	// Delete removes a key-value pair from storage.
	// Returns an error if the key doesn't exist or deletion fails.
	Delete(key string) error

	// Reset clears all entries from storage.
	// Returns an error if the operation fails.
	Reset() error

	// Close releases any resources held by the storage implementation.
	// The storage should not be used after calling Close.
	Close() error
}

// Mutex defines the interface for key-based mutual exclusion.
// This allows synchronization on specific resources identified by string keys.
type Mutex interface {
	// Lock acquires a lock for the given key. Blocks until the lock is available.
	// Returns an error if the lock cannot be acquired.
	Lock(key string) error

	// Unlock releases the lock for the given key.
	// Returns an error if the key isn't locked or unlock fails.
	Unlock(key string) error
}

// Options configures the MySQL database connection and associated features.
// All fields are optional; zero values use sensible defaults.
// When ConnectionString is provided, most other connection-related fields are ignored.
type Options struct {
	// Connection configuration
	Host     string // Database server hostname or IP address (default: "localhost")
	Username string // Authentication username (required)
	Password string // Authentication password (required)
	Database string // Database name to connect to (required)
	Port     int    // TCP port number (default: 3306)

	// Connection pooling
	MaxConnections int // Maximum number of open connections (0 = driver default)

	// Character set configuration
	Charset   string // Connection charset (default: "utf8mb4")
	Collation string // Connection collation (default: "utf8mb4_unicode_ci")

	// Timeout settings (in seconds)
	Timeout      int // Connection timeout (default: 30)
	ReadTimeout  int // Read operation timeout (default: 30)
	WriteTimeout int // Write operation timeout (default: 30)

	// Cache configuration
	Cache         Storage       // Custom cache implementation (nil uses default in-memory cache)
	CacheEnabled  bool          // Enable query caching (default: false)
	CacheSize     int           // Maximum cache size in megabytes (default: 10)
	CacheTTLCheck time.Duration // Interval for cache cleanup (default: 5 minutes)

	// Concurrency control
	Mutex Mutex // Custom mutex implementation for distributed locking

	// Serialization
	Codec Codec // Custom codec for data serialization (nil uses default MessagePack)

	// Advanced
	ConnectionString string // Pre-built DSN; if set, overrides individual connection fields
}

// defaultOptions creates and returns Options with sensible defaults.
// It merges user-provided options with defaults, generating a connection string
// if one isn't explicitly provided. This function ensures all required fields
// have valid values before establishing a database connection.
//
// Use this function when initializing a MySQL connection to ensure proper
// configuration even when users don't specify all options.
func defaultOptions(opts ...Options) Options {
	// Initialize with defaults
	options := Options{
		Host:           "localhost",
		Port:           3306,
		Charset:        "utf8mb4",
		Collation:      "utf8mb4_unicode_ci",
		Timeout:        30,
		ReadTimeout:    30,
		WriteTimeout:   30,
		CacheSize:      10,              // 10 MB default cache size
		CacheTTLCheck:  5 * time.Minute, // Check every 5 minutes
		CacheEnabled:   false,           // Cache disabled by default
		MaxConnections: 0,               // Use driver's default pool size
	}

	// Merge user-provided options if any
	if len(opts) > 0 {
		userOpts := opts[0]

		// Connection fields with validation
		if userOpts.Host != "" {
			options.Host = userOpts.Host
		}
		if userOpts.Username != "" {
			options.Username = userOpts.Username
		}
		if userOpts.Password != "" {
			options.Password = userOpts.Password
		}
		if userOpts.Database != "" {
			options.Database = userOpts.Database
		}
		if userOpts.Port > 0 {
			options.Port = userOpts.Port
		}

		// Connection pooling
		if userOpts.MaxConnections > 0 {
			options.MaxConnections = userOpts.MaxConnections
		}

		// Character set configuration
		if userOpts.Charset != "" {
			options.Charset = userOpts.Charset
		}
		if userOpts.Collation != "" {
			options.Collation = userOpts.Collation
		}

		// Timeout configuration
		if userOpts.Timeout > 0 {
			options.Timeout = userOpts.Timeout
		}
		if userOpts.ReadTimeout > 0 {
			options.ReadTimeout = userOpts.ReadTimeout
		}
		if userOpts.WriteTimeout > 0 {
			options.WriteTimeout = userOpts.WriteTimeout
		}

		// Cache configuration
		if userOpts.CacheSize > 0 {
			options.CacheSize = userOpts.CacheSize
		}
		if userOpts.CacheTTLCheck > 0 {
			options.CacheTTLCheck = userOpts.CacheTTLCheck
		}

		// Direct assignment for interface and boolean fields
		options.Cache = userOpts.Cache
		options.CacheEnabled = userOpts.CacheEnabled
		options.Mutex = userOpts.Mutex
		options.Codec = userOpts.Codec
		options.ConnectionString = userOpts.ConnectionString
	}

	// Generate connection string if not provided
	if options.ConnectionString == "" {
		// Base DSN with required parameters
		options.ConnectionString = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
			options.Username, options.Password, options.Host, options.Port, options.Database)

		// Add charset configuration
		if options.Charset != "" {
			options.ConnectionString += "&charset=" + options.Charset
		}

		// Add collation configuration
		if options.Collation != "" {
			options.ConnectionString += "&collation=" + options.Collation
		}

		// Add timeout configurations
		if options.Timeout > 0 {
			options.ConnectionString += fmt.Sprintf("&timeout=%ds", options.Timeout)
		}
		if options.ReadTimeout > 0 {
			options.ConnectionString += fmt.Sprintf("&readTimeout=%ds", options.ReadTimeout)
		}
		if options.WriteTimeout > 0 {
			options.ConnectionString += fmt.Sprintf("&writeTimeout=%ds", options.WriteTimeout)
		}
	}

	return options
}
