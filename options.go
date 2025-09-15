package mysql

import (
	"fmt"
	"time"
)

// Storage interface defines methods for a generic key-value storage system.
type Storage interface {
	Get(key string) ([]byte, error)                      // Retrieves the value associated with the given key.
	Set(key string, val []byte, exp time.Duration) error // Stores a key-value pair with an optional expiration duration.
	Delete(key string) error                             // Removes the value associated with the given key.
	Reset() error                                        // Clears all key-value pairs in the storage.
	Close() error                                        // Cleans up resources used by the storage.
}

// Mutex interface defines methods for locking and unlocking a resource by key.
type Mutex interface {
	Lock(key string) error   // Attempts to acquire a lock for the given key.
	Unlock(key string) error // Releases the lock for the given key.
}

// Options struct defines configuration parameters for the database connection.
type Options struct {
	Host             string        // The database host address (e.g., "localhost" or an IP address).
	Username         string        // The username to authenticate with the database.
	Password         string        // The password to authenticate with the database.
	Database         string        // The name of the specific database to connect to.
	Port             int           // The port number on which the database server is listening.
	MaxConnections   int           // The maximum number of open database connections.
	Cache            Storage       // A custom cache implementation (implements the Storage interface).
	CacheEnabled     bool          // A flag indicating whether caching is enabled.
	CacheSize        int           // Maximum cache size in megabytes (MB). Default: 10 MB.
	CacheTTLCheck    time.Duration // Interval for checking and expiring cache entries. Default: 5 minutes.
	Mutex            Mutex         // A custom mutex implementation (implements the Mutex interface).
	Charset          string        // The character set to use for the connection (e.g., "utf8mb4").
	Collation        string        // The collation to use for the connection (e.g., "utf8mb4_unicode_ci").
	Timeout          int           // Connection timeout in seconds (optional).
	ReadTimeout      int           // Read timeout in seconds (optional).
	WriteTimeout     int           // Write timeout in seconds (optional).
	ConnectionString string        // Pre-built connection string (optional, will be generated if empty).
}

// connectionString constructs the MySQL connection string from the provided options.
func defaultOptions(opts ...Options) Options {
	// Устанавливаем значения по умолчанию
	options := Options{
		Host:           "localhost",
		Port:           3306,
		Charset:        "utf8mb4",
		Collation:      "utf8mb4_unicode_ci",
		Timeout:        30,
		ReadTimeout:    30,
		WriteTimeout:   30,
		CacheSize:      10,                    // Default: 10 MB
		CacheTTLCheck:  5 * time.Minute,       // Default: 5 minutes
		CacheEnabled:   false,                 // Default: caching disabled
		MaxConnections: 0,                     // Default: use database driver default
	}

	// Если переданы опции, мержим их
	if len(opts) > 0 {
		userOpts := opts[0]
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
		if userOpts.MaxConnections > 0 {
			options.MaxConnections = userOpts.MaxConnections
		}
		if userOpts.Charset != "" {
			options.Charset = userOpts.Charset
		}
		if userOpts.Collation != "" {
			options.Collation = userOpts.Collation
		}
		if userOpts.Timeout > 0 {
			options.Timeout = userOpts.Timeout
		}
		if userOpts.ReadTimeout > 0 {
			options.ReadTimeout = userOpts.ReadTimeout
		}
		if userOpts.WriteTimeout > 0 {
			options.WriteTimeout = userOpts.WriteTimeout
		}
		if userOpts.CacheSize > 0 {
			options.CacheSize = userOpts.CacheSize
		}
		if userOpts.CacheTTLCheck > 0 {
			options.CacheTTLCheck = userOpts.CacheTTLCheck
		}
		
		// Для булевых и интерфейсных полей просто копируем
		options.Cache = userOpts.Cache
		options.CacheEnabled = userOpts.CacheEnabled
		options.Mutex = userOpts.Mutex
		options.ConnectionString = userOpts.ConnectionString
	}

	// Если ConnectionString не предоставлен, генерируем его
	if options.ConnectionString == "" {
		// Base connection string
		options.ConnectionString = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
			options.Username, options.Password, options.Host, options.Port, options.Database)

		// Add charset if specified
		if options.Charset != "" {
			options.ConnectionString += "&charset=" + options.Charset
		}

		// Add collation if specified
		if options.Collation != "" {
			options.ConnectionString += "&collation=" + options.Collation
		}

		// Add timeouts if specified
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