package mysql

import (
	"fmt"
	"sync"
	"time"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

// ErrMySQLNotInitialized is a predefined error indicating the MySQL connection
// has not been properly initialized.
var (
	ErrMySQLNotInitialized = &MySQLError{
		Number:  45000,                      // Custom error number.
		Message: "mysql is not initialized", // Error message indicating uninitialized MySQL.
	}
)

// Options struct defines configuration parameters for the database connection.
type Options struct {
	Host           string  // The database host address (e.g., "localhost" or an IP address).
	Username       string  // The username to authenticate with the database.
	Password       string  // The password to authenticate with the database.
	Database       string  // The name of the specific database to connect to.
	Port           int     // The port number on which the database server is listening.
	MaxConnections int     // The maximum number of open database connections.
	Cache          Storage // A custom cache implementation (implements the Storage interface).
	CacheEnabled   bool    // A flag indicating whether caching is enabled.
	Mutex          Mutex   // A custom mutex implementation (implements the Mutex interface).
}

// CoreEntity struct encapsulates the database connection, cache, and synchronization primitives.
type CoreEntity struct {
	DB           *sql.DB              // The underlying SQL database connection.
	prepare      map[string]*sql.Stmt // A map to store prepared SQL statements.
	cache        Storage              // The storage interface for caching query results.
	mutex        Mutex                // The mutex interface for synchronizing access.
	stop         chan bool            // A channel to signal the shutdown of the database connection.
	mx           sync.RWMutex         // A read-write mutex to synchronize internal access.
	CacheEnabled bool                 // Indicates whether caching is enabled.
}

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

// MySQLError represents a custom MySQL error structure with additional fields for error handling.
type MySQLError struct {
	Number   uint16  // Error code number.
	SQLState [5]byte // SQL state to categorize the error.
	Message  string  // Descriptive message for the error.
}

// Error returns the error message formatted with its number and SQL state.
func (me *MySQLError) Error() string {
	if me.SQLState != [5]byte{} {
		// Format including SQL state if it's set.
		return fmt.Sprintf("Error %d (%s): %s", me.Number, me.SQLState, me.Message)
	}
	// Format without SQL state.
	return fmt.Sprintf("Error %d: %s", me.Number, me.Message)
}

// Is checks if an error matches the MySQLError based on the error number.
func (me *MySQLError) Is(err error) bool {
	if merr, ok := err.(*MySQLError); ok {
		return merr.Number == me.Number
	}
	return false
}

// New initializes and returns a new CoreEntity with the given options.
func New(opt Options) (*CoreEntity, error) {
	// Open a connection to the MySQL database.
	db, err := sql.Open("mysql", connectionString(opt))
	if err != nil {
		return nil, err // Return error if opening the connection fails.
	}

	// Configure connection pool settings.
	db.SetMaxOpenConns(opt.MaxConnections) // Set max open connections.
	db.SetMaxIdleConns(opt.MaxConnections) // Set max idle connections.
	db.SetConnMaxLifetime(time.Minute * 5) // Set connection max lifetime.

	// Verify the database connection.
	err = db.Ping()
	if err != nil {
		return nil, err // Return error if connection verification fails.
	}

	// Initialize a new CoreEntity instance.
	core := &CoreEntity{
		DB:           db,
		prepare:      make(map[string]*sql.Stmt), // Initialize map for prepared statements.
		CacheEnabled: opt.CacheEnabled,           // Enable caching based on option.
	}

	// Assign the provided mutex or use default if none is provided.
	if opt.Mutex != nil {
		core.mutex = opt.Mutex
	}

	// Assign the provided cache or a new in-memory storage if none is provided.
	if opt.Cache != nil {
		core.cache = opt.Cache
	} else {
		core.cache = NewInMemoryStorage()
	}

	return core, nil
}

// Close cleans up resources used by the CoreEntity instance.
func (c *CoreEntity) Close() {
	// Stop any background processes.
	c.stop <- true

	// Close all prepared SQL statements.
	for _, stmt := range c.prepare {
		if stmt != nil {
			stmt.Close()
		}
	}

	// Close the database connection.
	c.DB.Close()
}

// connectionString constructs the MySQL connection string from the provided options.
func connectionString(opts Options) string {
	// Format: username:password@tcp(host:port)/database?parseTime=true
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", opts.Username, opts.Password, opts.Host, opts.Port, opts.Database)
}
