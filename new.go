package mysql

import (
	"database/sql"
	"sync"
	"time"
)

// Core struct encapsulates the database connection, cache, and synchronization primitives.
type MySQL struct {
	DB           *sql.DB              // The underlying SQL database connection.
	prepare      map[string]*sql.Stmt // A map to store prepared SQL statements.
	stop         chan bool            // A channel to signal the shutdown of the database connection.
	mx           sync.RWMutex         // A read-write mutex to synchronize internal access.
	cache        Storage              // The storage interface for caching query results.
	mutex        Mutex                // The mutex interface for synchronizing access.
	codec        Codec
	CacheEnabled bool // Indicates whether caching is enabled.
}

func New(opts ...Options) (*MySQL, error) {

	opt := defaultOptions(opts...)

	// Open a connection to the MySQL database.
	db, err := sql.Open("mysql", opt.ConnectionString)
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
	core := &MySQL{
		DB:           db,
		prepare:      make(map[string]*sql.Stmt), // Initialize map for prepared statements.
		CacheEnabled: opt.CacheEnabled,           // Enable caching based on option.
	}

	if opt.Codec != nil {
		core.codec = opt.Codec
	} else {
		core.codec = MsgpackCodec{} // по умолчанию msgpack
	}

	// Assign the provided mutex or use default if none is provided.
	if opt.Mutex != nil {
		core.mutex = opt.Mutex
	} else {
		core.mutex = NewMutex()
	}

	// Assign the provided cache or a new in-memory storage if none is provided.
	if opt.Cache != nil {
		core.cache = opt.Cache
	} else {
		core.cache = NewInMemoryStorage(opt.CacheSize, opt.CacheTTLCheck)
	}

	return core, nil

}

// Close cleans up resources used by the CoreEntity instance.
func (c *MySQL) Close() {
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
