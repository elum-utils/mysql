package mysql

import (
	"database/sql"
	"sync"
	"time"
)

// MySQL manages a DB connection along with caches, codecs, and prepared statements.
// It is safe for concurrent use.
type MySQL struct {
	DB           DB               // Underlying SQL database connection.
	dbName       string           // Default database name.
	prepare      map[string]Stmt  // Cached prepared statements.
	stop         chan struct{}    // Shutdown signal channel.
	mx           sync.RWMutex     // Guards internal state.
	cache        Storage          // External cache for L2 results.
	inMemory     *InMemoryStorage // In-memory cache for L1 results.
	mutex        Mutex            // Keyed mutex for cache stampede protection.
	codec        Codec            // Codec used for cache serialization.
	CacheEnabled bool             // Whether caching is enabled.
}

// sqlOpen is a test seam that defaults to sql.Open.
var sqlOpen = sql.Open

// New creates a MySQL client using the provided options.
// It validates connectivity via Ping and configures the connection pool.
func New(opts ...Options) (*MySQL, error) {

	opt := defaultOptions(opts...)

	// Open a connection to the MySQL database.
	db, err := sqlOpen("mysql", opt.ConnectionString)
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

	// Initialize MySQL client state.
	core := &MySQL{
		DB:           &sqlDB{db: db},
		dbName:       opt.Database,
		inMemory:     NewInMemoryStorage(opt.CacheSize, opt.CacheTTLCheck),
		prepare:      make(map[string]Stmt), // Initialize map for prepared statements.
		CacheEnabled: opt.CacheEnabled,      // Enable caching based on option.
		stop:         make(chan struct{}, 1),
	}

	if opt.Codec != nil {
		core.codec = opt.Codec
	} else {
		// Default to MessagePack when no codec is provided.
		core.codec = MsgpackCodec{}
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
	}

	return core, nil

}

// Close releases prepared statements and closes the underlying database.
// It is safe to call multiple times.
func (c *MySQL) Close() {
	select {
	case <-c.stop:
	default:
		select {
		case c.stop <- struct{}{}:
		default:
		}
	}

	for _, stmt := range c.prepare {
		if stmt != nil {
			_ = stmt.Close()
		}
	}
	if c.DB != nil {
		_ = c.DB.Close()
	}
}
