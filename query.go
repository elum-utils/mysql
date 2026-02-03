package mysql

import (
	"context"
	"errors"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Params holds the inputs used by Query.
type Params struct {
	Key            string        // Cache key (if caching is enabled). If empty, will be auto-generated based on query and arguments.
	Database       string        // Optional database name for qualifying stored procedure calls (e.g., "dbname.proc_name")
	Query          string        // SQL query string. If provided, takes precedence over Exec field for direct SQL execution.
	Exec           string        // Stored procedure name or SQL executable string. Used when Query is empty.
	Args           []any         // Arguments for the SQL query. Bound to placeholders in the query/procedure call.
	Timeout        time.Duration // Timeout for the query execution. Zero value uses default timeout (100 seconds).
	CacheDelay     time.Duration // TTL for external/distributed cache (L2 cache). Zero means no external caching.
	NodeCacheDelay time.Duration // TTL for local in-memory cache (L1 cache). Zero means no local caching.
}

// getPreparedStatement retrieves a prepared SQL statement from the cache or prepares a new one
// Uses a mutex-protected map to cache prepared statements by query text, reducing database server overhead
// for frequently repeated queries. This is especially beneficial for parameterized queries and stored procedures.
func (c *MySQL) getPreparedStatement(ctx context.Context, query string) (Stmt, error) {
	c.mx.Lock()
	defer c.mx.Unlock()

	// Check cache first - cache hit avoids database roundtrip for statement preparation
	if stmt, ok := c.prepare[query]; ok {
		return stmt, nil
	}

	// Cache miss - prepare new statement via database connection
	stmt, err := c.DB.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	// Store in cache for future reuse. Note: statement is not closed here;
	// it remains cached for the lifetime of the MySQL connection or until cache eviction.
	c.prepare[query] = stmt
	return stmt, nil
}

// Query executes a database query with optional multi-level caching support.
// Generic type T represents the expected result type. The callback function processes
// raw database rows and converts them to the desired type.
// Automatically handles caching, prepared statement reuse, timeout, and error conversion.
func Query[T any](
	c *MySQL,
	params Params,
	callback func(rows Rows) (*T, *MySQLError),
) (*T, *MySQLError) {

	// Route to appropriate implementation based on whether external cache is configured
	if c.cache == nil {
		return internalQuery(c, params, callback)
	}

	return externalQuery(c, params, callback)

}

// externalQuery handles queries when external cache (L2) is configured.
// Implements two-level caching strategy: L1 (in-memory) and L2 (external/shared cache).
// Uses distributed locking to prevent cache stampede (multiple concurrent requests
// for the same uncached data overwhelming the database).
func externalQuery[T any](
	c *MySQL,
	params Params,
	callback func(rows Rows) (*T, *MySQLError),
) (*T, *MySQLError) {

	// Generate final SQL query from parameters (handles both direct SQL and stored procedures)
	query := generateQuery(params)

	// Determine cache key only when caching is enabled and used.
	needKey := c.CacheEnabled && (params.NodeCacheDelay > 0 || params.CacheDelay > 0)
	var key string
	if needKey {
		if params.Key == "" {
			key = CreateKey(params, c)
		} else {
			key = params.Key
		}
	}

	// Check L1 cache (in-memory) if node-level caching is enabled and configured
	// This is the fastest cache level but limited to current process memory
	if params.NodeCacheDelay > 0 && c.CacheEnabled {
		if val, err := c.inMemory.Get(key); err == nil {
			if res, ok := val.(*T); ok {
				// L1 cache hit - return immediately without database access
				return res, nil
			}
		}
	}

	// Check L2 cache (external/shared) if external caching is enabled
	// This cache is shared across multiple application instances/nodes
	if params.CacheDelay > 0 && c.CacheEnabled {
		// First optimistic check - proceed if cache miss
		if res := checkExternalCache[T](c, key); res != nil {
			// L2 cache hit - warm up L1 cache for faster subsequent access
			if params.NodeCacheDelay > 0 {
				c.inMemory.Set(key, res, params.NodeCacheDelay)
			}
			return res, nil
		}

		// Cache miss - acquire distributed lock to prevent concurrent database queries
		// for the same cache key (cache stampede protection)
		mutexKey := "mutex_" + key
		if err := c.mutex.Lock(mutexKey); err != nil {
			// Lock acquisition failed - cannot safely proceed with cache population
			// In production, consider logging this and proceeding without cache protection
			return nil, nil
		}
		defer c.mutex.Unlock(mutexKey)

		// Double-check cache after acquiring lock (other goroutine might have populated it)
		if res := checkExternalCache[T](c, key); res != nil {
			// Cache was populated while waiting for lock - warm up L1 and return
			if params.NodeCacheDelay > 0 {
				c.inMemory.Set(key, res, params.NodeCacheDelay)
			}
			return res, nil
		}
	}

	// Create context with timeout for database operations
	// Uses default timeout if params.Timeout is zero
	ctx, cancel := createContextWithTimeout(params.Timeout)
	defer cancel()

	// Get cached or newly prepared statement
	prepare, err := c.getPreparedStatement(ctx, query)
	if err != nil {
		// Convert MySQL driver error to application error type
		if sqlErr, ok := err.(*mysql.MySQLError); ok {
			return nil, &MySQLError{
				Number:   sqlErr.Number,
				SQLState: sqlErr.SQLState,
				Message:  sqlErr.Message,
			}
		}
		// Non-MySQL error (network, context cancelled, etc.)
		return nil, &MySQLError{}
	}

	// Execute query with parameters
	rows, err := prepare.QueryContext(ctx, params.Args...)
	if err != nil {
		// Handle specific MySQL error conditions with application-specific codes
		if sqlErr, ok := err.(*mysql.MySQLError); ok && sqlErr.Number == 1213 {
			// MySQL error 1213: Deadlock found when trying to get lock
			return nil, &MySQLError{Number: 45000, Message: "DEADLOCK"}
		}
		if errors.Is(err, context.DeadlineExceeded) {
			// Query exceeded timeout
			return nil, &MySQLError{Number: 45000, Message: "TIMEOUT"}
		}
		if sqlErr, ok := err.(*mysql.MySQLError); ok {
			// Other MySQL-specific errors
			return nil, &MySQLError{
				Number:   sqlErr.Number,
				SQLState: sqlErr.SQLState,
				Message:  sqlErr.Message,
			}
		}
		// Generic error (network, driver, etc.)
		return nil, &MySQLError{}
	}
	// Ensure rows are closed even if callback panics
	defer rows.Close()

	// Process query results through user-provided callback
	// Callback is responsible for scanning rows and constructing result object
	clbRes, clbErr := callback(rows)

	// Cache successful results for future requests
	if clbErr == nil && clbRes != nil {

		// Store in L2 cache (external/shared) if enabled
		if params.CacheDelay > 0 && c.CacheEnabled {
			// Serialize result using configured codec (e.g., MessagePack, JSON)
			data, err := c.codec.Marshal(clbRes)
			if err != nil {
				// Serialization error - log but don't fail the query
				// The result is still returned to caller, just not cached
				return clbRes, &MySQLError{Number: 45000, Message: "SERIALIZE"}
			}
			// Store in external cache with TTL (best-effort, ignore Set errors)
			_ = c.cache.Set(key, data, params.CacheDelay)

			// Also store in L1 cache for faster local access
			if params.NodeCacheDelay > 0 {
				c.inMemory.Set(key, clbRes, params.NodeCacheDelay)
			}
		}
	}

	// Return result and error from callback
	// Note: caching errors are not returned to caller (caching is best-effort)
	return clbRes, clbErr

}

// internalQuery handles queries when only in-memory (L1) cache is available.
// Simplified version without external cache or distributed locking.
func internalQuery[T any](
	c *MySQL,
	params Params,
	callback func(rows Rows) (*T, *MySQLError),
) (*T, *MySQLError) {

	query := generateQuery(params)

	// Check L1 cache only (no L2 cache available)
	var key string
	if params.CacheDelay > 0 {
		if params.Key == "" {
			key = CreateKey(params, c)
		} else {
			key = params.Key
		}
		if val, err := c.inMemory.Get(key); err == nil {
			if res, ok := val.(*T); ok {
				// Cache hit - return immediately
				return res, nil
			}
		}
	}

	// Create execution context with timeout
	ctx, cancel := createContextWithTimeout(params.Timeout)
	defer cancel()

	// Get prepared statement (cached or new)
	prepare, err := c.getPreparedStatement(ctx, query)
	if err != nil {
		// Error handling identical to externalQuery
		if sqlErr, ok := err.(*mysql.MySQLError); ok {
			return nil, &MySQLError{
				Number:   sqlErr.Number,
				SQLState: sqlErr.SQLState,
				Message:  sqlErr.Message,
			}
		}
		return nil, &MySQLError{}
	}

	// Execute query
	rows, err := prepare.QueryContext(ctx, params.Args...)
	if err != nil {
		// Error handling identical to externalQuery
		if sqlErr, ok := err.(*mysql.MySQLError); ok && sqlErr.Number == 1213 {
			return nil, &MySQLError{Number: 45000, Message: "DEADLOCK"}
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, &MySQLError{Number: 45000, Message: "TIMEOUT"}
		}
		if sqlErr, ok := err.(*mysql.MySQLError); ok {
			return nil, &MySQLError{
				Number:   sqlErr.Number,
				SQLState: sqlErr.SQLState,
				Message:  sqlErr.Message,
			}
		}
		return nil, &MySQLError{}
	}
	defer rows.Close()

	// Process results via callback
	clbRes, clbErr := callback(rows)

	// Cache result in L1 if successful and caching enabled
	if clbErr == nil && clbRes != nil && params.CacheDelay > 0 {
		if key == "" {
			if params.Key == "" {
				key = CreateKey(params, c)
			} else {
				key = params.Key
			}
		}
		c.inMemory.Set(key, clbRes, params.CacheDelay)
	}

	return clbRes, clbErr
}

// createContextWithTimeout creates a context with timeout for query execution.
// If timeout is zero or not specified, uses a conservative default of 100 seconds
// to prevent queries from hanging indefinitely while allowing long-running operations.
func createContextWithTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout == 0 {
		timeout = 100 * time.Second
	}
	return context.WithTimeout(context.Background(), timeout)
}

// checkExternalCache retrieves and deserializes an item from external cache.
// Returns nil on cache miss, deserialization error, or if cache is not configured.
// Performs type-safe deserialization using the configured codec.
func checkExternalCache[T any](c *MySQL, key string) *T {
	// Get raw bytes from external cache
	data, err := c.cache.Get(key)
	if err != nil {
		// Cache miss or cache error
		return nil
	}

	// Deserialize bytes into typed object
	var obj T
	if err := c.codec.Unmarshal(data, &obj); err != nil {
		// Deserialization error - corrupted cache entry or schema mismatch
		return nil
	}
	return &obj
}
