package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	jsoniter "github.com/json-iterator/go"
)

// Params is a structure for storing query parameters used in the Query function
type Params struct {
	Key        string        // Cache key (if caching is enabled)
	Query      string        // SQL query string
	Exec       string        // Stored procedure or SQL executable string
	Args       []any         // Arguments for the SQL query
	Timeout    time.Duration // Timeout for the query execution
	CacheDelay time.Duration // Cache delay time (time to keep data in cache)
}

// getPreparedStatement retrieves a prepared SQL statement from the cache or prepares a new one
func (c *CoreEntity) getPreparedStatement(query string) (*sql.Stmt, error) {
	c.mx.Lock()         // Lock the mutex to safely access the prepared queries map
	defer c.mx.Unlock() // Unlock the mutex once the function is done

	// If the query is already prepared and cached, return it
	if stmt, ok := c.prepare[query]; ok {
		return stmt, nil
	}

	// If the query is not prepared yet, prepare it
	stmt, err := c.DB.Prepare(query)
	if err != nil {
		return nil, err // Return the error if preparing the query fails
	}

	// Store the prepared statement in the cache for future use
	c.prepare[query] = stmt
	return stmt, nil
}

// Query executes a database query with the given parameters and returns the result via a callback function
func Query[T any](
	c *CoreEntity,
	params Params,
	callback func(rows *sql.Rows) (*T, *MySQLError),
) (*T, *MySQLError) {

	query := params.Query
	if params.Query == "" {
		args := strings.TrimRight(strings.Repeat("?, ", len(params.Args)), ", ")
		query = fmt.Sprintf("CALL %v(%v)", params.Exec, args)
	}

	key := params.Key
	// If no key is provided, generate one from the query and arguments
	if key == "" {
		key = CreateKey(query, params.Args...)
	}

	// If caching is enabled and a cache delay is specified, try fetching data from the cache first
	if c.CacheEnabled && params.CacheDelay > 0 {

		mutexKey := fmt.Sprintf("mutex_%v", key)

		// Try to fetch data from the cache
		data := check[T](c, key)
		if data != nil {
			res, ok := data.(*T)
			if !ok {
				return nil, nil
			}
			return res, nil // If data is found in cache, return it immediately
		}

		// If data is not found in cache, lock access for other queries with the same key
		if err := c.mutex.Lock(mutexKey); err != nil {
			return nil, nil // If locking fails, return nil (no data)
		}
		defer c.mutex.Unlock(mutexKey) // Unlock the mutex after the execution

		// Recheck the cache after locking
		data = check[T](c, key)
		if data != nil {
			res, ok := data.(*T)
			if !ok {
				return nil, nil
			}
			return res, nil // If data is found in cache, return it immediately
		}

	}

	// Create a context with a timeout for the query execution
	ctx, cancel := createContextWithTimeout(params.Timeout)
	defer cancel() // Cancel the context after the query execution

	// Retrieve the prepared statement
	prepare, err := c.getPreparedStatement(query)
	if err != nil {
		sqlErr, ok := err.(*mysql.MySQLError)
		if ok {
			return nil, &MySQLError{
				Number:   sqlErr.Number,
				SQLState: sqlErr.SQLState,
				Message:  sqlErr.Message,
			} // Return MySQL-specific error if encountered
		}
		return nil, &MySQLError{} // If it's not a MySQL error, return a generic empty MySQL error
	}

	// Execute the query with the provided arguments
	rows, err := prepare.QueryContext(ctx, params.Args...)
	if err != nil {
		sqlErr, ok := err.(*mysql.MySQLError)
		// Check for specific error code 1213 (deadlock) and return a custom error
		if ok && sqlErr.Number == 1213 {
			return nil, &MySQLError{
				Number:  45000,
				Message: "DEADLOCK", // Custom error for deadlock
			}
		}

		// Check if the error is a timeout
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, &MySQLError{
				Number:  45000,
				Message: "TIMEOUT", // Custom error for query timeout
			}
		}

		// Return the SQL error if it is any other error
		return nil, &MySQLError{
			Number:   sqlErr.Number,
			SQLState: sqlErr.SQLState,
			Message:  sqlErr.Message,
		}
	}
	defer rows.Close() // Close the rows after finishing the query

	// Call the callback function to process the rows and extract the result
	clbRes, clbErr := callback(rows)

	// Serialize the result of the callback function
	res, err := jsoniter.Marshal(clbRes)
	if err != nil {
		// If serialization fails, return a custom error indicating serialization failure
		return clbRes, &MySQLError{
			Number:  45000,
			Message: "SERIALIZE", // Custom error for serialization issues
		}
	}

	// If caching is enabled and no errors occurred, store the result in the cache
	if c.CacheEnabled &&
		params.CacheDelay > 0 &&
		clbErr == nil &&
		clbRes != nil {
		_ = c.cache.Set(key, res, params.CacheDelay) // Cache the result with the given delay
	}

	// Return the result and any potential MySQL error from the callback
	return clbRes, clbErr
}

// createContextWithTimeout creates a context with a timeout duration for the query
func createContextWithTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	// Set a default timeout of 100 seconds if the timeout is zero
	if timeout == 0 {
		timeout = 100 * time.Second
	}
	return context.WithTimeout(context.Background(), timeout)
}

// check attempts to retrieve data from the cache for the given key and returns it
func check[T any](c *CoreEntity, key string) any {
	// Attempt to get the data from the cache
	data, err := c.cache.Get(key)

	// Return nil if an error occurred while fetching data from the cache
	if err != nil {
		return nil
	}

	// Deserialize the cached data into the result variable
	var res T
	err = jsoniter.Unmarshal(data, &res)

	// Return nil if deserialization failed
	if err != nil {
		return nil
	}

	// Return the deserialized result
	return &res
}
