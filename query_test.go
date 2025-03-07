package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
)

// MockCache simulates a simple in-memory cache for testing purposes.
type MockCache struct {
	storage map[string][]byte // Stores key-value pairs
}

// NewMockCache creates a new instance of MockCache.
func NewMockCache() *MockCache {
	return &MockCache{storage: make(map[string][]byte)}
}

// Get retrieves a value by key. Returns an error if the key doesn't exist.
func (m *MockCache) Get(key string) ([]byte, error) {
	val, ok := m.storage[key]
	if !ok {
		return nil, errors.New("key not found")
	}
	return val, nil
}

// Set saves a key-value pair in the cache.
func (m *MockCache) Set(key string, val []byte, exp time.Duration) error {
	m.storage[key] = val
	return nil
}

// Delete removes a key-value pair from the cache.
func (m *MockCache) Delete(key string) error {
	delete(m.storage, key)
	return nil
}

// Reset clears all key-value pairs from the cache.
func (m *MockCache) Reset() error {
	m.storage = make(map[string][]byte)
	return nil
}

// Close is a no-op function for MockCache, included for compatibility.
func (m *MockCache) Close() error {
	return nil
}

// MockMutex simulates a mutex for testing purposes.
type MockMutex struct{}

// Lock simulates acquiring a lock for a given key.
func (m *MockMutex) Lock(key string) error {
	return nil
}

// Unlock simulates releasing a lock for a given key.
func (m *MockMutex) Unlock(key string) error {
	return nil
}

// User struct represents a simple user object for query results.
type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// TestQuery contains multiple test cases for the Query function.
func TestQuery(t *testing.T) {
	// Test case: Successful query execution
	t.Run("Successful Query Execution", func(t *testing.T) {
		// Create a mock database connection
		db, mock, mockErr := sqlmock.New()
		assert.NoError(t, mockErr) // Ensure no errors during mock setup
		defer db.Close()           // Ensure database connection is closed after test

		// Initialize a new MySQL object with mock DB and dependencies
		c := &CoreEntity{
			DB:           db,
			prepare:      make(map[string]*sql.Stmt),
			CacheEnabled: true,
			cache:        NewMockCache(),
			mutex:        &MockMutex{},
		}

		// Define the query and mock the database response
		query := "SELECT id, name FROM users WHERE id = ?"
		rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John Doe")
		mock.ExpectPrepare(query).ExpectQuery().WithArgs(1).WillReturnRows(rows)

		// Set up query parameters
		params := Params{
			Query:   query,
			Args:    []interface{}{1},
			Timeout: 5 * time.Second,
		}

		// Execute the query and process the results
		result, _ := Query(c, params, func(rows *sql.Rows) (*User, *MySQLError) {
			var data User
			if rows.Next() {
				_ = rows.Scan(&data.ID, &data.Name) // Safely ignore scanning error
			}
			return &data, nil
		})

		// Verify the result matches the expected output
		assert.Equal(t, User{ID: 1, Name: "John Doe"}, *result)
		assert.NoError(t, mock.ExpectationsWereMet()) // Ensure all mock expectations were met
	})

	// Test case: Query with cache hit
	t.Run("Query with Cache Hit", func(t *testing.T) {
		// Create a mock database connection
		db, _, err := sqlmock.New()
		assert.NoError(t, err)
		defer db.Close()

		// Initialize MySQL object with mock DB and cache
		c := &CoreEntity{
			DB:           db,
			prepare:      make(map[string]*sql.Stmt),
			CacheEnabled: true,
			cache:        NewMockCache(),
			mutex:        &MockMutex{},
		}

		// Set a cached value
		cacheKey := "SELECT id, name FROM users WHERE id = ?"
		cachedData, _ := jsoniter.Marshal(User{ID: 2, Name: "Jane Doe"})
		c.cache.Set(cacheKey, cachedData, 10*time.Second)

		// Set up query parameters
		params := Params{
			Query:      "SELECT id, name FROM users WHERE id = ?",
			Args:       []interface{}{1},
			Timeout:    5 * time.Second,
			CacheDelay: 10 * time.Second,
			Key:        cacheKey,
		}

		// Execute the query and expect a cache hit
		result, _ := Query(c, params, func(rows *sql.Rows) (*User, *MySQLError) {
			// Callback should not be called since data is fetched from cache
			return nil, nil
		})

		// Verify the result matches the cached value
		assert.Equal(t, User{ID: 2, Name: "Jane Doe"}, *result)
	})

	// Test case: Query timeout
	t.Run("Query Timeout", func(t *testing.T) {
		// Create a mock database connection
		db, mock, mockErr := sqlmock.New()
		assert.NoError(t, mockErr)
		defer db.Close()

		// Initialize MySQL object
		c := &CoreEntity{
			DB:           db,
			prepare:      make(map[string]*sql.Stmt),
			CacheEnabled: true,
			cache:        NewMockCache(),
			mutex:        &MockMutex{},
		}

		// Define the query and mock a timeout error
		query := "SELECT id, name FROM users WHERE id = ?"
		mock.ExpectPrepare(query).ExpectQuery().WithArgs(1).WillReturnError(context.DeadlineExceeded)

		// Set up query parameters
		params := Params{
			Query:   query,
			Args:    []interface{}{1},
			Timeout: 10 * time.Millisecond, // Intentionally short timeout
		}

		// Execute the query and expect a timeout error
		result, err := Query(c, params, func(rows *sql.Rows) (*User, *MySQLError) {
			return nil, nil
		})

		assert.Nil(t, result)                      // Result should be nil due to timeout
		assert.NotNil(t, err)                      // Error should be returned
		assert.Equal(t, uint16(45000), err.Number) // Custom timeout error code
		assert.Equal(t, "TIMEOUT", err.Message)    // Custom timeout error message
	})

	// Test case: Deadlock handling
	t.Run("Query Deadlock Handling", func(t *testing.T) {
		// Create a mock database connection
		db, mock, mockErr := sqlmock.New()
		assert.NoError(t, mockErr)
		defer db.Close()

		// Initialize MySQL object
		c := &CoreEntity{
			DB:           db,
			prepare:      make(map[string]*sql.Stmt),
			CacheEnabled: true,
			cache:        NewMockCache(),
			mutex:        &MockMutex{},
		}

		// Define the query and mock a deadlock error
		query := "SELECT id, name FROM users WHERE id = ?"
		mock.ExpectPrepare(query).ExpectQuery().WithArgs(1).WillReturnError(&mysql.MySQLError{
			Number:  1213,
			Message: "Deadlock found",
		})

		// Set up query parameters
		params := Params{
			Query:   query,
			Args:    []interface{}{1},
			Timeout: 5 * time.Second,
		}

		// Execute the query and expect a deadlock error
		result, err := Query(c, params, func(rows *sql.Rows) (*map[string]interface{}, *MySQLError) {
			return nil, nil
		})

		assert.Nil(t, result)                      // Result should be nil due to deadlock
		assert.NotNil(t, err)                      // Error should be returned
		assert.Equal(t, uint16(45000), err.Number) // Custom deadlock error code
		assert.Equal(t, "DEADLOCK", err.Message)   // Custom deadlock error message
	})
}
