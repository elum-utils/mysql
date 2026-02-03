package mysql

import (
	"context"
	"errors"
	"testing"
	"time"
)

// newMockDBWithRows creates a MockDB configured with a prepared statement
// that returns the provided mock data rows.
// This helper simplifies test setup by handling the common pattern of
// creating a mock database with predefined query results.
func newMockDBWithRows(data [][]any) *MockDB {
	rowsFactory := func() Rows {
		return &MockRows{data: data}
	}
	stmt := &MockStmt{Factory: rowsFactory}
	db := NewMockDB()
	db.WithStmt("SELECT * FROM table", stmt)
	return db
}

// failPrepareDB creates a MockDB without any prepared statements,
// causing PrepareContext to return an error when attempting to prepare queries.
// Useful for testing error handling when statement preparation fails.
func failPrepareDB() *MockDB {
	return &MockDB{
		Stmts: make(map[string]*MockStmt),
	}
}

// TestQuery_Success verifies the basic happy path where a query executes successfully
// and returns expected data. This test ensures the Query function properly
// integrates with the mock database, prepares statements, executes queries,
// and processes results through the callback.
func TestQuery_Success(t *testing.T) {
	// Create mock database with sample user data
	mockDB := newMockDBWithRows([][]any{
		{1, "Alice"},
		{2, "Bob"},
	})

	// Create MySQL instance with mock DB and empty caches
	mysql := &MySQL{
		DB:       mockDB,
		prepare:  make(map[string]Stmt),
		cache:    nil,
		inMemory: nil,
	}

	type User struct {
		ID   int
		Name string
	}

	// Execute query and process results
	res, err := Query(mysql, Params{
		Query: "SELECT * FROM table",
	}, func(rows Rows) (*[]User, *MySQLError) {
		var users []User
		for rows.Next() {
			var u User
			_ = rows.Scan(&u.ID, &u.Name)
			users = append(users, u)
		}
		return &users, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %+v", err)
	}
	if len(*res) != 2 {
		t.Fatalf("expected 2 users, got %d", len(*res))
	}
}

// TestQuery_NoRows verifies that the Query function correctly handles
// empty result sets. The callback should be called and return an empty slice,
// not an error.
func TestQuery_NoRows(t *testing.T) {
	mockDB := newMockDBWithRows([][]any{})
	mysql := &MySQL{
		DB:      mockDB,
		prepare: make(map[string]Stmt),
	}

	type User struct {
		ID   int
		Name string
	}

	res, err := Query(mysql, Params{
		Query: "SELECT * FROM table",
	}, func(rows Rows) (*[]User, *MySQLError) {
		var users []User
		for rows.Next() {
			var u User
			_ = rows.Scan(&u.ID, &u.Name)
			users = append(users, u)
		}
		return &users, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %+v", err)
	}
	if len(*res) != 0 {
		t.Fatalf("expected 0 users, got %d", len(*res))
	}
}

// TestQuery_PrepareError tests error handling when statement preparation fails.
// This simulates scenarios like syntax errors in the query or database
// connection issues during preparation.
func TestQuery_PrepareError(t *testing.T) {
	mockDB := failPrepareDB()
	mysql := &MySQL{
		DB:      mockDB,
		prepare: make(map[string]Stmt),
	}

	_, err := Query(mysql, Params{Query: "SELECT * FROM missing_table"}, func(rows Rows) (*[]any, *MySQLError) {
		return nil, nil
	})

	if err == nil {
		t.Fatal("expected prepare error, got nil")
	}
}

// TestQuery_QueryError tests error handling when query execution fails
// after successful preparation. This simulates runtime errors like
// constraint violations, missing tables, or permission issues.
func TestQuery_QueryError(t *testing.T) {
	stmt := &MockStmt{Err: errors.New("query failed")}

	mockDB := NewMockDB()
	mockDB.WithStmt("SELECT * FROM error_table", stmt)

	mysql := &MySQL{
		DB:      mockDB,
		prepare: make(map[string]Stmt),
	}

	_, err := Query(mysql, Params{Query: "SELECT * FROM error_table"}, func(rows Rows) (*[]any, *MySQLError) {
		return nil, nil
	})

	if err == nil {
		t.Fatal("expected query error, got nil")
	}

	t.Logf("Query returned expected error: %+v", err)
}

// TestQuery_Timeout verifies that the Query function respects timeout settings
// and properly returns a timeout error when execution exceeds the specified duration.
// This tests both the context cancellation and error conversion logic.
func TestQuery_Timeout(t *testing.T) {
	rowsFactory := func() Rows {
		return &MockRows{data: [][]any{{1, "Alice"}}}
	}

	// Simulate a slow query via Delay parameter
	stmt := &MockStmt{
		Factory: rowsFactory,
		Delay:   200 * time.Millisecond, // Exceeds the timeout below
	}

	mockDB := NewMockDB()
	mockDB.WithStmt("SELECT * FROM table", stmt)

	mysql := &MySQL{
		DB:       mockDB,
		prepare:  make(map[string]Stmt),
		cache:    nil,
		inMemory: nil,
	}

	type User struct {
		ID   int
		Name string
	}

	start := time.Now()
	_, err := Query(mysql, Params{
		Query:   "SELECT * FROM table",
		Timeout: 50 * time.Millisecond, // Shorter than the mock delay
	}, func(rows Rows) (*[]User, *MySQLError) {
		var users []User
		for rows.Next() {
			var u User
			_ = rows.Scan(&u.ID, &u.Name)
			users = append(users, u)
		}
		return &users, nil
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	// Check for either context error or converted MySQLError
	if !errors.Is(err, context.DeadlineExceeded) && err.Number != 45000 {
		t.Fatalf("expected timeout MySQLError, got %+v", err)
	}

	t.Logf("Timeout triggered as expected after %v", elapsed)
}

// TestQuery_CacheHit verifies the caching functionality by testing that
// subsequent identical queries return cached results instead of hitting
// the database. This tests the in-memory cache layer (L1 cache).
func TestQuery_CacheHit(t *testing.T) {
	rowsFactory := func() Rows {
		return &MockRows{
			data: [][]any{{1, "Alice"}},
		}
	}

	stmt := &MockStmt{Factory: rowsFactory}
	mockDB := NewMockDB()
	mockDB.WithStmt("SELECT * FROM table", stmt)

	inMemory := NewInMemoryStorage(100, time.Second)
	defer inMemory.Stop()

	mysql := &MySQL{
		DB:           mockDB,
		prepare:      make(map[string]Stmt),
		inMemory:     inMemory,
		cache:        nil,
		CacheEnabled: true,
	}

	type User struct {
		ID   int
		Name string
	}

	// First call: warms the cache by executing the query and storing results
	res1, err := Query(mysql, Params{
		Query:      "SELECT * FROM table",
		CacheDelay: 2 * time.Second,
	}, func(rows Rows) (*[]User, *MySQLError) {
		var users []User
		for rows.Next() {
			var u User
			_ = rows.Scan(&u.ID, &u.Name)
			users = append(users, u)
		}
		return &users, nil
	})
	if err != nil || len(*res1) != 1 {
		t.Fatal("first query failed")
	}

	// Second call: should hit the cache - callback should not be executed
	res2, err := Query(mysql, Params{
		Query:      "SELECT * FROM table",
		CacheDelay: 2 * time.Second,
	}, func(rows Rows) (*[]User, *MySQLError) {
		t.Fatal("callback should not be called on cache hit")
		return nil, nil
	})
	if err != nil || len(*res2) != 1 {
		t.Fatal("cache hit failed")
	}
}

type User struct {
	ID   int
	Name string
}

type unit struct{}

var unitValue = unit{}

// BenchmarkQuery_Success measures the performance of successful query execution
// with multiple result rows. This benchmark helps identify performance
// bottlenecks in the query execution path without caching.
func BenchmarkQuery_Success(b *testing.B) {
	rowsFactory := func() Rows {
		return &MockRows{
			data: [][]any{
				{1, "Alice"},
				{2, "Bob"},
				{3, "Charlie"},
			},
		}
	}

	stmt := &MockStmt{
		Factory: rowsFactory,
	}
	mockDB := NewMockDB()
	mockDB.WithStmt("SELECT * FROM users", stmt)

	mysql := &MySQL{
		DB:      mockDB,
		prepare: make(map[string]Stmt),
		cache:   nil,
	}

	params := Params{
		Query: "SELECT * FROM users",
	}

	handler := func(rows Rows) (*[]User, *MySQLError) {
		var users []User
		for rows.Next() {
			var u User
			_ = rows.Scan(&u.ID, &u.Name)
			users = append(users, u)
		}
		return &users, nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Query(mysql, params, handler)
	}
}

// BenchmarkQuery_Minimal measures library overhead with a minimal callback.
// The callback only iterates rows and returns a stable pointer to avoid
// per-iteration allocations in the callback itself.
func BenchmarkQuery_Minimal(b *testing.B) {
	rowsFactory := func() Rows {
		return &MockRows{
			data: [][]any{
				{1, "Alice"},
				{2, "Bob"},
				{3, "Charlie"},
			},
		}
	}

	stmt := &MockStmt{
		Factory: rowsFactory,
	}
	mockDB := NewMockDB()
	mockDB.WithStmt("SELECT * FROM users", stmt)

	mysql := &MySQL{
		DB:      mockDB,
		prepare: make(map[string]Stmt),
		cache:   nil,
	}

	params := Params{
		Query: "SELECT * FROM users",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Query(mysql, params, func(rows Rows) (*unit, *MySQLError) {
			for rows.Next() {
				// Intentionally avoid Scan to keep callback minimal.
			}
			return &unitValue, nil
		})
	}
}

// BenchmarkQuery_Empty measures the performance of queries that return
// empty result sets. This tests the overhead of query execution without
// the cost of processing result rows.
func BenchmarkQuery_Empty(b *testing.B) {
	rowsFactory := func() Rows {
		return &MockRows{data: [][]any{}}
	}

	stmt := &MockStmt{
		Factory: rowsFactory,
	}
	mockDB := NewMockDB()
	mockDB.WithStmt("SELECT * FROM users WHERE 1=0", stmt)

	mysql := &MySQL{
		DB:      mockDB,
		prepare: make(map[string]Stmt),
		cache:   nil,
	}

	params := Params{
		Query: "SELECT * FROM users WHERE 1=0",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Query(mysql, params, func(rows Rows) (*[]User, *MySQLError) {
			var users []User
			for rows.Next() {
				var u User
				_ = rows.Scan(&u.ID, &u.Name)
				users = append(users, u)
			}
			return &users, nil
		})
	}
}

// BenchmarkQuery_WithDelay measures the performance impact of slow queries
// by simulating network latency or slow database responses.
// This benchmark helps understand the cost of context handling and
// timeout management in the query execution path.
func BenchmarkQuery_WithDelay(b *testing.B) {
	rowsFactory := func() Rows {
		return &MockRows{
			data: [][]any{
				{1, "Alice"},
			},
		}
	}

	stmt := &MockStmt{
		Factory: rowsFactory,
		Delay:   10 * time.Millisecond, // Simulate network/database latency
	}
	mockDB := NewMockDB()
	mockDB.WithStmt("SELECT * FROM users_delay", stmt)

	mysql := &MySQL{
		DB:      mockDB,
		prepare: make(map[string]Stmt),
		cache:   nil,
	}

	params := Params{
		Query: "SELECT * FROM users_delay",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Query(mysql, params, func(rows Rows) (*[]User, *MySQLError) {
			var users []User
			for rows.Next() {
				var u User
				_ = rows.Scan(&u.ID, &u.Name)
				users = append(users, u)
			}
			return &users, nil
		})
	}
}
