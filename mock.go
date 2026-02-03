package mysql

import (
	"context"
	"database/sql"
	"time"
)

// Rows is the interface representing the result of a database query.
// It provides iteration over result rows and scanning of row data into variables.
// This interface abstracts the standard sql.Rows for testing and alternative implementations.
type Rows interface {
	// Next prepares the next result row for reading with Scan.
	// Returns true on success, false if there are no more rows.
	Next() bool

	// Scan copies the values from the current row into the provided destinations.
	// The number of destinations must match the number of columns in the result.
	Scan(dest ...any) error

	// Close closes the Rows iterator, preventing further enumeration.
	// It should be called after iteration is complete to free resources.
	Close() error
}

// RowsFactory is a function type that creates new Rows instances.
// Used by mocks to generate Rows with specific test data for each query execution.
type RowsFactory func() Rows

// MockRows implements the Rows interface with in-memory data for testing.
// It allows simulating database query results without an actual database connection.
type MockRows struct {
	data [][]any // Two-dimensional slice containing mock data rows and columns
	idx  int     // Current row index (0 before first row, 1 after first Next(), etc.)
}

// Next advances to the next row of mock data.
// Returns true if a row is available, false if all rows have been consumed.
// The first call to Next() makes the first row available for scanning.
func (r *MockRows) Next() bool {
	r.idx++
	return r.idx <= len(r.data)
}

// Scan copies values from the current mock row into the provided destinations.
// Currently supports scanning into *int and *string pointers only.
// The number of destinations must match the number of columns in the current row.
func (r *MockRows) Scan(dest ...any) error {
	row := r.data[r.idx-1] // Get current row data (idx is 1-indexed after Next())
	for i := range dest {
		switch d := dest[i].(type) {
		case *int:
			*d = row[i].(int) // Type assertion for integer columns
		case *string:
			*d = row[i].(string) // Type assertion for string columns
			// Additional type cases should be added as needed for other column types
		}
	}
	return nil
}

// Close implements the Rows interface for MockRows.
// Since MockRows uses only in-memory data, no cleanup is required.
func (r *MockRows) Close() error { return nil }

// MockStmt implements a mock prepared statement for testing database interactions.
// It can simulate delays, errors, and produce configurable result sets.
type MockStmt struct {
	Factory RowsFactory   // Function to generate Rows with test data for each query
	Err     error         // Error to return from QueryContext (nil for successful execution)
	Delay   time.Duration // Artificial delay to simulate slow database responses
}

// QueryContext executes the mock prepared statement with optional delay and context support.
// If Delay is set, it simulates a slow database response before returning results.
// If context is cancelled during delay, returns context error immediately.
func (s *MockStmt) QueryContext(ctx context.Context, args ...any) (Rows, error) {
	if s.Delay > 0 {
		select {
		case <-time.After(s.Delay):
			// Simulated delay completed
		case <-ctx.Done():
			// Context cancelled during delay
			return nil, ctx.Err()
		}
	}

	// Return either error or rows from factory function
	if s.Err != nil {
		return nil, s.Err
	}
	return s.Factory(), nil
}

// Close implements the Stmt interface for MockStmt.
// No cleanup needed for mock statement.
func (s *MockStmt) Close() error { return nil }

// MockDB implements a mock database for testing database-dependent code.
// It maps SQL queries to predefined MockStmt responses, allowing comprehensive
// testing without a real database connection.
type MockDB struct {
	Stmts    map[string]*MockStmt // Query-to-statement mapping for different SQL queries
	Closed   bool                 // Whether the mock database has been closed
	Prepares int                  // Counter for PrepareContext calls (useful for assertions)
}

// NewMockDB creates and initializes a new MockDB instance.
// The returned MockDB is ready to have statements registered via WithStmt.
func NewMockDB() *MockDB {
	return &MockDB{Stmts: make(map[string]*MockStmt)}
}

// WithStmt registers a MockStmt for a specific SQL query string.
// This allows different queries to return different mock responses in tests.
func (m *MockDB) WithStmt(query string, stmt *MockStmt) {
	m.Stmts[query] = stmt
}

// PrepareContext simulates preparing a SQL statement in the mock database.
// If the database is closed, returns context.Canceled error.
// If no mock statement is registered for the query, returns sql.ErrNoRows.
// If a registered statement has an error and no factory, returns the error immediately.
func (m *MockDB) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	if m.Closed {
		return nil, context.Canceled
	}
	m.Prepares++

	stmt, ok := m.Stmts[query]
	if !ok {
		return nil, sql.ErrNoRows
	}

	if stmt.Err != nil && stmt.Factory == nil {
		// Special case: error-only statement (no result rows expected)
		return nil, stmt.Err
	}

	return stmt, nil
}

// Close marks the mock database as closed, preventing further operations.
// Subsequent PrepareContext calls will return context.Canceled.
func (m *MockDB) Close() error {
	m.Closed = true
	return nil
}
