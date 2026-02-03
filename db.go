package mysql

import (
	"context"
	"database/sql"
)

// DB defines the interface for database operations with context support.
// This abstraction allows for different database implementations and facilitates testing.
type DB interface {
	// PrepareContext creates a prepared statement for later queries or executions.
	// The provided context is used for the preparation phase (e.g., timeout, cancellation).
	// Returns a Stmt interface for executing the prepared statement.
	PrepareContext(ctx context.Context, query string) (Stmt, error)

	// Close closes the database and releases any open resources.
	// After Close is called, the database cannot be used for further operations.
	Close() error
}

// Stmt represents a prepared statement.
// Prepared statements can be reused multiple times with different parameters.
type Stmt interface {
	// QueryContext executes a prepared query statement with the given arguments.
	// Returns rows from the query result. The context controls execution timeout/cancellation.
	QueryContext(ctx context.Context, args ...any) (Rows, error)

	// Close closes the statement and releases associated database resources.
	// Statements should be closed when no longer needed to free database resources.
	Close() error
}

// sqlDB is a concrete implementation of the DB interface wrapping *sql.DB.
// This adapter pattern allows using the standard sql.DB while maintaining
// a clean interface for the rest of the application.
type sqlDB struct {
	db *sql.DB // Underlying standard database connection
}

// PrepareContext implements the DB interface by delegating to the underlying *sql.DB.
// Wraps the returned *sql.Stmt in a sqlStmt adapter to implement the Stmt interface.
func (s *sqlDB) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	stmt, err := s.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &sqlStmt{stmt: stmt}, nil
}

// Close implements the DB interface by closing the underlying database connection.
// This closes all open connections and stops new ones from being created.
func (s *sqlDB) Close() error {
	return s.db.Close()
}

// sqlStmt is a concrete implementation of the Stmt interface wrapping *sql.Stmt.
// Provides an abstraction layer over the standard library's prepared statement.
type sqlStmt struct {
	stmt *sql.Stmt // Underlying prepared statement
}

// QueryContext implements the Stmt interface by delegating to the underlying *sql.Stmt.
// Returns standard sql.Rows which already satisfies the Rows interface.
func (s *sqlStmt) QueryContext(ctx context.Context, args ...any) (Rows, error) {
	return s.stmt.QueryContext(ctx, args...)
}

// Close implements the Stmt interface by closing the underlying prepared statement.
// Releases server and client resources associated with the prepared statement.
func (s *sqlStmt) Close() error {
	return s.stmt.Close()
}
