package mysql

import (
	"context"
	"database/sql"
)

type DB interface {
	PrepareContext(ctx context.Context, query string) (Stmt, error)
	Close() error
}

type Stmt interface {
	QueryContext(ctx context.Context, args ...any) (Rows, error)
	Close() error
}

type sqlDB struct {
	db *sql.DB
}

func (s *sqlDB) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	stmt, err := s.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &sqlStmt{stmt: stmt}, nil
}

func (s *sqlDB) Close() error {
	return s.db.Close()
}

type sqlStmt struct {
	stmt *sql.Stmt
}

func (s *sqlStmt) QueryContext(ctx context.Context, args ...any) (Rows, error) {
	return s.stmt.QueryContext(ctx, args...)
}

func (s *sqlStmt) Close() error {
	return s.stmt.Close()
}
