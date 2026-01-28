package mysql

import (
	"context"
	"database/sql"
	"time"
)

type RowsFactory func() Rows

type MockRows struct {
	data [][]any
	idx  int
}

func (r *MockRows) Next() bool {
	r.idx++
	return r.idx <= len(r.data)
}

func (r *MockRows) Scan(dest ...any) error {
	row := r.data[r.idx-1]
	for i := range dest {
		switch d := dest[i].(type) {
		case *int:
			*d = row[i].(int)
		case *string:
			*d = row[i].(string)
		}
	}
	return nil
}

func (r *MockRows) Close() error { return nil }

type MockStmt struct {
    Factory   RowsFactory
    Err       error
    Delay     time.Duration
}

func (s *MockStmt) QueryContext(ctx context.Context, args ...any) (Rows, error) {
    if s.Delay > 0 {
        select {
        case <-time.After(s.Delay):
        case <-ctx.Done():
            return nil, ctx.Err()
        }
    }
    return s.Factory(), s.Err
}

func (s *MockStmt) Close() error { return nil }

type MockDB struct {
    Stmts    map[string]*MockStmt
    Closed   bool
    Prepares int
}

func NewMockDB() *MockDB {
    return &MockDB{Stmts: make(map[string]*MockStmt)}
}

func (m *MockDB) WithStmt(query string, stmt *MockStmt) {
    m.Stmts[query] = stmt
}

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
        // если есть ошибка и нет фабрики — вернуть сразу
        return nil, stmt.Err
    }
    return stmt, nil
}

func (m *MockDB) Close() error {
    m.Closed = true
    return nil
}
