package mysql

import (
	"context"
	"errors"
	"testing"
)

type stubStmt struct{}

func (s *stubStmt) QueryContext(ctx context.Context, args ...any) (Rows, error) { return nil, nil }
func (s *stubStmt) Close() error                                                { return nil }

type stubDB struct {
	prepareCalls int
	stmt         Stmt
	err          error
}

func (d *stubDB) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	d.prepareCalls++
	if d.err != nil {
		return nil, d.err
	}
	return d.stmt, nil
}

func (d *stubDB) Close() error { return nil }

func TestGetPreparedStatement_CacheHit(t *testing.T) {
	stmt := &stubStmt{}
	db := &stubDB{stmt: stmt}
	client := &MySQL{
		DB:      db,
		prepare: map[string]Stmt{"q": stmt},
	}

	got, err := client.getPreparedStatement(context.Background(), "q")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != stmt {
		t.Fatalf("expected cached statement")
	}
	if db.prepareCalls != 0 {
		t.Fatalf("expected PrepareContext not to be called")
	}
}

func TestGetPreparedStatement_CacheMiss(t *testing.T) {
	stmt := &stubStmt{}
	db := &stubDB{stmt: stmt}
	client := &MySQL{
		DB:      db,
		prepare: make(map[string]Stmt),
	}

	got, err := client.getPreparedStatement(context.Background(), "q")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != stmt {
		t.Fatalf("expected prepared statement to be returned")
	}
	if db.prepareCalls != 1 {
		t.Fatalf("expected PrepareContext to be called once")
	}
}

func TestGetPreparedStatement_PrepareError(t *testing.T) {
	db := &stubDB{err: errors.New("prepare failed")}
	client := &MySQL{
		DB:      db,
		prepare: make(map[string]Stmt),
	}

	_, err := client.getPreparedStatement(context.Background(), "q")
	if err == nil {
		t.Fatalf("expected prepare error")
	}
	if db.prepareCalls != 1 {
		t.Fatalf("expected PrepareContext to be called once")
	}
}
