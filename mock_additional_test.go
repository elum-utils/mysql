package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestMockStmt_DelayCompletes(t *testing.T) {
	stmt := &MockStmt{
		Delay: 5 * time.Millisecond,
		Factory: func() Rows {
			return &MockRows{data: [][]any{{1, "ok"}}}
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		t.Fatalf("expected delay to complete, got error: %v", err)
	}
	defer rows.Close()
}

func TestMockStmt_ReturnsError(t *testing.T) {
	stmt := &MockStmt{
		Err: errors.New("boom"),
		Factory: func() Rows {
			return &MockRows{data: [][]any{{1, "ok"}}}
		},
	}

	_, err := stmt.QueryContext(context.Background())
	if err == nil {
		t.Fatalf("expected error from mock stmt")
	}
}

func TestMockStmt_Close(t *testing.T) {
	stmt := &MockStmt{}
	if err := stmt.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}

func TestMockDB_PrepareContext_Closed(t *testing.T) {
	db := NewMockDB()
	db.Closed = true

	_, err := db.PrepareContext(context.Background(), "SELECT 1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestMockDB_PrepareContext_NoStmt(t *testing.T) {
	db := NewMockDB()
	_, err := db.PrepareContext(context.Background(), "SELECT 1")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestMockDB_PrepareContext_ErrorOnlyStmt(t *testing.T) {
	db := NewMockDB()
	stmtErr := errors.New("prepare failed")
	db.WithStmt("SELECT 1", &MockStmt{Err: stmtErr})

	_, err := db.PrepareContext(context.Background(), "SELECT 1")
	if !errors.Is(err, stmtErr) {
		t.Fatalf("expected %v, got %v", stmtErr, err)
	}
}

func TestMockDB_Close(t *testing.T) {
	db := NewMockDB()
	if err := db.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	if !db.Closed {
		t.Fatalf("expected Closed to be true")
	}
}
