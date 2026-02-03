package mysql

import (
	"context"
	"errors"
	"testing"
)

func TestSQLDB_PrepareAndQuery(t *testing.T) {
	db := newTestSQLDB(nil)
	defer db.Close()

	wrapper := &sqlDB{db: db}
	stmt, err := wrapper.PrepareContext(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("PrepareContext failed: %v", err)
	}

	rows, err := stmt.QueryContext(context.Background())
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}
	defer rows.Close()

	var value string
	if !rows.Next() {
		t.Fatalf("expected one row")
	}
	if err := rows.Scan(&value); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if value != "ok" {
		t.Fatalf("unexpected value: %q", value)
	}

	if err := stmt.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestSQLDB_Close(t *testing.T) {
	db := newTestSQLDB(nil)
	wrapper := &sqlDB{db: db}
	if err := wrapper.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestSQLDB_PrepareContextError(t *testing.T) {
	db := newTestSQLDBWithPrepareErr(nil, errors.New("prepare failed"))
	defer db.Close()

	wrapper := &sqlDB{db: db}
	if _, err := wrapper.PrepareContext(context.Background(), "SELECT 1"); err == nil {
		t.Fatalf("expected prepare error")
	}
}
