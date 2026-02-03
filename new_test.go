package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

type closeStmt struct {
	closed bool
}

func (s *closeStmt) QueryContext(ctx context.Context, args ...any) (Rows, error) {
	return nil, nil
}

func (s *closeStmt) Close() error {
	s.closed = true
	return nil
}

type closeDB struct {
	closed bool
}

func (d *closeDB) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	return nil, nil
}

func (d *closeDB) Close() error {
	d.closed = true
	return nil
}

func TestNew_OpenError(t *testing.T) {
	origOpen := sqlOpen
	sqlOpen = func(driverName, dataSourceName string) (*sql.DB, error) {
		return nil, errors.New("open failed")
	}
	t.Cleanup(func() { sqlOpen = origOpen })

	_, err := New(Options{Username: "u", Password: "p", Database: "db"})
	if err == nil {
		t.Fatalf("expected open error")
	}
}

func TestNew_PingError(t *testing.T) {
	origOpen := sqlOpen
	sqlOpen = func(driverName, dataSourceName string) (*sql.DB, error) {
		return newTestSQLDB(errors.New("ping failed")), nil
	}
	t.Cleanup(func() { sqlOpen = origOpen })

	_, err := New(Options{Username: "u", Password: "p", Database: "db"})
	if err == nil {
		t.Fatalf("expected ping error")
	}
}

func TestNew_Success(t *testing.T) {
	origOpen := sqlOpen
	sqlOpen = func(driverName, dataSourceName string) (*sql.DB, error) {
		return newTestSQLDB(nil), nil
	}
	t.Cleanup(func() { sqlOpen = origOpen })

	client, err := New(Options{
		Username:       "u",
		Password:       "p",
		Database:       "db",
		CacheEnabled:   true,
		CacheSize:      5,
		CacheTTLCheck:  10 * time.Millisecond,
		Codec:          stubCodec{},
		MaxConnections: 2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client.DB == nil || client.inMemory == nil {
		t.Fatalf("expected DB and in-memory cache to be initialized")
	}
	if !client.CacheEnabled {
		t.Fatalf("expected CacheEnabled to be true")
	}
	if client.dbName != "db" {
		t.Fatalf("expected dbName to be set")
	}
	if _, ok := client.codec.(stubCodec); !ok {
		t.Fatalf("expected custom codec to be used")
	}
	client.Close()
}

func TestNew_DefaultCodecAndCache(t *testing.T) {
	origOpen := sqlOpen
	sqlOpen = func(driverName, dataSourceName string) (*sql.DB, error) {
		return newTestSQLDB(nil), nil
	}
	t.Cleanup(func() { sqlOpen = origOpen })

	client, err := New(Options{
		Username: "u",
		Password: "p",
		Database: "db",
		Cache:    stubCache{},
		Mutex:    stubMutex{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := client.codec.(MsgpackCodec); !ok {
		t.Fatalf("expected default MsgpackCodec")
	}
	if _, ok := client.cache.(stubCache); !ok {
		t.Fatalf("expected custom cache to be used")
	}
	if _, ok := client.mutex.(stubMutex); !ok {
		t.Fatalf("expected custom mutex to be used")
	}
	client.Close()
}

func TestMySQL_Close(t *testing.T) {
	stmt := &closeStmt{}
	db := &closeDB{}
	client := &MySQL{
		DB:      db,
		prepare: map[string]Stmt{"q": stmt},
		stop:    make(chan struct{}, 1),
	}

	client.Close()
	client.Close()

	if !stmt.closed {
		t.Fatalf("expected prepared statement to be closed")
	}
	if !db.closed {
		t.Fatalf("expected DB to be closed")
	}
}
