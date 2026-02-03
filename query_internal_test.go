package mysql

import (
	"errors"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
)

func newInternalClient(db DB) (*MySQL, func()) {
	inMemory := NewInMemoryStorage(10, time.Second)
	client := &MySQL{
		DB:       db,
		dbName:   "db",
		prepare:  make(map[string]Stmt),
		inMemory: inMemory,
	}
	return client, func() { inMemory.Stop() }
}

func TestQuery_InternalManualKeyCacheHit(t *testing.T) {
	client, cleanup := newInternalClient(&countingDB{})
	defer cleanup()

	key := "manual-key"
	expected := []int{1, 2}
	_ = client.inMemory.Set(key, &expected, time.Minute)

	params := Params{
		Key:        key,
		CacheDelay: time.Minute,
	}

	res, err := Query(client, params, func(rows Rows) (*[]int, *MySQLError) {
		t.Fatal("callback should not be invoked on cache hit")
		return nil, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(*res) != 2 {
		t.Fatalf("unexpected cached result: %+v", res)
	}
}

func TestQuery_InternalPrepareMySQLError(t *testing.T) {
	mysqlErr := &driver.MySQLError{Number: 1064, Message: "syntax"}
	client, cleanup := newInternalClient(&errDB{err: mysqlErr})
	defer cleanup()

	_, err := Query(client, Params{Query: "SELECT 1"}, func(rows Rows) (*[]int, *MySQLError) {
		t.Fatal("callback should not be invoked on prepare error")
		return nil, nil
	})
	if err == nil || err.Number != 1064 {
		t.Fatalf("expected MySQLError to be propagated, got %+v", err)
	}
}

func TestQuery_InternalPrepareGenericError(t *testing.T) {
	client, cleanup := newInternalClient(&errDB{err: errors.New("prepare failed")})
	defer cleanup()

	_, err := Query(client, Params{Query: "SELECT 1"}, func(rows Rows) (*[]int, *MySQLError) {
		t.Fatal("callback should not be invoked on prepare error")
		return nil, nil
	})
	if err == nil || err.Number != 0 {
		t.Fatalf("expected generic prepare error, got %+v", err)
	}
}

func TestQuery_InternalQueryDeadlock(t *testing.T) {
	stmt := &MockStmt{
		Err: &driver.MySQLError{Number: 1213},
		Factory: func() Rows {
			return &MockRows{data: [][]any{{1}}}
		},
	}
	db := NewMockDB()
	db.WithStmt("SELECT * FROM table", stmt)

	client, cleanup := newInternalClient(db)
	defer cleanup()

	_, err := Query(client, Params{Query: "SELECT * FROM table"}, func(rows Rows) (*[]int, *MySQLError) {
		t.Fatal("callback should not be invoked on query error")
		return nil, nil
	})
	if err == nil || err.Message != "DEADLOCK" {
		t.Fatalf("expected deadlock error, got %+v", err)
	}
}

func TestQuery_InternalQueryMySQLError(t *testing.T) {
	stmt := &MockStmt{Err: &driver.MySQLError{Number: 1064, Message: "syntax"}}
	db := NewMockDB()
	db.WithStmt("SELECT * FROM table", stmt)

	client, cleanup := newInternalClient(db)
	defer cleanup()

	_, err := Query(client, Params{Query: "SELECT * FROM table"}, func(rows Rows) (*[]int, *MySQLError) {
		t.Fatal("callback should not be invoked on query error")
		return nil, nil
	})
	if err == nil || err.Number != 1064 {
		t.Fatalf("expected MySQLError to be propagated, got %+v", err)
	}
}

func TestQuery_InternalQueryGenericError(t *testing.T) {
	stmt := &MockStmt{Err: errors.New("boom")}
	db := NewMockDB()
	db.WithStmt("SELECT * FROM table", stmt)

	client, cleanup := newInternalClient(db)
	defer cleanup()

	_, err := Query(client, Params{Query: "SELECT * FROM table"}, func(rows Rows) (*[]int, *MySQLError) {
		t.Fatal("callback should not be invoked on query error")
		return nil, nil
	})
	if err == nil || err.Number != 0 {
		t.Fatalf("expected generic error, got %+v", err)
	}
}
