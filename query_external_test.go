package mysql

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
)

type fakeCache struct {
	mu       sync.Mutex
	items    map[string][]byte
	getErr   error
	setErr   error
	setCalls int
}

func newFakeCache() *fakeCache {
	return &fakeCache{items: make(map[string][]byte)}
}

func (c *fakeCache) Get(key string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.getErr != nil {
		return nil, c.getErr
	}
	val, ok := c.items[key]
	if !ok {
		return nil, ErrNotFound
	}
	return val, nil
}

func (c *fakeCache) Set(key string, val []byte, exp time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setCalls++
	if c.setErr != nil {
		return c.setErr
	}
	c.items[key] = val
	return nil
}

func (c *fakeCache) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
	return nil
}

func (c *fakeCache) Reset() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string][]byte)
	return nil
}

func (c *fakeCache) Close() error { return nil }

type fakeMutex struct {
	lockErr error
}

func (m *fakeMutex) Lock(key string) error   { return m.lockErr }
func (m *fakeMutex) Unlock(key string) error { return nil }

type countingDB struct {
	prepares int
}

func (d *countingDB) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	d.prepares++
	return nil, errors.New("unexpected prepare call")
}

func (d *countingDB) Close() error { return nil }

type failingCodec struct{}

func (f failingCodec) Marshal(v any) ([]byte, error)      { return nil, errors.New("marshal failed") }
func (f failingCodec) Unmarshal(data []byte, v any) error { return nil }

type errDB struct {
	err error
}

func (d *errDB) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	return nil, d.err
}

func (d *errDB) Close() error { return nil }

type flipCache struct {
	mu    sync.Mutex
	data  []byte
	calls int
}

func (c *flipCache) Get(key string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.calls == 1 {
		return nil, ErrNotFound
	}
	return c.data, nil
}

func (c *flipCache) Set(key string, val []byte, exp time.Duration) error { return nil }
func (c *flipCache) Delete(key string) error                             { return nil }
func (c *flipCache) Reset() error                                        { return nil }
func (c *flipCache) Close() error                                        { return nil }

func newExternalClient(db DB, cache Storage) (*MySQL, func()) {
	inMemory := NewInMemoryStorage(10, time.Second)
	client := &MySQL{
		DB:           db,
		dbName:       "db",
		prepare:      make(map[string]Stmt),
		cache:        cache,
		inMemory:     inMemory,
		mutex:        NewMutex(),
		codec:        MsgpackCodec{},
		CacheEnabled: true,
	}
	return client, func() { inMemory.Stop() }
}

func TestQuery_ExternalCacheHit(t *testing.T) {
	type user struct {
		ID   int
		Name string
	}

	cache := newFakeCache()
	inMemory := NewInMemoryStorage(10, time.Second)
	defer inMemory.Stop()

	db := &countingDB{}
	client := &MySQL{
		DB:           db,
		dbName:       "db",
		prepare:      make(map[string]Stmt),
		cache:        cache,
		inMemory:     inMemory,
		mutex:        NewMutex(),
		codec:        MsgpackCodec{},
		CacheEnabled: true,
	}

	params := Params{
		Query:          "SELECT * FROM table",
		CacheDelay:     time.Minute,
		NodeCacheDelay: time.Minute,
	}

	expected := []user{{ID: 1, Name: "Alice"}}
	data, marshalErr := client.codec.Marshal(expected)
	if marshalErr != nil {
		t.Fatalf("Marshal failed: %v", marshalErr)
	}

	key := CreateKey(params, client)
	if err := cache.Set(key, data, params.CacheDelay); err != nil {
		t.Fatalf("cache set failed: %v", err)
	}

	res, err := Query(client, params, func(rows Rows) (*[]user, *MySQLError) {
		t.Fatal("callback should not be invoked on cache hit")
		return nil, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(*res) != 1 || (*res)[0].ID != 1 {
		t.Fatalf("unexpected cached result: %+v", res)
	}
	if db.prepares != 0 {
		t.Fatalf("expected DB not to be used on cache hit")
	}

	if val, err := inMemory.Get(key); err != nil {
		t.Fatalf("expected L1 cache warm, got error: %v", err)
	} else if _, ok := val.(*[]user); !ok {
		t.Fatalf("expected L1 cache to store typed pointer")
	}
}

func TestQuery_ExternalCacheMissStores(t *testing.T) {
	type user struct {
		ID   int
		Name string
	}

	cache := newFakeCache()
	inMemory := NewInMemoryStorage(10, time.Second)
	defer inMemory.Stop()

	stmt := &MockStmt{
		Factory: func() Rows {
			return &MockRows{data: [][]any{{1, "Alice"}}}
		},
	}
	db := NewMockDB()
	db.WithStmt("SELECT * FROM table", stmt)

	client := &MySQL{
		DB:           db,
		dbName:       "db",
		prepare:      make(map[string]Stmt),
		cache:        cache,
		inMemory:     inMemory,
		mutex:        NewMutex(),
		codec:        MsgpackCodec{},
		CacheEnabled: true,
	}

	params := Params{
		Query:          "SELECT * FROM table",
		CacheDelay:     time.Minute,
		NodeCacheDelay: time.Minute,
	}

	res, err := Query(client, params, func(rows Rows) (*[]user, *MySQLError) {
		var users []user
		for rows.Next() {
			var u user
			_ = rows.Scan(&u.ID, &u.Name)
			users = append(users, u)
		}
		return &users, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(*res) != 1 {
		t.Fatalf("expected 1 result, got %d", len(*res))
	}

	key := CreateKey(params, client)
	if cache.setCalls == 0 {
		t.Fatalf("expected cache Set to be called")
	}
	if _, err := cache.Get(key); err != nil {
		t.Fatalf("expected cache entry to be stored, got %v", err)
	}
	if _, err := inMemory.Get(key); err != nil {
		t.Fatalf("expected L1 cache to be warm, got %v", err)
	}
}

func TestQuery_ExternalCacheCorruptEntry(t *testing.T) {
	type user struct {
		ID int
	}

	cache := newFakeCache()
	inMemory := NewInMemoryStorage(10, time.Second)
	defer inMemory.Stop()

	stmt := &MockStmt{
		Factory: func() Rows {
			return &MockRows{data: [][]any{{1}}}
		},
	}
	db := NewMockDB()
	db.WithStmt("SELECT * FROM table", stmt)

	client := &MySQL{
		DB:           db,
		dbName:       "db",
		prepare:      make(map[string]Stmt),
		cache:        cache,
		inMemory:     inMemory,
		mutex:        NewMutex(),
		codec:        MsgpackCodec{},
		CacheEnabled: true,
	}

	params := Params{
		Query:          "SELECT * FROM table",
		CacheDelay:     time.Minute,
		NodeCacheDelay: time.Minute,
	}

	key := CreateKey(params, client)
	_ = cache.Set(key, []byte{0xff, 0x00}, params.CacheDelay)

	_, err := Query(client, params, func(rows Rows) (*[]user, *MySQLError) {
		var users []user
		for rows.Next() {
			var u user
			_ = rows.Scan(&u.ID)
			users = append(users, u)
		}
		return &users, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if db.Prepares == 0 {
		t.Fatalf("expected DB query after cache corruption")
	}
}

func TestQuery_ExternalCacheSerializeError(t *testing.T) {
	type user struct {
		ID int
	}

	cache := newFakeCache()
	inMemory := NewInMemoryStorage(10, time.Second)
	defer inMemory.Stop()

	stmt := &MockStmt{
		Factory: func() Rows {
			return &MockRows{data: [][]any{{1}}}
		},
	}
	db := NewMockDB()
	db.WithStmt("SELECT * FROM table", stmt)

	client := &MySQL{
		DB:           db,
		dbName:       "db",
		prepare:      make(map[string]Stmt),
		cache:        cache,
		inMemory:     inMemory,
		mutex:        NewMutex(),
		codec:        failingCodec{},
		CacheEnabled: true,
	}

	params := Params{
		Query:          "SELECT * FROM table",
		CacheDelay:     time.Minute,
		NodeCacheDelay: time.Minute,
	}

	_, err := Query(client, params, func(rows Rows) (*[]user, *MySQLError) {
		var users []user
		for rows.Next() {
			var u user
			_ = rows.Scan(&u.ID)
			users = append(users, u)
		}
		return &users, nil
	})
	if err == nil || err.Message != "SERIALIZE" {
		t.Fatalf("expected serialize error, got %+v", err)
	}
}

func TestQuery_ExternalCacheLockError(t *testing.T) {
	cache := newFakeCache()
	client := &MySQL{
		DB:           &countingDB{},
		dbName:       "db",
		prepare:      make(map[string]Stmt),
		cache:        cache,
		inMemory:     NewInMemoryStorage(10, time.Second),
		mutex:        &fakeMutex{lockErr: errors.New("lock failed")},
		codec:        MsgpackCodec{},
		CacheEnabled: true,
	}
	defer client.inMemory.Stop()

	params := Params{
		Query:      "SELECT * FROM table",
		CacheDelay: time.Minute,
	}

	res, err := Query(client, params, func(rows Rows) (*[]string, *MySQLError) {
		t.Fatal("callback should not be invoked on lock error")
		return nil, nil
	})
	if err != nil || res != nil {
		t.Fatalf("expected nil result and nil error on lock failure")
	}
}

func TestQuery_ExternalCacheL1Hit(t *testing.T) {
	cache := newFakeCache()
	client, cleanup := newExternalClient(&countingDB{}, cache)
	defer cleanup()

	key := "manual-key"
	expected := []int{1, 2}
	_ = client.inMemory.Set(key, &expected, time.Minute)

	params := Params{
		Key:            key,
		NodeCacheDelay: time.Minute,
	}

	res, err := Query(client, params, func(rows Rows) (*[]int, *MySQLError) {
		t.Fatal("callback should not be invoked on L1 cache hit")
		return nil, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(*res) != 2 {
		t.Fatalf("unexpected cached result: %+v", res)
	}
}

func TestQuery_ExternalCacheDoubleCheckAfterLock(t *testing.T) {
	expected := []string{"ok"}
	data, err := MsgpackCodec{}.Marshal(expected)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	cache := &flipCache{data: data}
	db := &countingDB{}
	client, cleanup := newExternalClient(db, cache)
	defer cleanup()

	params := Params{
		Query:          "SELECT * FROM table",
		CacheDelay:     time.Minute,
		NodeCacheDelay: time.Minute,
	}

	res, qerr := Query(client, params, func(rows Rows) (*[]string, *MySQLError) {
		t.Fatal("callback should not be invoked after cache double-check hit")
		return nil, nil
	})
	if qerr != nil {
		t.Fatalf("unexpected error: %v", qerr)
	}
	if len(*res) != 1 {
		t.Fatalf("unexpected cached result: %+v", res)
	}
	if db.prepares != 0 {
		t.Fatalf("expected DB not to be used on cache double-check hit")
	}
	key := CreateKey(params, client)
	if _, err := client.inMemory.Get(key); err != nil {
		t.Fatalf("expected L1 cache warm, got %v", err)
	}
}

func TestQuery_ExternalPrepareMySQLError(t *testing.T) {
	mysqlErr := &driver.MySQLError{
		Number:   1064,
		SQLState: [5]byte{'4', '2', '0', '0', '0'},
		Message:  "syntax error",
	}

	client, cleanup := newExternalClient(&errDB{err: mysqlErr}, newFakeCache())
	defer cleanup()

	_, err := Query(client, Params{Query: "SELECT 1"}, func(rows Rows) (*[]int, *MySQLError) {
		t.Fatal("callback should not be invoked on prepare error")
		return nil, nil
	})
	if err == nil || err.Number != 1064 {
		t.Fatalf("expected MySQLError to be propagated, got %+v", err)
	}
}

func TestQuery_ExternalPrepareGenericError(t *testing.T) {
	client, cleanup := newExternalClient(&errDB{err: errors.New("prepare failed")}, newFakeCache())
	defer cleanup()

	_, err := Query(client, Params{Query: "SELECT 1"}, func(rows Rows) (*[]int, *MySQLError) {
		t.Fatal("callback should not be invoked on prepare error")
		return nil, nil
	})
	if err == nil || err.Number != 0 {
		t.Fatalf("expected generic prepare error, got %+v", err)
	}
}

func TestQuery_ExternalQueryDeadlock(t *testing.T) {
	stmt := &MockStmt{
		Err: &driver.MySQLError{Number: 1213},
		Factory: func() Rows {
			return &MockRows{data: [][]any{{1}}}
		},
	}
	db := NewMockDB()
	db.WithStmt("SELECT * FROM table", stmt)

	client, cleanup := newExternalClient(db, newFakeCache())
	defer cleanup()

	_, err := Query(client, Params{Query: "SELECT * FROM table"}, func(rows Rows) (*[]int, *MySQLError) {
		t.Fatal("callback should not be invoked on query error")
		return nil, nil
	})
	if err == nil || err.Message != "DEADLOCK" {
		t.Fatalf("expected deadlock error, got %+v", err)
	}
}

func TestQuery_ExternalQueryTimeout(t *testing.T) {
	stmt := &MockStmt{
		Delay: 50 * time.Millisecond,
		Factory: func() Rows {
			return &MockRows{data: [][]any{{1}}}
		},
	}
	db := NewMockDB()
	db.WithStmt("SELECT * FROM table", stmt)

	client, cleanup := newExternalClient(db, newFakeCache())
	defer cleanup()

	_, err := Query(client, Params{
		Query:   "SELECT * FROM table",
		Timeout: 10 * time.Millisecond,
	}, func(rows Rows) (*[]int, *MySQLError) {
		t.Fatal("callback should not be invoked on timeout")
		return nil, nil
	})
	if err == nil || err.Message != "TIMEOUT" {
		t.Fatalf("expected timeout error, got %+v", err)
	}
}

func TestQuery_ExternalQueryMySQLError(t *testing.T) {
	mysqlErr := &driver.MySQLError{Number: 1064, Message: "syntax"}
	stmt := &MockStmt{Err: mysqlErr}
	db := NewMockDB()
	db.WithStmt("SELECT * FROM table", stmt)

	client, cleanup := newExternalClient(db, newFakeCache())
	defer cleanup()

	_, err := Query(client, Params{Query: "SELECT * FROM table"}, func(rows Rows) (*[]int, *MySQLError) {
		t.Fatal("callback should not be invoked on query error")
		return nil, nil
	})
	if err == nil || err.Number != 1064 {
		t.Fatalf("expected MySQLError to be propagated, got %+v", err)
	}
}

func TestQuery_ExternalQueryGenericError(t *testing.T) {
	stmt := &MockStmt{Err: errors.New("boom")}
	db := NewMockDB()
	db.WithStmt("SELECT * FROM table", stmt)

	client, cleanup := newExternalClient(db, newFakeCache())
	defer cleanup()

	_, err := Query(client, Params{Query: "SELECT * FROM table"}, func(rows Rows) (*[]int, *MySQLError) {
		t.Fatal("callback should not be invoked on query error")
		return nil, nil
	})
	if err == nil || err.Number != 0 {
		t.Fatalf("expected generic error, got %+v", err)
	}
}
