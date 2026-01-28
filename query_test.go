package mysql

import (
	"context"
	"errors"
	"testing"
	"time"
)

// ---------------------
// HELPERS
// ---------------------

func newMockDBWithRows(data [][]any) *MockDB {
	rowsFactory := func() Rows {
		return &MockRows{data: data}
	}
	stmt := &MockStmt{Factory: rowsFactory}
	db := NewMockDB()
	db.WithStmt("SELECT * FROM table", stmt)
	return db
}

func failPrepareDB() *MockDB {
	return &MockDB{
		Stmts: make(map[string]*MockStmt),
	}
}

// ---------------------
// TESTS
// ---------------------

func TestQuery_Success(t *testing.T) {
	mockDB := newMockDBWithRows([][]any{
		{1, "Alice"},
		{2, "Bob"},
	})

	mysql := &MySQL{
		DB:       mockDB,
		prepare:  make(map[string]Stmt),
		cache:    nil,
		inMemory: nil,
	}

	type User struct {
		ID   int
		Name string
	}

	res, err := Query(mysql, Params{
		Query: "SELECT * FROM table",
	}, func(rows Rows) (*[]User, *MySQLError) {
		var users []User
		for rows.Next() {
			var u User
			_ = rows.Scan(&u.ID, &u.Name)
			users = append(users, u)
		}
		return &users, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %+v", err)
	}
	if len(*res) != 2 {
		t.Fatalf("expected 2 users, got %d", len(*res))
	}
}

func TestQuery_NoRows(t *testing.T) {
	mockDB := newMockDBWithRows([][]any{})
	mysql := &MySQL{
		DB:      mockDB,
		prepare: make(map[string]Stmt),
	}

	type User struct {
		ID   int
		Name string
	}

	res, err := Query(mysql, Params{
		Query: "SELECT * FROM table",
	}, func(rows Rows) (*[]User, *MySQLError) {
		var users []User
		for rows.Next() {
			var u User
			_ = rows.Scan(&u.ID, &u.Name)
			users = append(users, u)
		}
		return &users, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %+v", err)
	}
	if len(*res) != 0 {
		t.Fatalf("expected 0 users, got %d", len(*res))
	}
}

func TestQuery_PrepareError(t *testing.T) {
	mockDB := failPrepareDB()
	mysql := &MySQL{
		DB:      mockDB,
		prepare: make(map[string]Stmt),
	}

	_, err := Query(mysql, Params{Query: "SELECT * FROM missing_table"}, func(rows Rows) (*[]any, *MySQLError) {
		return nil, nil
	})

	if err == nil {
		t.Fatal("expected prepare error, got nil")
	}
}

func TestQuery_QueryError(t *testing.T) {
	stmt := &MockStmt{Err: errors.New("query failed")}

	mockDB := NewMockDB()
	mockDB.WithStmt("SELECT * FROM error_table", stmt)

	mysql := &MySQL{
		DB:      mockDB,
		prepare: make(map[string]Stmt),
	}

	_, err := Query(mysql, Params{Query: "SELECT * FROM error_table"}, func(rows Rows) (*[]any, *MySQLError) {
		return nil, nil
	})

	if err == nil {
		t.Fatal("expected query error, got nil")
	}

	t.Logf("Query returned expected error: %+v", err)
}

func TestQuery_Timeout(t *testing.T) {
	rowsFactory := func() Rows {
		return &MockRows{data: [][]any{{1, "Alice"}}}
	}

	// Имитируем задержку через Delay
	stmt := &MockStmt{
		Factory: rowsFactory,
		Delay:   200 * time.Millisecond, // больше, чем таймаут
	}

	mockDB := NewMockDB()
	mockDB.WithStmt("SELECT * FROM table", stmt)

	mysql := &MySQL{
		DB:       mockDB,
		prepare:  make(map[string]Stmt),
		cache:    nil,
		inMemory: nil,
	}

	type User struct {
		ID   int
		Name string
	}

	start := time.Now()
	_, err := Query(mysql, Params{
		Query:   "SELECT * FROM table",
		Timeout: 50 * time.Millisecond, // таймаут меньше, чем Delay
	}, func(rows Rows) (*[]User, *MySQLError) {
		var users []User
		for rows.Next() {
			var u User
			_ = rows.Scan(&u.ID, &u.Name)
			users = append(users, u)
		}
		return &users, nil
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	if !errors.Is(err, context.DeadlineExceeded) && err.Number != 45000 {
		t.Fatalf("expected timeout MySQLError, got %+v", err)
	}

	t.Logf("Timeout triggered as expected after %v", elapsed)
}

func TestQuery_CacheHit(t *testing.T) {
	rowsFactory := func() Rows {
		return &MockRows{
			data: [][]any{{1, "Alice"}},
		}
	}

	stmt := &MockStmt{Factory: rowsFactory}
	mockDB := NewMockDB()
	mockDB.WithStmt("SELECT * FROM table", stmt)

	inMemory := NewInMemoryStorage(100, time.Second)
	defer inMemory.Stop()

	mysql := &MySQL{
		DB:           mockDB,
		prepare:      make(map[string]Stmt),
		inMemory:     inMemory,
		cache:        nil,
		CacheEnabled: true,
	}

	type User struct {
		ID   int
		Name string
	}

	// 1-й вызов: прогреваем кэш
	res1, err := Query(mysql, Params{
		Query:      "SELECT * FROM table",
		CacheDelay: 2 * time.Second,
	}, func(rows Rows) (*[]User, *MySQLError) {
		var users []User
		for rows.Next() {
			var u User
			_ = rows.Scan(&u.ID, &u.Name)
			users = append(users, u)
		}
		return &users, nil
	})
	if err != nil || len(*res1) != 1 {
		t.Fatal("first query failed")
	}

	// 2-й вызов: должен взять из inMemory cache
	res2, err := Query(mysql, Params{
		Query:      "SELECT * FROM table",
		CacheDelay: 2 * time.Second,
	}, func(rows Rows) (*[]User, *MySQLError) {
		t.Fatal("callback should not be called on cache hit")
		return nil, nil
	})
	if err != nil || len(*res2) != 1 {
		t.Fatal("cache hit failed")
	}
}

// bench

type User struct {
	ID   int
	Name string
}

func BenchmarkQuery_Success(b *testing.B) {
	rowsFactory := func() Rows {
		return &MockRows{
			data: [][]any{
				{1, "Alice"},
				{2, "Bob"},
				{3, "Charlie"},
			},
		}
	}

	stmt := &MockStmt{
		Factory: rowsFactory,
	}
	mockDB := NewMockDB()
	mockDB.WithStmt("SELECT * FROM users", stmt)

	mysql := &MySQL{
		DB:      mockDB,
		prepare: make(map[string]Stmt),
		cache:   nil,
	}

	params := Params{
		Query: "SELECT * FROM users",
	}

	handler := func(rows Rows) (*[]User, *MySQLError) {
		var users []User
		for rows.Next() {
			var u User
			_ = rows.Scan(&u.ID, &u.Name)
			users = append(users, u)
		}
		return &users, nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Query(mysql, params, handler)
	}
}

func BenchmarkQuery_Empty(b *testing.B) {
	rowsFactory := func() Rows {
		return &MockRows{data: [][]any{}}
	}

	stmt := &MockStmt{
		Factory: rowsFactory,
	}
	mockDB := NewMockDB()
	mockDB.WithStmt("SELECT * FROM users WHERE 1=0", stmt)

	mysql := &MySQL{
		DB:      mockDB,
		prepare: make(map[string]Stmt),
		cache:   nil,
	}

	params := Params{
		Query: "SELECT * FROM users WHERE 1=0",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Query(mysql, params, func(rows Rows) (*[]User, *MySQLError) {
			var users []User
			for rows.Next() {
				var u User
				_ = rows.Scan(&u.ID, &u.Name)
				users = append(users, u)
			}
			return &users, nil
		})
	}
}

func BenchmarkQuery_WithDelay(b *testing.B) {
	rowsFactory := func() Rows {
		return &MockRows{
			data: [][]any{
				{1, "Alice"},
			},
		}
	}

	stmt := &MockStmt{
		Factory: rowsFactory,
		Delay:   10 * time.Millisecond, // имитация медленного запроса
	}
	mockDB := NewMockDB()
	mockDB.WithStmt("SELECT * FROM users_delay", stmt)

	mysql := &MySQL{
		DB:      mockDB,
		prepare: make(map[string]Stmt),
		cache:   nil,
	}

	params := Params{
		Query: "SELECT * FROM users_delay",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Query(mysql, params, func(rows Rows) (*[]User, *MySQLError) {
			var users []User
			for rows.Next() {
				var u User
				_ = rows.Scan(&u.ID, &u.Name)
				users = append(users, u)
			}
			return &users, nil
		})
	}
}
