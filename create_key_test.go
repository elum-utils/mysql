package mysql

import (
	"strings"
	"testing"
	"time"
)

func TestCreateKeyWithMySQL(t *testing.T) {
	mysql := &MySQL{
		dbName: "shop",
	}

	tests := []struct {
		name   string
		mysql  *MySQL
		params Params
		expect string
	}{
		{
			name:  "exec_with_args_and_db_from_mysql",
			mysql: mysql,
			params: Params{
				Exec: "product_get",
				Args: []any{746457348, 20, 350},
			},
			expect: "shop:product_get:746457348:20:350",
		},
		{
			name:  "exec_with_args_and_db_from_params",
			mysql: mysql,
			params: Params{
				Database: "catalog",
				Exec:     "product_get",
				Args:     []any{1},
			},
			expect: "catalog:product_get:1",
		},
		{
			name:  "query_hash_used_when_exec_empty",
			mysql: mysql,
			params: Params{
				Query: "SELECT * FROM users WHERE id = ?",
				Args:  []any{42},
			},
			expect: "shop:f15e5e09c27c92be6ed2b586d171d68a:42",
		},
		{
			name:  "no_database_anywhere",
			mysql: &MySQL{},
			params: Params{
				Exec: "ping",
				Args: []any{},
			},
			expect: "ping",
		},
		{
			name:  "string_and_time_args",
			mysql: mysql,
			params: Params{
				Exec: "user_create",
				Args: []any{
					"John",
					time.Date(2024, 11, 17, 10, 0, 0, 0, time.UTC),
				},
			},
			expect: "shop:user_create:John:2024-11-17 10:00:00",
		},
		{
			name:  "large_string_arg",
			mysql: mysql,
			params: Params{
				Exec: "blob_set",
				Args: []any{strings.Repeat("A", 1024)},
			},
			expect: "shop:blob_set:" + strings.Repeat("A", 1024),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := CreateKey(tt.params, tt.mysql)
			if key != tt.expect {
				t.Fatalf(
					"unexpected key\nexpected: %q\ngot:      %q",
					tt.expect,
					key,
				)
			}
		})
	}
}

func BenchmarkCreateKeyWithMySQL_Exec(b *testing.B) {
	mysql := &MySQL{
		dbName: "shop",
	}
	params := Params{
		Exec: "product_get",
		Args: []any{746457348, 20, 350},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = CreateKey(params, mysql)
	}
}

func BenchmarkCreateKeyWithMySQL_Exec_Small(b *testing.B) {
	mysql := &MySQL{dbName: "shop"}
	params := Params{
		Exec: "product_get",
		Args: []any{42, "John"},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = CreateKey(params, mysql)
	}
}

func BenchmarkCreateKeyWithMySQL_Query(b *testing.B) {
	mysql := &MySQL{dbName: "shop"}

	params := Params{
		Query: "SELECT * FROM users WHERE id = ? AND name = ?",
		Args:  []any{42, "John"},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = CreateKey(params, mysql)
	}
}

func TestCreateKey_ArgTypes(t *testing.T) {
	type custom struct {
		ID int
	}

	mysql := &MySQL{dbName: "db"}
	params := Params{
		Args: []any{
			int8(-5),
			uint16(7),
			float32(1.25),
			float64(2.5),
			[]byte("bin"),
			true,
			false,
			custom{ID: 1},
		},
	}

	key := CreateKey(params, mysql)
	expected := "db:unknown:-5:7:1.25:2.5:bin:true:false:{1}"
	if key != expected {
		t.Fatalf("unexpected key\nexpected: %q\ngot:      %q", expected, key)
	}
}

func TestCreateKey_IntegerVariants(t *testing.T) {
	mysql := &MySQL{dbName: "db"}
	params := Params{
		Exec: "proc",
		Args: []any{
			int64(-1),
			int32(2),
			int16(3),
			uint(4),
			uint64(5),
			uint32(6),
			uint8(7),
		},
	}

	key := CreateKey(params, mysql)
	expected := "db:proc:-1:2:3:4:5:6:7"
	if key != expected {
		t.Fatalf("unexpected key\nexpected: %q\ngot:      %q", expected, key)
	}
}
