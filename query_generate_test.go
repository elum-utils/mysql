package mysql

import (
	"testing"
)

func TestGenerateQuery(t *testing.T) {
	tests := []struct {
		name     string
		params   Params
		database string
		expected string
	}{
		{
			name:     "with_query_provided",
			params:   Params{Query: "SELECT * FROM users", Args: []any{1, 2}},
			database: "app",
			expected: "SELECT * FROM users",
		},
		{
			name:     "with_exec_and_database",
			params:   Params{Database: "app", Exec: "get_user", Args: []any{1, 2}},
			database: "app",
			expected: "CALL app.get_user(?, ?)",
		},
		{
			name:     "with_exec_no_database",
			params:   Params{Exec: "get_user", Args: []any{1, 2, 3}},
			database: "",
			expected: "CALL get_user(?, ?, ?)",
		},
		{
			name:     "no_args_with_database",
			params:   Params{Database: "app", Exec: "get_all_users", Args: []any{}},
			database: "app",
			expected: "CALL app.get_all_users()",
		},
		{
			name:     "no_args_no_database",
			params:   Params{Exec: "get_all_users", Args: []any{}},
			database: "",
			expected: "CALL get_all_users()",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateQuery(tt.params)
			if result != tt.expected {
				t.Errorf("generateQuery() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func BenchmarkGenerateQuery(b *testing.B) {
	testCases := []struct {
		name     string
		params   Params
		database string
	}{
		{
			name:     "query_provided",
			params:   Params{Query: "SELECT * FROM table"},
			database: "",
		},
		{
			name:     "exec_with_database_no_args",
			params:   Params{Exec: "procedure", Args: []any{}},
			database: "app",
		},
		{
			name:     "exec_no_database_no_args",
			params:   Params{Exec: "procedure", Args: []any{}},
			database: "",
		},
		{
			name:     "exec_with_database",
			params:   Params{Exec: "procedure", Args: []any{1, 2, 3}},
			database: "app",
		},
		{
			name:     "exec_no_database",
			params:   Params{Exec: "procedure", Args: []any{1, 2}},
			database: "",
		},
		{
			name:     "many_args",
			params:   Params{Exec: "procedure", Args: []any{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
			database: "app",
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = generateQuery(tc.params)
			}
		})
	}
}
