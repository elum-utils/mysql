package mysql

import (
	"testing"
)

// TestGenerateQuery tests the generateQuery function with various input scenarios.
// It verifies that the function correctly handles both direct SQL queries and
// stored procedure calls with different combinations of parameters.
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
			// When Query field is provided, it should be returned as-is
			// regardless of other parameters (Args, Database, Exec)
			expected: "SELECT * FROM users",
		},
		{
			name:     "with_exec_and_database",
			params:   Params{Database: "app", Exec: "get_user", Args: []any{1, 2}},
			database: "app",
			// Should generate a CALL statement with database qualification
			// and parameter placeholders for each argument
			expected: "CALL app.get_user(?, ?)",
		},
		{
			name:     "with_exec_no_database",
			params:   Params{Exec: "get_user", Args: []any{1, 2, 3}},
			database: "",
			// Should generate a CALL statement without database qualification
			// when Database field is empty
			expected: "CALL get_user(?, ?, ?)",
		},
		{
			name:     "no_args_with_database",
			params:   Params{Database: "app", Exec: "get_all_users", Args: []any{}},
			database: "app",
			// Should generate a CALL statement with empty parentheses
			// when there are no arguments
			expected: "CALL app.get_all_users()",
		},
		{
			name:     "no_args_no_database",
			params:   Params{Exec: "get_all_users", Args: []any{}},
			database: "",
			// Should generate a simple CALL statement without parameters
			// or database qualification
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

// BenchmarkGenerateQuery measures the performance and memory allocation
// characteristics of the generateQuery function across different usage patterns.
// The benchmarks help identify performance bottlenecks and ensure the
// sync.Pool optimization is effective.
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
			// Fast path benchmark: when Query is provided, function should
			// return immediately without any processing
		},
		{
			name:     "exec_with_database_no_args",
			params:   Params{Exec: "procedure", Args: []any{}},
			database: "app",
			// Benchmark: stored procedure call with database qualification
			// but no arguments (minimal string concatenation)
		},
		{
			name:     "exec_no_database_no_args",
			params:   Params{Exec: "procedure", Args: []any{}},
			database: "",
			// Benchmark: simplest stored procedure call without
			// database qualification or arguments
		},
		{
			name:     "exec_with_database",
			params:   Params{Exec: "procedure", Args: []any{1, 2, 3}},
			database: "app",
			// Benchmark: typical stored procedure call with database
			// qualification and a few arguments
		},
		{
			name:     "exec_no_database",
			params:   Params{Exec: "procedure", Args: []any{1, 2}},
			database: "",
			// Benchmark: stored procedure call without database
			// qualification but with arguments
		},
		{
			name:     "many_args",
			params:   Params{Exec: "procedure", Args: []any{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
			database: "app",
			// Benchmark: stress test with many arguments to measure
			// performance with longer generated queries
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs() // Report memory allocations for each benchmark
			for i := 0; i < b.N; i++ {
				_ = generateQuery(tc.params)
			}
		})
	}
}