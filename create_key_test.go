package mysql

import (
	"strings"
	"testing"
	"time"
)

// TestCreateKey tests the CreateKey (string) function.
func TestCreateKey(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		args     []any
		expected string
	}{
		{
			name:     "simple_int",
			query:    "SELECT * FROM users WHERE id = ?",
			args:     []any{42},
			expected: "SELECT * FROM users WHERE id = ?42",
		},
		{
			name:     "string_and_time",
			query:    "INSERT INTO users (name, created_at) VALUES (?, ?)",
			args:     []any{"John Doe", time.Date(2024, 11, 17, 10, 0, 0, 0, time.UTC)},
			expected: "INSERT INTO users (name, created_at) VALUES (?, ?)John Doe2024-11-17 10:00:00",
		},
		{
			name:     "multiple_args",
			query:    "SELECT name FROM users WHERE age > ? AND country = ?",
			args:     []any{30, "USA"},
			expected: "SELECT name FROM users WHERE age > ? AND country = ?30USA",
		},
		{
			name:     "large_string",
			query:    "SELECT * FROM data WHERE content = ?",
			args:     []any{strings.Repeat("A", 1024)},
			expected: "SELECT * FROM data WHERE content = ?" + strings.Repeat("A", 1024),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := CreateKey(test.query, test.args...)
			if result != test.expected {
				t.Errorf("expected %d chars, got %d chars", len(test.expected), len(result))
				if len(test.expected) < 100 && len(result) < 100 {
					t.Errorf("expected '%v', got '%v'", test.expected, result)
				}
			}
		})
	}
}

// TestCreateKeyBytes tests the CreateKeyBytes ([]byte) function.
func TestCreateKeyBytes(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		args     []any
		expected string
	}{
		{
			name:     "simple_int",
			query:    "SELECT * FROM users WHERE id = ?",
			args:     []any{42},
			expected: "SELECT * FROM users WHERE id = ?42",
		},
		{
			name:     "string_and_time",
			query:    "INSERT INTO users (name, created_at) VALUES (?, ?)",
			args:     []any{"John Doe", time.Date(2024, 11, 17, 10, 0, 0, 0, time.UTC)},
			expected: "INSERT INTO users (name, created_at) VALUES (?, ?)John Doe2024-11-17 10:00:00",
		},
		{
			name:     "multiple_args",
			query:    "SELECT name FROM users WHERE age > ? AND country = ?",
			args:     []any{30, "USA"},
			expected: "SELECT name FROM users WHERE age > ? AND country = ?30USA",
		},
		{
			name:     "large_string",
			query:    "SELECT * FROM data WHERE content = ?",
			args:     []any{strings.Repeat("B", 2048)},
			expected: "SELECT * FROM data WHERE content = ?" + strings.Repeat("B", 2048),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := string(CreateKeyBytes(test.query, test.args...))
			if result != test.expected {
				t.Errorf("expected %d chars, got %d chars", len(test.expected), len(result))
				if len(test.expected) < 100 && len(result) < 100 {
					t.Errorf("expected '%v', got '%v'", test.expected, result)
				}
			}
		})
	}
}

// BenchmarkCreateKey benchmarks the CreateKey (string) version.
func BenchmarkCreateKey(b *testing.B) {
	smallQuery := "SELECT * FROM users WHERE id = ? AND name = ?"
	smallArgs := []any{42, "John Doe"}

	largeQuery := "SELECT * FROM large_table WHERE content LIKE ? AND category = ? AND created_at > ?"
	largeString := strings.Repeat("X", 1024) // 1KB строка
	largeArgs := []any{"%" + largeString + "%", "test_category", time.Now()}

	b.Run("small_input", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = CreateKey(smallQuery, smallArgs...)
		}
	})

	b.Run("large_input_1kb", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = CreateKey(largeQuery, largeArgs...)
		}
	})

	b.Run("very_large_input", func(b *testing.B) {
		veryLargeString := strings.Repeat("Y", 10*1024) // 10KB строка
		args := []any{veryLargeString, 123, true}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = CreateKey("SELECT * FROM huge_data WHERE data = ?", args...)
		}
	})
}

// BenchmarkCreateKeyBytes benchmarks the CreateKeyBytes ([]byte) version.
func BenchmarkCreateKeyBytes(b *testing.B) {
	smallQuery := "SELECT * FROM users WHERE id = ? AND name = ?"
	smallArgs := []any{42, "John Doe"}

	largeQuery := "SELECT * FROM large_table WHERE content LIKE ? AND category = ? AND created_at > ?"
	largeString := strings.Repeat("X", 1024) // 1KB строка
	largeArgs := []any{"%" + largeString + "%", "test_category", time.Now()}

	b.Run("small_input", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = CreateKeyBytes(smallQuery, smallArgs...)
		}
	})

	b.Run("large_input_1kb", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = CreateKeyBytes(largeQuery, largeArgs...)
		}
	})

	b.Run("very_large_input", func(b *testing.B) {
		veryLargeString := strings.Repeat("Y", 10*1024) // 10KB строка
		args := []any{veryLargeString, 123, true}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = CreateKeyBytes("SELECT * FROM huge_data WHERE data = ?", args...)
		}
	})
}

// BenchmarkCreateKeyMixed benchmarks with mixed argument types
func BenchmarkCreateKeyMixed(b *testing.B) {
	query := "INSERT INTO table (str, num, float, time, bool) VALUES (?, ?, ?, ?, ?)"
	args := []any{
		strings.Repeat("test", 256), // 1KB строка
		42,
		3.14,
		time.Now(),
		true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CreateKey(query, args...)
	}
}
