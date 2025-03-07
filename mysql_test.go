package mysql

import (
    "testing"

    "github.com/stretchr/testify/assert"
)

func TestConnectionString(t *testing.T) {
    // Test that the connection string is generated correctly.
    t.Run("Generate Connection String", func(t *testing.T) {
        options := Options{
            Host:     "localhost",
            Username: "user",
            Password: "pass",
            Database: "testdb",
            Port:     3306,
        }

        // Expected MySQL connection string format
        expected := "user:pass@tcp(localhost:3306)/testdb?parseTime=true"
        // Generate actual connection string using provided options
        actual := connectionString(options)

        // Assert that the generated connection string matches the expected format
        assert.Equal(t, expected, actual, "Connection string is not generated correctly")
    })
}