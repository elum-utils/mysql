# MySQL Package for Go

A high-performance, feature-rich MySQL client library for Go with built-in caching, connection pooling, and advanced query capabilities.

## Features

- **Dual-Level Caching**: In-memory (L1) and external (L2) cache support with configurable TTL
- **Prepared Statement Caching**: Reduces database server overhead for repeated queries
- **Stored Procedure Support**: First-class support for MySQL stored procedures
- **Keyed Mutexes**: Per-resource locking for distributed synchronization
- **Timeout Management**: Configurable timeouts for all operations
- **Connection Pooling**: Efficient connection management with configurable limits
- **Mock Testing**: Comprehensive mock framework for unit testing
- **Codec Support**: Pluggable serialization (default: MessagePack)
- **Thread-Safe**: All operations are safe for concurrent use

## Installation

```bash
go get github.com/yourusername/mysql
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "github.com/yourusername/mysql"
    "time"
)

func main() {
    // Create a MySQL connection with caching enabled
    db, err := mysql.New(mysql.Options{
        Host:         "localhost",
        Port:         3306,
        Username:     "user",
        Password:     "password",
        Database:     "mydb",
        CacheEnabled: true,
        CacheSize:    100, // MB
    })
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Execute a query with caching
    type User struct {
        ID   int    `msgpack:"id"`
        Name string `msgpack:"name"`
    }
    
    users, err := mysql.Query(db, mysql.Params{
        Query:      "SELECT id, name FROM users WHERE active = ?",
        Args:       []any{true},
        CacheDelay: 5 * time.Minute,
    }, func(rows mysql.Rows) (*[]User, *mysql.MySQLError) {
        var result []User
        for rows.Next() {
            var u User
            _ = rows.Scan(&u.ID, &u.Name)
            result = append(result, u)
        }
        return &result, nil
    })
    
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }
    
    fmt.Printf("Found %d users\n", len(*users))
}
```

## Advanced Usage

### Stored Procedures

```go
// Call a stored procedure
result, err := mysql.Query(db, mysql.Params{
    Exec:     "get_user_stats",
    Database: "analytics",
    Args:     []any{userID, startDate, endDate},
}, func(rows mysql.Rows) (*Stats, *mysql.MySQLError) {
    // Process results
})
```

### Custom Cache Implementation

```go
type RedisCache struct{}

func (r *RedisCache) Get(key string) ([]byte, error) {
    // Implement cache retrieval
}

func (r *RedisCache) Set(key string, val []byte, exp time.Duration) error {
    // Implement cache storage
}

// Use with MySQL client
db, err := mysql.New(mysql.Options{
    Cache: &RedisCache{},
    CacheEnabled: true,
})
```

### Distributed Locking

```go
// Use keyed mutex for distributed locking
mutex := mysql.NewMutex()

// Lock a specific resource
mutex.Lock("user:123:profile")
defer mutex.Unlock("user:123:profile")

// Perform operations on the resource
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Host` | `string` | `"localhost"` | MySQL server hostname |
| `Port` | `int` | `3306` | MySQL server port |
| `Username` | `string` | (required) | Authentication username |
| `Password` | `string` | (required) | Authentication password |
| `Database` | `string` | (required) | Database name |
| `MaxConnections` | `int` | `0` | Maximum open connections (0 = driver default) |
| `CacheEnabled` | `bool` | `false` | Enable query caching |
| `CacheSize` | `int` | `10` | Cache size in MB |
| `CacheTTLCheck` | `time.Duration` | `5m` | Cache cleanup interval |
| `Timeout` | `int` | `30` | Connection timeout in seconds |
| `ReadTimeout` | `int` | `30` | Read timeout in seconds |
| `WriteTimeout` | `int` | `30` | Write timeout in seconds |
| `Charset` | `string` | `"utf8mb4"` | Connection charset |
| `Collation` | `string` | `"utf8mb4_unicode_ci"` | Connection collation |
| `ConnectionString` | `string` | `""` | Pre-built DSN (overrides other connection options) |

## Caching Strategy

The package implements a sophisticated dual-level caching strategy:

1. **L1 Cache (In-Memory)**: Local to each application instance using LRU with TTL
2. **L2 Cache (External)**: Shared cache (Redis, Memcached, etc.) for distributed applications

Cache keys are automatically generated from query parameters, or can be specified manually. The system includes protection against cache stampede using distributed locking.

## Error Handling

All errors are returned as `MySQLError` structs with MySQL error codes, SQL states, and descriptive messages:

```go
result, err := mysql.Query(db, params, callback)
if err != nil {
    switch err.Number {
    case 1213:
        // Handle deadlock
    case 45000:
        // Handle custom application error
    default:
        // Handle other errors
    }
}
```

## Testing

The package includes a comprehensive mock framework for unit testing:

```go
func TestMyFunction(t *testing.T) {
    // Create mock database with predefined results
    mockDB := mysql.NewMockDB()
    mockDB.WithStmt("SELECT * FROM users", &mysql.MockStmt{
        Factory: func() mysql.Rows {
            return &mysql.MockRows{
                data: [][]any{{1, "Alice"}, {2, "Bob"}},
            }
        },
    })
    
    // Test your code with the mock
    // ...
}
```

## Performance Considerations

- **Prepared Statement Caching**: Statements are cached per connection to reduce database overhead
- **Buffer Pooling**: Query generation uses `sync.Pool` for byte buffers to reduce allocations
- **LRU Eviction**: In-memory cache uses LRU with configurable size limits
- **Zero-Copy Conversions**: Efficient string conversion techniques where possible

## Benchmarks

Run the included benchmarks:

```bash
go test -bench=. -benchmem ./...
```

Benchmarks measure:
- Query execution with various result sizes
- Cache hit/miss performance
- Concurrent access patterns
- Memory allocation patterns

## Best Practices

1. **Enable caching** for read-heavy workloads with stable queries
2. **Use prepared statements** for repeated queries with different parameters
3. **Set appropriate timeouts** based on your SLA requirements
4. **Monitor cache hit ratios** to optimize cache size and TTL
5. **Use connection pooling** for high-concurrency applications
6. **Implement circuit breakers** for database availability
