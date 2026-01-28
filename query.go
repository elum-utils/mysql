package mysql

import (
	"context"
	"errors"
	"time"

	"github.com/go-sql-driver/mysql"
)

type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
}

// Params is a structure for storing query parameters used in the Query function
type Params struct {
	Key            string        // Cache key (if caching is enabled)
	Database       string        //
	Query          string        // SQL query string
	Exec           string        // Stored procedure or SQL executable string
	Args           []any         // Arguments for the SQL query
	Timeout        time.Duration // Timeout for the query execution
	CacheDelay     time.Duration // Cache delay time (time to keep data in cache)
	NodeCacheDelay time.Duration //
}

// getPreparedStatement retrieves a prepared SQL statement from the cache or prepares a new one
func (c *MySQL) getPreparedStatement(ctx context.Context, query string) (Stmt, error) {
	c.mx.Lock()
	defer c.mx.Unlock()

	if stmt, ok := c.prepare[query]; ok {
		return stmt, nil
	}

	stmt, err := c.DB.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	c.prepare[query] = stmt
	return stmt, nil
}

func Query[T any](
	c *MySQL,
	params Params,
	callback func(rows Rows) (*T, *MySQLError),
) (*T, *MySQLError) {

	if c.cache == nil {
		return internalQuery(c, params, callback)
	}

	return externalQuery(c, params, callback)

}

func externalQuery[T any](
	c *MySQL,
	params Params,
	callback func(rows Rows) (*T, *MySQLError),
) (*T, *MySQLError) {

	query := generateQuery(params)
	var key string
	if params.Key == "" {
		key = CreateKey(query, params.Args...)
	} else {
		key = params.Key
	}

	mutexKey := "mutex_" + key

	if params.NodeCacheDelay > 0 && c.CacheEnabled {
		if val, err := c.inMemory.Get(key); err == nil {
			if res, ok := val.(*T); ok {
				return res, nil
			}
		}
	}

	if params.CacheDelay > 0 && c.CacheEnabled {
		if res := checkExternalCache[T](c, key); res != nil {
			// прогреваем L1
			if params.NodeCacheDelay > 0 {
				c.inMemory.Set(key, res, params.NodeCacheDelay)
			}
			return res, nil
		}

		// блокируем для одновременных запросов одного ключа
		if err := c.mutex.Lock(mutexKey); err != nil {
			return nil, nil
		}
		defer c.mutex.Unlock(mutexKey)

		// повторная проверка после Lock
		if res := checkExternalCache[T](c, key); res != nil {
			if params.NodeCacheDelay > 0 {
				c.inMemory.Set(key, res, params.NodeCacheDelay)
			}
			return res, nil
		}
	}

	ctx, cancel := createContextWithTimeout(params.Timeout)
	defer cancel()

	prepare, err := c.getPreparedStatement(ctx, query)
	if err != nil {
		if sqlErr, ok := err.(*mysql.MySQLError); ok {
			return nil, &MySQLError{
				Number:   sqlErr.Number,
				SQLState: sqlErr.SQLState,
				Message:  sqlErr.Message,
			}
		}
		return nil, &MySQLError{}
	}

	rows, err := prepare.QueryContext(ctx, params.Args...)
	if err != nil {
		if sqlErr, ok := err.(*mysql.MySQLError); ok && sqlErr.Number == 1213 {
			return nil, &MySQLError{Number: 45000, Message: "DEADLOCK"}
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, &MySQLError{Number: 45000, Message: "TIMEOUT"}
		}
		if sqlErr, ok := err.(*mysql.MySQLError); ok {
			return nil, &MySQLError{
				Number:   sqlErr.Number,
				SQLState: sqlErr.SQLState,
				Message:  sqlErr.Message,
			}
		}
		return nil, &MySQLError{}
	}
	defer rows.Close()

	clbRes, clbErr := callback(rows)

	if clbErr == nil && clbRes != nil {
		// Внешний кеш
		if params.CacheDelay > 0 && c.CacheEnabled {
			data, err := c.codec.Marshal(clbRes)
			if err != nil {
				return clbRes, &MySQLError{Number: 45000, Message: "SERIALIZE"}
			}
			_ = c.cache.Set(key, data, params.CacheDelay)

			// L1 in-memory
			if params.NodeCacheDelay > 0 {
				c.inMemory.Set(key, clbRes, params.NodeCacheDelay)
			}
		}
	}

	return clbRes, clbErr

}

func internalQuery[T any](
	c *MySQL,
	params Params,
	callback func(rows Rows) (*T, *MySQLError),
) (*T, *MySQLError) {

	query := generateQuery(params)
	var key string
	if params.Key == "" {
		key = CreateKey(query, params.Args...)
	} else {
		key = params.Key
	}

	if params.CacheDelay > 0 {
		if val, err := c.inMemory.Get(key); err == nil {
			if res, ok := val.(*T); ok {
				return res, nil
			}
		}
	}

	ctx, cancel := createContextWithTimeout(params.Timeout)
	defer cancel()

	prepare, err := c.getPreparedStatement(ctx, query)
	if err != nil {
		if sqlErr, ok := err.(*mysql.MySQLError); ok {
			return nil, &MySQLError{
				Number:   sqlErr.Number,
				SQLState: sqlErr.SQLState,
				Message:  sqlErr.Message,
			}
		}
		return nil, &MySQLError{}
	}

	rows, err := prepare.QueryContext(ctx, params.Args...)
	if err != nil {
		if sqlErr, ok := err.(*mysql.MySQLError); ok && sqlErr.Number == 1213 {
			return nil, &MySQLError{Number: 45000, Message: "DEADLOCK"}
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, &MySQLError{Number: 45000, Message: "TIMEOUT"}
		}
		if sqlErr, ok := err.(*mysql.MySQLError); ok {
			return nil, &MySQLError{
				Number:   sqlErr.Number,
				SQLState: sqlErr.SQLState,
				Message:  sqlErr.Message,
			}
		}
		return nil, &MySQLError{}
	}
	defer rows.Close()

	clbRes, clbErr := callback(rows)

	if clbErr == nil && clbRes != nil && params.CacheDelay > 0 {
		c.inMemory.Set(key, clbRes, params.CacheDelay)
	}

	return clbRes, clbErr
}

func createContextWithTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout == 0 {
		timeout = 100 * time.Second
	}
	return context.WithTimeout(context.Background(), timeout)
}

func checkExternalCache[T any](c *MySQL, key string) *T {
	data, err := c.cache.Get(key)
	if err != nil {
		return nil
	}

	var obj T
	if err := c.codec.Unmarshal(data, &obj); err != nil {
		return nil
	}
	return &obj
}
