package mysql

import "sync"

// keyBufPool is a pool of reusable byte buffers for query generation.
// Each buffer is initially allocated with 1024 bytes capacity to accommodate
// most stored procedure calls without reallocation.
// Using sync.Pool reduces allocation pressure and GC overhead in high-load scenarios.
var keyBufPool = sync.Pool{
	New: func() any {
		// Initial capacity of 1024 bytes balances memory usage and performance
		// for typical stored procedure call queries
		b := make([]byte, 0, 1024)
		return &b
	},
}

// generateQuery constructs a MySQL stored procedure call query from parameters.
// It's optimized for minimal allocations by reusing byte buffers from a pool
// and pre-calculating the exact buffer size needed.
//
// For regular queries (params.Query != ""), returns the query directly.
// For stored procedures, generates: "CALL [database.]procedure_name(?, ?, ...)"
//
// This function is particularly useful when working with prepared statements
// that call stored procedures with variable numbers of parameters.
func generateQuery(params Params) string {
	// Fast path: if a direct query is provided, return it unchanged
	if params.Query != "" {
		return params.Query
	}

	argCount := len(params.Args)
	procLen := len(params.Exec)
	dbLen := len(params.Database)

	// Pre-calculate required buffer size to avoid reallocations
	// Base size: "CALL " (5) + procedure name + "()" (2) = 7
	size := 6 + procLen + 2 // 6 for "CALL " (5 chars + 1 for potential DB prefix)

	// Account for optional database prefix: "database."
	if dbLen > 0 {
		size += dbLen + 1 // +1 for the dot separator
	}

	// Account for parameter placeholders: "?, ?, ..."
	if argCount > 0 {
		// Each parameter adds "?" (1 char), plus ", " (2 chars) between them
		// For n parameters: n * 1 + (n-1) * 2 = 3n - 2 characters
		size += argCount*2 - 1 // Optimized calculation for "?," pattern
	}

	// Get a byte buffer from the pool
	p := keyBufPool.Get().(*[]byte)
	buf := *p

	// Ensure buffer has sufficient capacity
	if cap(buf) < size {
		// Allocate new buffer with exact capacity if pooled one is too small
		buf = make([]byte, 0, size)
	} else {
		// Reuse existing buffer, resetting length to 0
		buf = buf[:0]
	}

	// Build the CALL statement
	buf = append(buf, "CALL "...)

	// Add optional database qualifier
	if dbLen > 0 {
		buf = append(buf, params.Database...)
		buf = append(buf, '.')
	}

	// Add procedure name
	buf = append(buf, params.Exec...)

	// Start parameter list
	buf = append(buf, '(')

	// Add parameter placeholders
	if argCount > 0 {
		for i := 0; i < argCount; i++ {
			if i > 0 {
				// Add separator between parameters
				buf = append(buf, ',', ' ')
			}
			buf = append(buf, '?') // MySQL prepared statement placeholder
		}
	}

	// Close parameter list
	buf = append(buf, ')')

	// Convert to string (allocates new memory for the string)
	// This allocation is necessary as we cannot safely reference the buffer
	result := string(buf)

	// Reset and return buffer to pool for reuse
	// Important: Reset to length 0 to avoid retaining old data
	*p = buf[:0]
	keyBufPool.Put(p)

	return result
}
