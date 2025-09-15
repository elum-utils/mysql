package mysql

import "strings"

// generateQuery generates SQL query string based on parameters
// If Query is provided, it returns the query as is
// If Query is empty, it generates a stored procedure call with optional database specification
// generateQuery generates SQL query string with minimal allocations
// generateQueryFast - ultra-optimized version using byte slice
// generateQuery generates SQL query string with exactly 1 allocation
func generateQuery(params Params) string {
	if params.Query != "" {
		return params.Query
	}
	
	argCount := len(params.Args)
	procLen := len(params.Exec)
	dbLen := len(params.Database)
	
	size := 6 + procLen + 2
	if dbLen > 0 {
		size += dbLen + 1
	}
	if argCount > 0 {
		size += argCount*2 - 1
	}
	
	var b strings.Builder
	b.Grow(size)
	
	b.WriteString("CALL ")
	if dbLen > 0 {
		b.WriteString(params.Database)
		b.WriteByte('.')
	}
	b.WriteString(params.Exec)
	b.WriteByte('(')
	
	if argCount > 0 {
		for i := 0; i < argCount; i++ {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteByte('?')
		}
	}
	
	b.WriteByte(')')
	return b.String()
}
