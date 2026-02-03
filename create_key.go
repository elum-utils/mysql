package mysql

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
	"unsafe"
)

// CreateKey generates a cache key from database parameters and query information.
// The key is constructed in the format: "database:queryHash:arg1:arg2:...".
// If no database name is provided and mysql connection is available, the connection's
// database name is used. Query strings are hashed with MD5 for consistent key length.
//
// The function pre-allocates a buffer with exact size to avoid reallocations,
// then constructs the key by concatenating components with ':' separators.
//
// Note: Uses unsafe.Pointer for zero-copy conversion from []byte to string.
// This is safe because the byte slice is not modified after conversion.
func CreateKey(params Params, mysql *MySQL) string {
	// Determine database name for the key
	db := params.Database
	if db == "" && mysql != nil {
		db = mysql.dbName
	}

	// Pre-calculate the required buffer size to allocate once
	size := 0

	// Account for database name and separator
	if db != "" {
		size += len(db) + 1 // +1 for ':' separator
	}

	// Account for query/exec portion
	if params.Exec != "" {
		size += len(params.Exec)
	} else if params.Query != "" {
		size += 32 // MD5 produces 32-character hex string
	} else {
		size += len("unknown")
	}

	// Calculate size needed for all arguments
	for _, arg := range params.Args {
		size++ // For ':' separator before each argument
		switch v := arg.(type) {
		case int, int64, int32, int16, int8,
			uint, uint64, uint32, uint16, uint8:
			// Integers: maximum 20 digits for int64 (including sign)
			size += 20
		case float32, float64:
			// Floats: up to 24 characters for scientific notation
			size += 24
		case string:
			size += len(v)
		case []byte:
			size += len(v)
		case time.Time:
			// "2006-01-02 15:04:05" format is 19 characters
			size += 19
		case bool:
			// "true" or "false" maximum 5 characters
			size += 5
		default:
			// Arbitrary types via fmt.Sprintf
			size += 64
		}
	}

	// Allocate buffer with exact capacity to avoid reallocations
	buf := make([]byte, 0, size)

	if db != "" {
		buf = append(buf, db...)
		buf = append(buf, ':')
	}

	if params.Exec != "" {
		// Use raw exec statement
		buf = append(buf, params.Exec...)
	} else if params.Query != "" {
		// Hash query with MD5 for consistent key length and to avoid
		// storing potentially large queries in cache keys
		sum := md5.Sum([]byte(params.Query))
		var dst [32]byte // MD5 produces 32 hex characters
		hex.Encode(dst[:], sum[:])
		buf = append(buf, dst[:]...)
	} else {
		// Fallback for unknown query type
		buf = append(buf, "unknown"...)
	}

	for _, arg := range params.Args {
		buf = append(buf, ':')
		switch v := arg.(type) {
		case int:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case int32:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int16:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int8:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case uint:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case uint64:
			buf = strconv.AppendUint(buf, v, 10)
		case uint32:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case uint16:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case uint8:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'f', -1, 64)
		case float32:
			buf = strconv.AppendFloat(buf, float64(v), 'f', -1, 32)
		case string:
			buf = append(buf, v...)
		case []byte:
			buf = append(buf, v...)
		case time.Time:
			// Format as MySQL datetime string
			buf = v.AppendFormat(buf, "2006-01-02 15:04:05")
		case bool:
			if v {
				buf = append(buf, "true"...)
			} else {
				buf = append(buf, "false"...)
			}
		default:
			// Use fmt.Sprintf for any other type
			buf = fmt.Appendf(buf, "%v", v)
		}
	}

	// Zero-copy conversion from byte slice to string
	// Safe because buf is not modified after this point
	return *(*string)(unsafe.Pointer(&buf))
}
