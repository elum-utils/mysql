package mysql

import (
	"fmt"
	"strconv"
	"time"
	"unsafe"
)

// CreateKeyBytes создает байтовый ключ с предвычислением точного размера,
// чтобы гарантировать максимум 1 аллокацию даже на больших данных.
func CreateKeyBytes(query string, args ...any) []byte {
	sizeEstimate := len(query)

	for _, item := range args {
		switch v := item.(type) {
		case int, int64, int32, int16, int8,
			uint, uint64, uint32, uint16, uint8:
			sizeEstimate += 20
		case float64, float32:
			sizeEstimate += 24
		case string:
			sizeEstimate += len(v)
		case []byte:
			sizeEstimate += len(v)
		case time.Time:
			sizeEstimate += 19
		case bool:
			sizeEstimate += 5
		default:
			sizeEstimate += 64
		}
	}

	buf := make([]byte, 0, sizeEstimate)
	buf = append(buf, query...)

	for _, item := range args {
		switch v := item.(type) {
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
			buf = v.AppendFormat(buf, "2006-01-02 15:04:05")
		case bool:
			if v {
				buf = append(buf, "true"...)
			} else {
				buf = append(buf, "false"...)
			}
		default:
			// fallback
			buf = append(buf, fmt.Sprintf("%v", v)...)
		}
	}

	return buf
}

// CreateKey возвращает string, без копирования через unsafe
func CreateKey(query string, args ...any) string {
	buf := CreateKeyBytes(query, args...)
	return *(*string)(unsafe.Pointer(&buf))
}
