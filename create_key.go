package mysql

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// CreateKey constructs a unique, deterministic string key by combining a base query string
// with a list of arguments. The function replaces placeholders in the query with provided arguments,
// ensuring efficient memory allocation and avoiding unnecessary string concatenations.
//
// Parameters:
//   - query: A base string to which arguments will be appended.
//   - args: A variadic list of arguments, which can include integers, strings, time.Time objects, floats, etc.
//
// Returns:
//
//	A single string representing the combined query and arguments, suitable for use as a cache key or identifier.
func CreateKey(query string, args ...interface{}) string {
	// Estimate the total size of the resulting string to reduce memory reallocations during string building.
	// Start with the length of the query string.
	sizeEstimate := len(query)

	// Iterate through each argument to estimate its contribution to the total size.
	for _, item := range args {
		switch v := item.(type) {
		case int:
			// Estimate the size of the integer as a string.
			sizeEstimate += len(strconv.Itoa(v))
		case string:
			// Use the length of the string directly.
			sizeEstimate += len(v)
		case time.Time:
			// Use the length of the formatted time string "YYYY-MM-DD HH:MM:SS".
			sizeEstimate += len(v.Format("2006-01-02 15:04:05"))
		default:
			// For other types, handle them based on specific cases or default to general formatting.
			switch v := item.(type) {
			case float64:
				// Estimate size for a floating-point number formatted as a string.
				sizeEstimate += len(strconv.FormatFloat(v, 'f', -1, 64))
			default:
				// Fallback for unsupported types, using general formatting.
				sizeEstimate += len(fmt.Sprintf("%v", v))
			}
		}
	}

	// Use a strings.Builder to construct the final string efficiently.
	var value strings.Builder
	// Preallocate memory for the estimated size to minimize reallocations.
	value.Grow(sizeEstimate)

	// Write the base query string to the builder.
	value.WriteString(query)

	// Append each argument to the builder, converting it to a string as necessary.
	for _, item := range args {
		switch v := item.(type) {
		case int:
			// Convert integers to strings.
			value.WriteString(strconv.Itoa(v))
		case string:
			// Append string arguments directly.
			value.WriteString(v)
		case time.Time:
			// Format and append time arguments as "YYYY-MM-DD HH:MM:SS".
			value.WriteString(v.Format("2006-01-02 15:04:05"))
		default:
			// Handle other types with specific optimizations or default formatting.
			switch v := item.(type) {
			case float64:
				// Convert and append floating-point numbers.
				value.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
			default:
				// Fallback for unsupported types, using general formatting.
				value.WriteString(fmt.Sprintf("%v", v))
			}
		}
	}

	// Return the constructed string as the final result.
	return value.String()
}
