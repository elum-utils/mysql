package mysql

import "fmt"

// MySQLError represents a MySQL-specific error with structured information.
// It implements the error interface and provides additional context beyond
// a simple error message, including MySQL error codes and SQL states.
// This type is useful for programmatic error handling and client communication.
type MySQLError struct {
	Number   uint16  // MySQL-specific error code (e.g., 1062 for duplicate entry)
	SQLState [5]byte // ANSI SQL state (5-character code categorizing the error type)
	Message  string  // Human-readable error description
}

// Error implements the error interface for MySQLError.
// Returns a formatted string representation of the error.
// The format includes error number and SQL state (if present) for easy identification.
// Example: "Error 1064 (42000): You have an error in your SQL syntax"
func (me *MySQLError) Error() string {
	if me.SQLState != [5]byte{} {
		// Include SQL state when it's set (not all zeroes)
		return fmt.Sprintf("Error %d (%s): %s", me.Number, me.SQLState, me.Message)
	}
	// Fallback format for errors without SQL state
	return fmt.Sprintf("Error %d: %s", me.Number, me.Message)
}

// Is implements the Is method for error comparison (Go 1.13+ error wrapping).
// It allows errors.Is() to match MySQLError instances by their error number,
// enabling error type checking without exact instance comparison.
// Returns true if both errors are MySQLError instances with the same error number.
func (me *MySQLError) Is(err error) bool {
	if merr, ok := err.(*MySQLError); ok {
		return merr.Number == me.Number
	}
	return false
}

// NewError creates a MySQLError from a standard Go error.
// This is useful for converting generic errors into MySQL-compatible errors
// with a standardized structure. The resulting error uses a generic error
// number (45000) which typically represents "unhandled user-defined exception".
//
// Use this function when you need to propagate errors through MySQL protocol
// or maintain consistent error formatting across the application.
func NewError(err error) *MySQLError {
	return &MySQLError{
		Number:   45000,                  // Generic user-defined error code in MySQL
		SQLState: [5]byte{0, 0, 0, 0, 0}, // Zeroed SQL state indicates no specific category
		Message:  err.Error(),            // Preserve the original error message
	}
}
