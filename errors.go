package mysql

import "fmt"

// MySQLError represents a custom MySQL error structure with additional fields for error handling.
type MySQLError struct {
	Number   uint16  // Error code number.
	SQLState [5]byte // SQL state to categorize the error.
	Message  string  // Descriptive message for the error.
}

// Error returns the error message formatted with its number and SQL state.
func (me *MySQLError) Error() string {
	if me.SQLState != [5]byte{} {
		// Format including SQL state if it's set.
		return fmt.Sprintf("Error %d (%s): %s", me.Number, me.SQLState, me.Message)
	}
	// Format without SQL state.
	return fmt.Sprintf("Error %d: %s", me.Number, me.Message)
}

// Is checks if an error matches the MySQLError based on the error number.
func (me *MySQLError) Is(err error) bool {
	if merr, ok := err.(*MySQLError); ok {
		return merr.Number == me.Number
	}
	return false
}

// NewError creates an instance of MySQLError based on a provided Go error.
// It constructs a custom error with a predefined error number and SQL state.
//
// Parameters:
//   - err: An error object to be converted into a MySQLError.
//
// Returns:
//   - A pointer to a MySQLError instance containing a custom error number (45000),
//     a default SQL state (array of zeroes), and the error message from the provided error.
func NewError(err error) *MySQLError {
	return &MySQLError{
		Number:   45000,                  // Assign a custom error number for general use.
		SQLState: [5]byte{0, 0, 0, 0, 0}, // Set a default SQL state with zeroes.
		Message:  err.Error(),            // Extract and use the error message from the provided error.
	}
}
