package mysql

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
