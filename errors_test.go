package mysql

import (
	"errors"
	"testing"
)

func TestMySQLError_ErrorFormatting(t *testing.T) {
	withState := &MySQLError{
		Number:   1064,
		SQLState: [5]byte{'4', '2', '0', '0', '0'},
		Message:  "syntax error",
	}
	if got := withState.Error(); got != "Error 1064 (42000): syntax error" {
		t.Fatalf("unexpected Error() output: %q", got)
	}

	withoutState := &MySQLError{
		Number:  1064,
		Message: "syntax error",
	}
	if got := withoutState.Error(); got != "Error 1064: syntax error" {
		t.Fatalf("unexpected Error() output without state: %q", got)
	}
}

func TestMySQLError_Is(t *testing.T) {
	base := &MySQLError{Number: 1234}
	if !errors.Is(base, &MySQLError{Number: 1234}) {
		t.Fatalf("expected errors.Is to match on number")
	}
	if errors.Is(base, &MySQLError{Number: 5678}) {
		t.Fatalf("expected errors.Is to fail for different number")
	}
	if errors.Is(base, errors.New("other")) {
		t.Fatalf("expected errors.Is to be false for non-MySQLError")
	}
}

func TestNewError(t *testing.T) {
	err := errors.New("boom")
	got := NewError(err)
	if got.Number != 45000 {
		t.Fatalf("expected error number 45000, got %d", got.Number)
	}
	if got.Message != "boom" {
		t.Fatalf("expected error message to be preserved")
	}
	if got.SQLState != [5]byte{} {
		t.Fatalf("expected SQLState to be zeroed")
	}
}
