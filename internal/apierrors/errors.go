// Package apierrors defines application-level error codes and errors.
package apierrors

import "fmt"

// Code identifies a category of API error.
type Code string

// API error codes
const (
	CodeInvalidArgument   Code = "invalid_argument"
	CodeUnauthenticated   Code = "unauthenticated"
	CodePermissionDenied  Code = "permission_denied"
	CodeNotFound          Code = "not_found"
	CodeConflict          Code = "conflict"
	CodeResourceExhausted Code = "resource_exhausted"
	CodeInternal          Code = "internal"
	CodeUnavailable       Code = "unavailable"
	CodeDeadlineExceeded  Code = "deadline_exceeded"
)

// Error represents an application-level API error.
type Error struct {
	Code    Code
	Message string
}

// New creates an API error with the given code and message.
func New(code Code, message string) *Error {
	return &Error{
		Code:    code,
		Message: message,
	}
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Valid reports whether the error code is recognized.
func (c Code) Valid() bool {
	switch c {
	case CodeInvalidArgument,
		CodeUnauthenticated,
		CodePermissionDenied,
		CodeNotFound,
		CodeConflict,
		CodeResourceExhausted,
		CodeInternal,
		CodeUnavailable,
		CodeDeadlineExceeded:
		return true
	default:
		return false
	}
}
