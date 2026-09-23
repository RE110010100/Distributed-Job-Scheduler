package apierrors

import "fmt"

type Code string

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

type Error struct {
	Code    Code
	Message string
}

func New(code Code, message string) *Error {
	return &Error{
		Code:    code,
		Message: message,
	}
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

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
