// Package requestid provides utilities for working with request identifiers.
package requestid

// Request ID header names.
const (
	HTTPHeader   = "X-Request-ID"
	GRPCMetadata = "x-request-id"

	MaxLength = 128
)

// Valid reports whether id is a valid request identifier.
func Valid(id string) bool {
	if len(id) == 0 || len(id) > MaxLength {
		return false
	}

	for i := 0; i < len(id); i++ {
		if id[i] < 0x21 || id[i] > 0x7e {
			return false
		}
	}

	return true
}
