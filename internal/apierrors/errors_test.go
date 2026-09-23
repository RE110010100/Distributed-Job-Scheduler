package apierrors

import "testing"

func TestCodeValid(t *testing.T) {
	tests := []struct {
		name string
		code Code
		want bool
	}{
		{"invalid argument", CodeInvalidArgument, true},
		{"unauthenticated", CodeUnauthenticated, true},
		{"permission denied", CodePermissionDenied, true},
		{"not found", CodeNotFound, true},
		{"conflict", CodeConflict, true},
		{"resource exhausted", CodeResourceExhausted, true},
		{"internal", CodeInternal, true},
		{"unavailable", CodeUnavailable, true},
		{"deadline exceeded", CodeDeadlineExceeded, true},
		{"unknown", Code("unknown"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.code.Valid(); got != tt.want {
				t.Fatalf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestError(t *testing.T) {
	err := New(CodeNotFound, "job not found")

	if got, want := err.Error(), "not_found: job not found"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}
