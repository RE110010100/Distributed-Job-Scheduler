package requestid

import (
	"strings"
	"testing"
)

func TestValid(t *testing.T) {
	tests := []struct {
		name string
		id   string
		want bool
	}{
		{
			name: "simple",
			id:   "req-123",
			want: true,
		},
		{
			name: "uuid",
			id:   "550e8400-e29b-41d4-a716-446655440000",
			want: true,
		},
		{
			name: "empty",
			id:   "",
			want: false,
		},
		{
			name: "contains space",
			id:   "req 123",
			want: false,
		},
		{
			name: "contains newline",
			id:   "req\n123",
			want: false,
		},
		{
			name: "too long",
			id:   strings.Repeat("a", MaxLength+1),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Valid(tt.id); got != tt.want {
				t.Fatalf("Valid(%q) = %v, want %v", tt.id, got, tt.want)
			}
		})
	}
}
