package job

import "testing"

func TestJobStatusTransitions(t *testing.T) {
	tests := []struct {
		name string
		from Status
		to   Status
		want bool
	}{
		{
			name: "queued to running",
			from: StatusQueued,
			to:   StatusRunning,
			want: true,
		},
		{
			name: "queued to cancelled",
			from: StatusQueued,
			to:   StatusCancelled,
			want: true,
		},
		{
			name: "running to succeeded",
			from: StatusRunning,
			to:   StatusSucceeded,
			want: true,
		},
		{
			name: "running to failed",
			from: StatusRunning,
			to:   StatusFailed,
			want: true,
		},
		{
			name: "running to cancelled",
			from: StatusRunning,
			to:   StatusCancelled,
			want: true,
		},
		{
			name: "queued cannot succeed directly",
			from: StatusQueued,
			to:   StatusSucceeded,
			want: false,
		},
		{
			name: "queued cannot fail directly",
			from: StatusQueued,
			to:   StatusFailed,
			want: false,
		},
		{
			name: "running cannot return to queued",
			from: StatusRunning,
			to:   StatusQueued,
			want: false,
		},
		{
			name: "succeeded is terminal",
			from: StatusSucceeded,
			to:   StatusRunning,
			want: false,
		},
		{
			name: "failed is terminal",
			from: StatusFailed,
			to:   StatusRunning,
			want: false,
		},
		{
			name: "cancelled is terminal",
			from: StatusCancelled,
			to:   StatusRunning,
			want: false,
		},
		{
			name: "same state is not transition",
			from: StatusRunning,
			to:   StatusRunning,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.from.CanTransitionTo(tt.to); got != tt.want {
				t.Fatalf(
					"%s.CanTransitionTo(%s) = %v, want %v",
					tt.from,
					tt.to,
					got,
					tt.want,
				)
			}
		})
	}
}

func TestJobTerminalStates(t *testing.T) {
	tests := []struct {
		status Status
		want   bool
	}{
		{StatusQueued, false},
		{StatusRunning, false},
		{StatusSucceeded, true},
		{StatusFailed, true},
		{StatusCancelled, true},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			if got := tt.status.Terminal(); got != tt.want {
				t.Fatalf(
					"%s.Terminal() = %v, want %v",
					tt.status,
					got,
					tt.want,
				)
			}
		})
	}
}

func TestJobValidateTransitionRejectsUnknownStatus(t *testing.T) {
	if err := Status("UNKNOWN").ValidateTransition(StatusRunning); err == nil {
		t.Fatal("ValidateTransition() error = nil, want error")
	}

	if err := StatusQueued.ValidateTransition(Status("UNKNOWN")); err == nil {
		t.Fatal("ValidateTransition() error = nil, want error")
	}
}

func TestAttemptStatusTransitions(t *testing.T) {
	tests := []struct {
		name string
		from AttemptStatus
		to   AttemptStatus
		want bool
	}{
		{
			name: "assigned to running",
			from: AttemptStatusAssigned,
			to:   AttemptStatusRunning,
			want: true,
		},
		{
			name: "assigned to failed",
			from: AttemptStatusAssigned,
			to:   AttemptStatusFailed,
			want: true,
		},
		{
			name: "assigned to cancelled",
			from: AttemptStatusAssigned,
			to:   AttemptStatusCancelled,
			want: true,
		},
		{
			name: "running to succeeded",
			from: AttemptStatusRunning,
			to:   AttemptStatusSucceeded,
			want: true,
		},
		{
			name: "running to failed",
			from: AttemptStatusRunning,
			to:   AttemptStatusFailed,
			want: true,
		},
		{
			name: "running to cancelled",
			from: AttemptStatusRunning,
			to:   AttemptStatusCancelled,
			want: true,
		},
		{
			name: "assigned cannot succeed before running",
			from: AttemptStatusAssigned,
			to:   AttemptStatusSucceeded,
			want: false,
		},
		{
			name: "running cannot return to assigned",
			from: AttemptStatusRunning,
			to:   AttemptStatusAssigned,
			want: false,
		},
		{
			name: "succeeded is terminal",
			from: AttemptStatusSucceeded,
			to:   AttemptStatusRunning,
			want: false,
		},
		{
			name: "failed is terminal",
			from: AttemptStatusFailed,
			to:   AttemptStatusRunning,
			want: false,
		},
		{
			name: "cancelled is terminal",
			from: AttemptStatusCancelled,
			to:   AttemptStatusRunning,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.from.CanTransitionTo(tt.to); got != tt.want {
				t.Fatalf(
					"%s.CanTransitionTo(%s) = %v, want %v",
					tt.from,
					tt.to,
					got,
					tt.want,
				)
			}
		})
	}
}

func TestAttemptTerminalStates(t *testing.T) {
	tests := []struct {
		status AttemptStatus
		want   bool
	}{
		{AttemptStatusAssigned, false},
		{AttemptStatusRunning, false},
		{AttemptStatusSucceeded, true},
		{AttemptStatusFailed, true},
		{AttemptStatusCancelled, true},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			if got := tt.status.Terminal(); got != tt.want {
				t.Fatalf(
					"%s.Terminal() = %v, want %v",
					tt.status,
					got,
					tt.want,
				)
			}
		})
	}
}

func TestAttemptValidateTransitionRejectsUnknownStatus(t *testing.T) {
	if err := AttemptStatus("UNKNOWN").
		ValidateTransition(AttemptStatusRunning); err == nil {
		t.Fatal("ValidateTransition() error = nil, want error")
	}

	if err := AttemptStatusAssigned.
		ValidateTransition(AttemptStatus("UNKNOWN")); err == nil {
		t.Fatal("ValidateTransition() error = nil, want error")
	}
}
