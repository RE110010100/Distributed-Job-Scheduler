package job

import "fmt"

// Valid reports whether the status is part of the logical job state machine.
func (s Status) Valid() bool {
	switch s {
	case StatusQueued,
		StatusRunning,
		StatusSucceeded,
		StatusFailed,
		StatusCancelled:
		return true
	default:
		return false
	}
}

// Terminal reports whether no further logical state transition is permitted.
func (s Status) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

// CanTransitionTo reports whether transitioning from s to next is permitted by
// the logical job state machine.
//
// A transition to the same state is not considered a state transition.
// Idempotent command/report handling belongs at the application/persistence
// boundary rather than weakening the state machine.
func (s Status) CanTransitionTo(next Status) bool {
	switch s {
	case StatusQueued:
		return next == StatusRunning ||
			next == StatusCancelled

	case StatusRunning:
		return next == StatusSucceeded ||
			next == StatusFailed ||
			next == StatusCancelled

	default:
		return false
	}
}

// ValidateTransition verifies a logical job state transition.
func (s Status) ValidateTransition(next Status) error {
	if !s.Valid() {
		return fmt.Errorf("invalid current job status %q", s)
	}

	if !next.Valid() {
		return fmt.Errorf("invalid target job status %q", next)
	}

	if !s.CanTransitionTo(next) {
		return fmt.Errorf(
			"invalid job status transition %s -> %s",
			s,
			next,
		)
	}

	return nil
}

// Valid reports whether the status is part of the execution-attempt state
// machine.
func (s AttemptStatus) Valid() bool {
	switch s {
	case AttemptStatusAssigned,
		AttemptStatusRunning,
		AttemptStatusSucceeded,
		AttemptStatusFailed,
		AttemptStatusCancelled:
		return true
	default:
		return false
	}
}

// Terminal reports whether the physical attempt has finished.
func (s AttemptStatus) Terminal() bool {
	switch s {
	case AttemptStatusSucceeded,
		AttemptStatusFailed,
		AttemptStatusCancelled:
		return true
	default:
		return false
	}
}

// CanTransitionTo reports whether the attempt may move to next.
//
// ASSIGNED may become FAILED or CANCELLED without reaching RUNNING because
// assignment, worker failure, cancellation, or startup failure can occur
// before execution begins.
func (s AttemptStatus) CanTransitionTo(next AttemptStatus) bool {
	switch s {
	case AttemptStatusAssigned:
		return next == AttemptStatusRunning ||
			next == AttemptStatusFailed ||
			next == AttemptStatusCancelled

	case AttemptStatusRunning:
		return next == AttemptStatusSucceeded ||
			next == AttemptStatusFailed ||
			next == AttemptStatusCancelled

	default:
		return false
	}
}

// ValidateTransition verifies an execution-attempt state transition.
func (s AttemptStatus) ValidateTransition(next AttemptStatus) error {
	if !s.Valid() {
		return fmt.Errorf("invalid current attempt status %q", s)
	}

	if !next.Valid() {
		return fmt.Errorf("invalid target attempt status %q", next)
	}

	if !s.CanTransitionTo(next) {
		return fmt.Errorf(
			"invalid attempt status transition %s -> %s",
			s,
			next,
		)
	}

	return nil
}
