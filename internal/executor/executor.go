// Package executor executes containerized job assignments.
package executor

import (
	"context"
)

// Assignment contains the worker-local information required to execute
// one physical job attempt.
type Assignment struct {
	JobID          string
	AttemptID      string
	ContainerImage string

	Command     []string
	Args        []string
	Environment map[string]string

	CPULimitMillis   *int64
	MemoryLimitBytes *int64
}

// Executor executes one physical assignment.
type Executor interface {
	Execute(
		ctx context.Context,
		assignment Assignment,
	) error

	Close() error
}
