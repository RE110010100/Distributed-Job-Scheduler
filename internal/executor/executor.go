// Package executor executes containerized job assignments.
package executor

import (
	"context"
	"time"
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

	Timeout time.Duration
}

// Outcome represents the terminal state of a job execution.
type Outcome string

// Outcome values for terminal job states.
const (
	OutcomeSucceeded        Outcome = "SUCCEEDED"
	OutcomeFailed           Outcome = "FAILED"
	OutcomeTimedOut         Outcome = "TIMED_OUT"
	OutcomeCancelled        Outcome = "CANCELLED"
	OutcomeResourceExceeded Outcome = "RESOURCE_EXCEEDED"
	OutcomeInfrastructure   Outcome = "INFRASTRUCTURE_ERROR"
)

// FailureCode provides a machine-readable reason for a non-succeeded outcome.
type FailureCode string

// FailureCode values for machine-readable failure reasons.
const (
	FailureNone            FailureCode = ""
	FailureImagePull       FailureCode = "IMAGE_PULL_FAILED"
	FailureContainerCreate FailureCode = "CONTAINER_CREATE_FAILED"
	FailureContainerStart  FailureCode = "CONTAINER_START_FAILED"
	FailureContainerWait   FailureCode = "CONTAINER_WAIT_FAILED"
	FailureNonzeroExit     FailureCode = "NONZERO_EXIT"
	FailureOOMKilled       FailureCode = "OOM_KILLED"
	FailureTimeout         FailureCode = "EXECUTION_TIMEOUT"
	FailureCancelled       FailureCode = "EXECUTION_CANCELLED"
	FailureCleanup         FailureCode = "CONTAINER_CLEANUP_FAILED"
	FailureInvalidSpec     FailureCode = "INVALID_SPECIFICATION"
)

// ExecutionResult holds the outcome and metadata for a completed job execution.
type ExecutionResult struct {
	JobID       string
	AttemptID   string
	ContainerID string

	Outcome     Outcome
	FailureCode FailureCode
	Message     string

	ExitCode  *int64
	OOMKilled bool

	StartedAt   time.Time
	CompletedAt time.Time
}

// Executor runs a job assignment and returns the execution result.
type Executor interface {
	Execute(
		ctx context.Context,
		assignment Assignment,
	) (ExecutionResult, error)

	Close() error
}
