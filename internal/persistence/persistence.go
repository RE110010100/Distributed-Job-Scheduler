// Package persistence defines the storage interfaces used by the control plane.
package persistence

import (
	"context"
	"errors"
	"time"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/job"
)

var (
	// ErrNotFound is returned when the requested record does not exist.
	ErrNotFound = errors.New("persistence: not found")
	// ErrConflict is returned when an optimistic concurrency check fails.
	ErrConflict = errors.New("persistence: version conflict")
)

// JobRepository persists logical jobs.
type JobRepository interface {
	CreateJob(ctx context.Context, j *job.Job) error

	CreateJobIdempotent(
		ctx context.Context,
		j *job.Job,
	) (*job.Job, bool, error)

	GetJob(ctx context.Context, id job.ID) (*job.Job, error)

	TransitionJobStatus(
		ctx context.Context,
		id job.ID,
		from job.Status,
		to job.Status,
		expectedVersion uint64,
		at time.Time,
	) (*job.Job, error)
}

// AttemptRepository persists physical execution attempts.
type AttemptRepository interface {
	CreateAttempt(
		ctx context.Context,
		attempt *job.ExecutionAttempt,
	) error

	GetAttempt(
		ctx context.Context,
		id job.AttemptID,
	) (*job.ExecutionAttempt, error)

	TransitionAttemptStatus(
		ctx context.Context,
		id job.AttemptID,
		from job.AttemptStatus,
		to job.AttemptStatus,
		expectedVersion uint64,
		at time.Time,
		failure *job.FailureInformation,
	) (*job.ExecutionAttempt, error)
}

// Repository is the persistence capability required by the control plane.
type Repository interface {
	JobRepository
	AttemptRepository
}
