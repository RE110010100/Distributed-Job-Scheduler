package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/job"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/persistence"
)

// CreateAttempt inserts a new execution attempt and sets its version to 1.
func (s *Store) CreateAttempt(
	ctx context.Context,
	attempt *job.ExecutionAttempt,
) error {
	failureInformation, err := marshalFailure(
		attempt.FailureInformation,
	)
	if err != nil {
		return err
	}

	const query = `
		INSERT INTO execution_attempts (
			attempt_id,
			job_id,
			worker_id,
			attempt_number,
			status,
			version,
			started_at,
			completed_at,
			failure_information
		)
		VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8)
	`

	_, err = s.pool.Exec(
		ctx,
		query,
		attempt.ID,
		attempt.JobID,
		attempt.WorkerID,
		attempt.AttemptNumber,
		attempt.Status,
		attempt.StartedAt,
		attempt.CompletedAt,
		failureInformation,
	)
	if err != nil {
		return fmt.Errorf(
			"insert execution attempt %q: %w",
			attempt.ID,
			err,
		)
	}

	attempt.Version = 1

	return nil
}

// GetAttempt returns the execution attempt with the given ID, or persistence.ErrNotFound.
func (s *Store) GetAttempt(
	ctx context.Context,
	id job.AttemptID,
) (*job.ExecutionAttempt, error) {
	const query = `
		SELECT
			attempt_id,
			job_id,
			worker_id,
			attempt_number,
			status,
			version,
			started_at,
			completed_at,
			failure_information
		FROM execution_attempts
		WHERE attempt_id = $1
	`

	return scanAttempt(s.pool.QueryRow(ctx, query, id))
}

func scanAttempt(
	row rowScanner,
) (*job.ExecutionAttempt, error) {
	var (
		attempt            job.ExecutionAttempt
		attemptNumber      int32
		version            int64
		failureInformation []byte
	)

	err := row.Scan(
		&attempt.ID,
		&attempt.JobID,
		&attempt.WorkerID,
		&attemptNumber,
		&attempt.Status,
		&version,
		&attempt.StartedAt,
		&attempt.CompletedAt,
		&failureInformation,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, persistence.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan execution attempt: %w", err)
	}

	if attemptNumber <= 0 {
		return nil, fmt.Errorf(
			"invalid persisted attempt number %d",
			attemptNumber,
		)
	}

	if version <= 0 {
		return nil, fmt.Errorf(
			"invalid persisted attempt version %d",
			version,
		)
	}

	attempt.AttemptNumber = uint32(attemptNumber)
	attempt.Version = uint64(version)

	if len(failureInformation) != 0 {
		var failure job.FailureInformation

		if err := json.Unmarshal(
			failureInformation,
			&failure,
		); err != nil {
			return nil, fmt.Errorf(
				"unmarshal failure information for attempt %q: %w",
				attempt.ID,
				err,
			)
		}

		attempt.FailureInformation = &failure
	}

	return &attempt, nil
}

func marshalFailure(
	failure *job.FailureInformation,
) ([]byte, error) {
	if failure == nil {
		return nil, nil
	}

	value, err := json.Marshal(failure)
	if err != nil {
		return nil, fmt.Errorf(
			"marshal failure information: %w",
			err,
		)
	}

	return value, nil
}

// TransitionAttemptStatus moves an execution attempt from one status to
// another, provided its current status and version match. Failure information
// may only be supplied when transitioning to FAILED.
func (s *Store) TransitionAttemptStatus(
	ctx context.Context,
	id job.AttemptID,
	from job.AttemptStatus,
	to job.AttemptStatus,
	expectedVersion uint64,
	at time.Time,
	failure *job.FailureInformation,
) (*job.ExecutionAttempt, error) {
	if err := from.ValidateTransition(to); err != nil {
		return nil, fmt.Errorf(
			"validate attempt transition: %w",
			err,
		)
	}

	if expectedVersion == 0 {
		return nil, fmt.Errorf(
			"expected attempt version must be greater than zero",
		)
	}

	if to != job.AttemptStatusFailed && failure != nil {
		return nil, fmt.Errorf(
			"failure information requires FAILED target status",
		)
	}

	failureInformation, err := marshalFailure(failure)
	if err != nil {
		return nil, err
	}

	const query = `
		UPDATE execution_attempts
		SET
			status = $1,
			version = version + 1,
			started_at = CASE
				WHEN $1 = 'RUNNING' AND started_at IS NULL THEN $2
				ELSE started_at
			END,
			completed_at = CASE
				WHEN $1 IN ('SUCCEEDED', 'FAILED', 'CANCELLED') THEN $2
				ELSE completed_at
			END,
			failure_information = CASE
				WHEN $1 = 'FAILED' THEN $3
				ELSE failure_information
			END
		WHERE
			attempt_id = $4
			AND status = $5
			AND version = $6
		RETURNING
			attempt_id,
			job_id,
			worker_id,
			attempt_number,
			status,
			version,
			started_at,
			completed_at,
			failure_information
	`

	attempt, err := scanAttempt(
		s.pool.QueryRow(
			ctx,
			query,
			to,
			at,
			failureInformation,
			id,
			from,
			expectedVersion,
		),
	)
	if errors.Is(err, persistence.ErrNotFound) {
		return nil, s.classifyAttemptTransitionMiss(ctx, id)
	}
	if err != nil {
		return nil, fmt.Errorf(
			"transition attempt %q from %s to %s: %w",
			id,
			from,
			to,
			err,
		)
	}

	return attempt, nil
}

func (s *Store) classifyAttemptTransitionMiss(
	ctx context.Context,
	id job.AttemptID,
) error {
	const query = `
		SELECT EXISTS (
			SELECT 1
			FROM execution_attempts
			WHERE attempt_id = $1
		)
	`

	var exists bool

	if err := s.pool.QueryRow(ctx, query, id).Scan(&exists); err != nil {
		return fmt.Errorf(
			"classify attempt transition for %q: %w",
			id,
			err,
		)
	}

	if !exists {
		return persistence.ErrNotFound
	}

	return persistence.ErrConflict
}
