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

// CreateJob inserts a new job and sets its version to 1.
func (s *Store) CreateJob(
	ctx context.Context,
	j *job.Job,
) error {
	specification, err := json.Marshal(j.Spec)
	if err != nil {
		return fmt.Errorf("marshal job specification: %w", err)
	}

	const query = `
		INSERT INTO jobs (
			job_id,
			owner_id,
			idempotency_key,
			specification,
			status,
			version,
			created_at,
			started_at,
			completed_at,
			cancellation_requested
		)
		VALUES ($1, $2, NULLIF($3, ''), $4, $5, 1, $6, $7, $8, $9)
	`

	_, err = s.pool.Exec(
		ctx,
		query,
		j.ID,
		j.OwnerID,
		j.IdempotencyKey,
		specification,
		j.Status,
		j.CreatedAt,
		j.StartedAt,
		j.CompletedAt,
		j.CancellationRequested,
	)
	if err != nil {
		return fmt.Errorf("insert job %q: %w", j.ID, err)
	}

	j.Version = 1

	return nil
}

// GetJob returns the job with the given ID, or persistence.ErrNotFound.
func (s *Store) GetJob(
	ctx context.Context,
	id job.ID,
) (*job.Job, error) {
	const query = `
		SELECT
			job_id,
			owner_id,
			COALESCE(idempotency_key, ''),
			specification,
			status,
			version,
			created_at,
			started_at,
			completed_at,
			cancellation_requested
		FROM jobs
		WHERE job_id = $1
	`

	return scanJob(s.pool.QueryRow(ctx, query, id))
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(row rowScanner) (*job.Job, error) {
	var (
		j             job.Job
		specification []byte
		version       int64
	)

	err := row.Scan(
		&j.ID,
		&j.OwnerID,
		&j.IdempotencyKey,
		&specification,
		&j.Status,
		&version,
		&j.CreatedAt,
		&j.StartedAt,
		&j.CompletedAt,
		&j.CancellationRequested,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, persistence.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan job: %w", err)
	}

	if version <= 0 {
		return nil, fmt.Errorf("invalid persisted job version %d", version)
	}

	j.Version = uint64(version)

	if err := json.Unmarshal(specification, &j.Spec); err != nil {
		return nil, fmt.Errorf(
			"unmarshal specification for job %q: %w",
			j.ID,
			err,
		)
	}

	return &j, nil
}

// TransitionJobStatus moves a job from one status to another, provided its
// current status and version match. It returns persistence.ErrNotFound if the
// job does not exist and persistence.ErrConflict if it has changed.
func (s *Store) TransitionJobStatus(
	ctx context.Context,
	id job.ID,
	from job.Status,
	to job.Status,
	expectedVersion uint64,
	at time.Time,
) (*job.Job, error) {
	if err := from.ValidateTransition(to); err != nil {
		return nil, fmt.Errorf("validate job transition: %w", err)
	}

	if expectedVersion == 0 {
		return nil, fmt.Errorf(
			"expected job version must be greater than zero",
		)
	}

	const query = `
		UPDATE jobs
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
			END
		WHERE
			job_id = $3
			AND status = $4
			AND version = $5
		RETURNING
			job_id,
			owner_id,
			COALESCE(idempotency_key, ''),
			specification,
			status,
			version,
			created_at,
			started_at,
			completed_at,
			cancellation_requested
	`

	j, err := scanJob(
		s.pool.QueryRow(
			ctx,
			query,
			to,
			at,
			id,
			from,
			expectedVersion,
		),
	)
	if errors.Is(err, persistence.ErrNotFound) {
		return nil, s.classifyJobTransitionMiss(ctx, id)
	}
	if err != nil {
		return nil, fmt.Errorf(
			"transition job %q from %s to %s: %w",
			id,
			from,
			to,
			err,
		)
	}

	return j, nil
}

func (s *Store) classifyJobTransitionMiss(
	ctx context.Context,
	id job.ID,
) error {
	const query = `
		SELECT EXISTS (
			SELECT 1
			FROM jobs
			WHERE job_id = $1
		)
	`

	var exists bool

	if err := s.pool.QueryRow(ctx, query, id).Scan(&exists); err != nil {
		return fmt.Errorf(
			"classify job transition for %q: %w",
			id,
			err,
		)
	}

	if !exists {
		return persistence.ErrNotFound
	}

	return persistence.ErrConflict
}

// CreateJobIdempotent inserts j unless the owner already has a job with the
// same idempotency key, in which case the existing job is returned. The
// boolean result reports whether a new job was created. Jobs without an
// idempotency key are always created.
func (s *Store) CreateJobIdempotent(
	ctx context.Context,
	j *job.Job,
) (*job.Job, bool, error) {
	if j.IdempotencyKey == "" {
		if err := s.CreateJob(ctx, j); err != nil {
			return nil, false, err
		}

		return j, true, nil
	}

	specification, err := json.Marshal(j.Spec)
	if err != nil {
		return nil, false, fmt.Errorf(
			"marshal job specification: %w",
			err,
		)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf(
			"begin idempotent job transaction: %w",
			err,
		)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	const insert = `
		INSERT INTO jobs (
			job_id,
			owner_id,
			idempotency_key,
			specification,
			status,
			version,
			created_at,
			started_at,
			completed_at,
			cancellation_requested
		)
		VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8, $9)
		ON CONFLICT (owner_id, idempotency_key)
			WHERE idempotency_key IS NOT NULL
		DO NOTHING
		RETURNING
			job_id
	`

	var insertedID job.ID

	err = tx.QueryRow(
		ctx,
		insert,
		j.ID,
		j.OwnerID,
		j.IdempotencyKey,
		specification,
		j.Status,
		j.CreatedAt,
		j.StartedAt,
		j.CompletedAt,
		j.CancellationRequested,
	).Scan(&insertedID)

	created := err == nil

	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf(
			"insert idempotent job: %w",
			err,
		)
	}

	const selectJob = `
		SELECT
			job_id,
			owner_id,
			COALESCE(idempotency_key, ''),
			specification,
			status,
			version,
			created_at,
			started_at,
			completed_at,
			cancellation_requested
		FROM jobs
		WHERE
			owner_id = $1
			AND idempotency_key = $2
	`

	result, err := scanJob(
		tx.QueryRow(
			ctx,
			selectJob,
			j.OwnerID,
			j.IdempotencyKey,
		),
	)
	if err != nil {
		return nil, false, fmt.Errorf(
			"load idempotent job: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf(
			"commit idempotent job transaction: %w",
			err,
		)
	}

	return result, created, nil
}

// ListJobs returns up to limit jobs, newest first.
func (s *Store) ListJobs(
	ctx context.Context,
	limit int,
) ([]*job.Job, error) {
	if limit <= 0 {
		return nil, fmt.Errorf(
			"job list limit must be greater than zero",
		)
	}

	const query = `
		SELECT
			job_id,
			owner_id,
			COALESCE(idempotency_key, ''),
			specification,
			status,
			version,
			created_at,
			started_at,
			completed_at,
			cancellation_requested
		FROM jobs
		ORDER BY created_at DESC, job_id DESC
		LIMIT $1
	`

	rows, err := s.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]*job.Job, 0)

	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf(
				"scan listed job: %w",
				err,
			)
		}

		jobs = append(jobs, j)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate listed jobs: %w",
			err,
		)
	}

	return jobs, nil
}
