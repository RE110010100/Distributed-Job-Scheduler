package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/job"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/persistence"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/worker"
	"github.com/jackc/pgx/v5"
)

// NextQueuedJob returns the oldest queued job.
func (s *Store) NextQueuedJob(
	ctx context.Context,
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
		WHERE
			status = 'QUEUED'
			AND cancellation_requested = FALSE
		ORDER BY created_at ASC, job_id ASC
		LIMIT 1
	`

	return scanJob(
		s.pool.QueryRow(ctx, query),
	)
}

// ListEligibleWorkers returns available workers that currently have
// sufficient advertised CPU and memory capacity.
func (s *Store) ListEligibleWorkers(
	ctx context.Context,
	required worker.Capacity,
	limit int,
) ([]*worker.Worker, error) {
	if required.CPUMillis < 0 ||
		required.MemoryBytes < 0 {
		return nil, fmt.Errorf(
			"required capacity must not be negative",
		)
	}

	if limit <= 0 {
		return nil, fmt.Errorf(
			"worker candidate limit must be greater than zero",
		)
	}

	const query = `
		SELECT
			worker_id,
			status,
			cpu_millis,
			memory_bytes,
			available_cpu_millis,
			available_memory_bytes,
			container_runtimes,
			registered_at,
			last_heartbeat_at
		FROM workers
		WHERE
			status = 'AVAILABLE'
			AND available_cpu_millis >= $1
			AND available_memory_bytes >= $2
		ORDER BY
			available_cpu_millis ASC,
			available_memory_bytes ASC,
			worker_id ASC
		LIMIT $3
	`

	rows, err := s.pool.Query(
		ctx,
		query,
		required.CPUMillis,
		required.MemoryBytes,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"list eligible workers: %w",
			err,
		)
	}
	defer rows.Close()

	workers := make(
		[]*worker.Worker,
		0,
		limit,
	)

	for rows.Next() {
		candidate, err := scanWorker(rows)
		if err != nil {
			return nil, fmt.Errorf(
				"scan eligible worker: %w",
				err,
			)
		}

		workers = append(workers, candidate)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate eligible workers: %w",
			err,
		)
	}

	return workers, nil
}

// AssignJob atomically claims a queued job, reserves worker capacity,
// creates its execution attempt, and transitions the logical job to RUNNING.
func (s *Store) AssignJob(
	ctx context.Context,
	jobID job.ID,
	workerID worker.ID,
	required worker.Capacity,
	attemptID job.AttemptID,
	at time.Time,
) (*job.ExecutionAttempt, error) {
	if jobID == "" {
		return nil, fmt.Errorf(
			"job ID must not be empty",
		)
	}

	if workerID == "" {
		return nil, fmt.Errorf(
			"worker ID must not be empty",
		)
	}

	if attemptID == "" {
		return nil, fmt.Errorf(
			"attempt ID must not be empty",
		)
	}

	if required.CPUMillis < 0 ||
		required.MemoryBytes < 0 {
		return nil, fmt.Errorf(
			"required capacity must not be negative",
		)
	}

	tx, err := s.pool.BeginTx(
		ctx,
		pgx.TxOptions{},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"begin job assignment transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// Lock the logical job first. Every scheduler replica follows this
	// lock ordering: job -> worker.
	var (
		currentStatus job.Status
		jobVersion    int64
	)

	err = tx.QueryRow(
		ctx,
		`
			SELECT status, version
			FROM jobs
			WHERE job_id = $1
			FOR UPDATE
		`,
		jobID,
	).Scan(
		&currentStatus,
		&jobVersion,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, persistence.ErrNotFound
	}

	if err != nil {
		return nil, fmt.Errorf(
			"lock job %q: %w",
			jobID,
			err,
		)
	}

	if currentStatus != job.StatusQueued {
		return nil, persistence.ErrConflict
	}

	// Lock the selected worker after the job.
	var (
		workerStatus    worker.Status
		availableCPU    int64
		availableMemory int64
	)

	err = tx.QueryRow(
		ctx,
		`
			SELECT
				status,
				available_cpu_millis,
				available_memory_bytes
			FROM workers
			WHERE worker_id = $1
			FOR UPDATE
		`,
		workerID,
	).Scan(
		&workerStatus,
		&availableCPU,
		&availableMemory,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, persistence.ErrNotFound
	}

	if err != nil {
		return nil, fmt.Errorf(
			"lock worker %q: %w",
			workerID,
			err,
		)
	}

	if workerStatus != worker.StatusAvailable {
		return nil, persistence.ErrConflict
	}

	if availableCPU < required.CPUMillis ||
		availableMemory < required.MemoryBytes {
		return nil, persistence.ErrConflict
	}

	// Determine the next physical attempt number while the job row is
	// locked, preventing concurrent assignments for the same job.
	var attemptNumber uint32

	err = tx.QueryRow(
		ctx,
		`
			SELECT COALESCE(MAX(attempt_number), 0) + 1
			FROM execution_attempts
			WHERE job_id = $1
		`,
		jobID,
	).Scan(&attemptNumber)
	if err != nil {
		return nil, fmt.Errorf(
			"determine next attempt number for job %q: %w",
			jobID,
			err,
		)
	}

	_, err = tx.Exec(
		ctx,
		`
			UPDATE workers
			SET
				available_cpu_millis =
					available_cpu_millis - $2,
				available_memory_bytes =
					available_memory_bytes - $3
			WHERE worker_id = $1
		`,
		workerID,
		required.CPUMillis,
		required.MemoryBytes,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"reserve worker %q capacity: %w",
			workerID,
			err,
		)
	}

	_, err = tx.Exec(
		ctx,
		`
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
			VALUES (
				$1,
				$2,
				$3,
				$4,
				'ASSIGNED',
				1,
				NULL,
				NULL,
				NULL
			)
		`,
		attemptID,
		jobID,
		workerID,
		attemptNumber,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"create execution attempt: %w",
			err,
		)
	}

	result, err := tx.Exec(
		ctx,
		`
			UPDATE jobs
			SET
				status = 'RUNNING',
				version = version + 1,
				started_at = COALESCE(
					started_at,
					$2
				)
			WHERE
				job_id = $1
				AND status = 'QUEUED'
		`,
		jobID,
		at,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"transition assigned job to running: %w",
			err,
		)
	}

	if result.RowsAffected() != 1 {
		return nil, persistence.ErrConflict
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf(
			"commit job assignment: %w",
			err,
		)
	}

	return &job.ExecutionAttempt{
		ID:            attemptID,
		JobID:         jobID,
		WorkerID:      string(workerID),
		AttemptNumber: attemptNumber,
		Status:        job.AttemptStatusAssigned,
		Version:       1,
	}, nil
}
