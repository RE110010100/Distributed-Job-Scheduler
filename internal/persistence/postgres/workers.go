package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/persistence"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/worker"
)

// RegisterWorker inserts a worker, or updates it if one with the same ID exists.
func (s *Store) RegisterWorker(
	ctx context.Context,
	w *worker.Worker,
) (*worker.Worker, error) {
	if w == nil {
		return nil, errors.New("worker must not be nil")
	}

	if w.ID == "" {
		return nil, errors.New("worker ID must not be empty")
	}

	if !w.Capacity.Valid() {
		return nil, errors.New(
			"worker capacity must contain positive CPU and memory",
		)
	}

	const query = `
		INSERT INTO workers (
			worker_id,
			cpu_millis,
			memory_bytes,
			available_cpu_millis,
			available_memory_bytes,
			container_runtimes,
			registered_at,
			last_heartbeat_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
		ON CONFLICT (worker_id)
		DO UPDATE SET
			cpu_millis = EXCLUDED.cpu_millis,
			memory_bytes = EXCLUDED.memory_bytes,
			available_cpu_millis =
				EXCLUDED.available_cpu_millis,
			available_memory_bytes =
				EXCLUDED.available_memory_bytes,
			container_runtimes =
				EXCLUDED.container_runtimes,
			registered_at = EXCLUDED.registered_at,
			last_heartbeat_at =
				EXCLUDED.last_heartbeat_at
		RETURNING
			worker_id,
			cpu_millis,
			memory_bytes,
			available_cpu_millis,
			available_memory_bytes,
			container_runtimes,
			registered_at,
			last_heartbeat_at
	`

	return scanWorker(
		s.pool.QueryRow(
			ctx,
			query,
			w.ID,
			w.Capacity.CPUMillis,
			w.Capacity.MemoryBytes,
			w.AvailableCapacity.CPUMillis,
			w.AvailableCapacity.MemoryBytes,
			w.ContainerRuntimes,
			w.RegisteredAt,
		),
	)
}

// GetWorker returns the worker with the given ID, or persistence.ErrNotFound.
func (s *Store) GetWorker(
	ctx context.Context,
	id worker.ID,
) (*worker.Worker, error) {
	const query = `
		SELECT
			worker_id,
			cpu_millis,
			memory_bytes,
			available_cpu_millis,
			available_memory_bytes,
			container_runtimes,
			registered_at,
			last_heartbeat_at
		FROM workers
		WHERE worker_id = $1
	`

	return scanWorker(
		s.pool.QueryRow(ctx, query, id),
	)
}

type workerScanner interface {
	Scan(dest ...any) error
}

func scanWorker(
	row workerScanner,
) (*worker.Worker, error) {
	var w worker.Worker

	err := row.Scan(
		&w.ID,
		&w.Capacity.CPUMillis,
		&w.Capacity.MemoryBytes,
		&w.AvailableCapacity.CPUMillis,
		&w.AvailableCapacity.MemoryBytes,
		&w.ContainerRuntimes,
		&w.RegisteredAt,
		&w.LastHeartbeatAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, persistence.ErrNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("scan worker: %w", err)
	}

	return &w, nil
}

// HeartbeatWorker records a heartbeat and the available capacity for a worker.
func (s *Store) HeartbeatWorker(
	ctx context.Context,
	id worker.ID,
	available worker.Capacity,
	at time.Time,
) (*worker.Worker, error) {
	if id == "" {
		return nil, errors.New("worker ID must not be empty")
	}

	if available.CPUMillis < 0 ||
		available.MemoryBytes < 0 {
		return nil, errors.New(
			"available worker capacity must not be negative",
		)
	}

	const query = `
		UPDATE workers
		SET
			available_cpu_millis = $2,
			available_memory_bytes = $3,
			last_heartbeat_at = $4
		WHERE
			worker_id = $1
			AND $2 <= cpu_millis
			AND $3 <= memory_bytes
		RETURNING
			worker_id,
			cpu_millis,
			memory_bytes,
			available_cpu_millis,
			available_memory_bytes,
			container_runtimes,
			registered_at,
			last_heartbeat_at
	`

	w, err := scanWorker(
		s.pool.QueryRow(
			ctx,
			query,
			id,
			available.CPUMillis,
			available.MemoryBytes,
			at,
		),
	)
	if !errors.Is(err, persistence.ErrNotFound) {
		return w, err
	}

	var exists bool

	err = s.pool.QueryRow(
		ctx,
		`SELECT EXISTS(
			SELECT 1
			FROM workers
			WHERE worker_id = $1
		)`,
		id,
	).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf(
			"classify worker heartbeat: %w",
			err,
		)
	}

	if !exists {
		return nil, persistence.ErrNotFound
	}

	return nil, persistence.ErrConflict
}
