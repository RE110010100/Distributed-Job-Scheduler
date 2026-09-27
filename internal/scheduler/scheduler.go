// Package scheduler implements durable job placement.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/job"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/persistence"
)

const (
	defaultPollInterval  = time.Second
	workerCandidateLimit = 32
)

// Scheduler continuously places queued jobs onto eligible workers.
type Scheduler struct {
	repository   persistence.SchedulingRepository
	pollInterval time.Duration
	logger       *log.Logger
	now          func() time.Time
	newAttemptID func() (job.AttemptID, error)
}

// New creates a scheduler.
func New(
	repository persistence.SchedulingRepository,
	logger *log.Logger,
) *Scheduler {
	return &Scheduler{
		repository:   repository,
		pollInterval: defaultPollInterval,
		logger:       logger,
		now: func() time.Time {
			return time.Now().UTC()
		},
		newAttemptID: newAttemptID,
	}
}

// Run schedules jobs until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	if s.repository == nil {
		return errors.New("scheduler repository is required")
	}

	if s.logger == nil {
		return errors.New("scheduler logger is required")
	}

	if s.pollInterval <= 0 {
		return errors.New(
			"scheduler poll interval must be greater than zero",
		)
	}

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		if err := s.scheduleAvailableWork(ctx); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
		}
	}
}

func (s *Scheduler) scheduleAvailableWork(
	ctx context.Context,
) error {
	for {
		scheduled, err := s.scheduleOne(ctx)
		if err != nil {
			return err
		}

		if !scheduled {
			return nil
		}
	}
}

func (s *Scheduler) scheduleOne(
	ctx context.Context,
) (bool, error) {
	queuedJob, err := s.repository.NextQueuedJob(ctx)
	switch {
	case errors.Is(err, persistence.ErrNotFound):
		return false, nil

	case err != nil:
		return false, fmt.Errorf(
			"load next queued job: %w",
			err,
		)
	}

	required, err := requiredCapacity(queuedJob)
	if err != nil {
		return false, fmt.Errorf(
			"determine capacity for job %q: %w",
			queuedJob.ID,
			err,
		)
	}

	workers, err := s.repository.ListEligibleWorkers(
		ctx,
		required,
		workerCandidateLimit,
	)
	if err != nil {
		return false, fmt.Errorf(
			"list eligible workers for job %q: %w",
			queuedJob.ID,
			err,
		)
	}

	selected, ok := selectWorker(workers, required)
	if !ok {
		// No worker can currently run the oldest queued job.
		//
		// Return rather than busy-spin. A later scheduler iteration
		// retries after capacity/liveness changes.
		return false, nil
	}

	attemptID, err := s.newAttemptID()
	if err != nil {
		return false, fmt.Errorf(
			"generate attempt ID: %w",
			err,
		)
	}

	attempt, err := s.repository.AssignJob(
		ctx,
		queuedJob.ID,
		selected.ID,
		required,
		attemptID,
		s.now(),
	)
	switch {
	case errors.Is(err, persistence.ErrConflict):
		// Another scheduler replica won the race, or worker capacity
		// changed between selection and assignment. This is normal.
		return true, nil

	case errors.Is(err, persistence.ErrNotFound):
		// The selected job/worker disappeared between discovery and
		// assignment. Retry discovery.
		return true, nil

	case err != nil:
		return false, fmt.Errorf(
			"assign job %q to worker %q: %w",
			queuedJob.ID,
			selected.ID,
			err,
		)
	}

	s.logger.Printf(
		"job_assigned job_id=%s attempt_id=%s worker_id=%s attempt_number=%d",
		attempt.JobID,
		attempt.ID,
		attempt.WorkerID,
		attempt.AttemptNumber,
	)

	return true, nil
}
