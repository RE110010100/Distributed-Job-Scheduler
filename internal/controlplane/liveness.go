package controlplane

import (
	"context"
	"fmt"
	"time"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/persistence"
)

// LivenessMonitor marks workers unavailable after their heartbeat expires.
type LivenessMonitor struct {
	workers       persistence.WorkerRepository
	timeout       time.Duration
	checkInterval time.Duration
	now           func() time.Time
}

// NewLivenessMonitor creates a worker liveness monitor.
func NewLivenessMonitor(
	workers persistence.WorkerRepository,
	timeout time.Duration,
	checkInterval time.Duration,
) (*LivenessMonitor, error) {
	if timeout <= 0 {
		return nil, fmt.Errorf(
			"heartbeat timeout must be greater than zero",
		)
	}

	if checkInterval <= 0 {
		return nil, fmt.Errorf(
			"liveness check interval must be greater than zero",
		)
	}

	return &LivenessMonitor{
		workers:       workers,
		timeout:       timeout,
		checkInterval: checkInterval,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}, nil
}

// Reconcile marks workers with expired heartbeats unavailable.
func (m *LivenessMonitor) Reconcile(
	ctx context.Context,
) (int64, error) {
	cutoff := m.now().Add(-m.timeout)

	return m.workers.MarkWorkersUnavailable(
		ctx,
		cutoff,
	)
}

// Run reconciles worker availability until ctx is cancelled.
func (m *LivenessMonitor) Run(
	ctx context.Context,
) error {
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
			if _, err := m.Reconcile(ctx); err != nil {
				return fmt.Errorf(
					"reconcile worker liveness: %w",
					err,
				)
			}
		}
	}
}
