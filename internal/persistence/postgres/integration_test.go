package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/job"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/persistence"
)

func integrationStore(t *testing.T) *Store {
	t.Helper()

	databaseURL := os.Getenv("DJS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DJS_TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	store, err := Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open postgres store: %v", err)
	}

	if err := store.Migrate(ctx); err != nil {
		store.Close()
		t.Fatalf("migrate postgres store: %v", err)
	}

	resetDatabase(t, store)

	// The database is reset at the start of each test, so cleanup only needs to
	// close the store. Some tests close it themselves to simulate a restart.
	t.Cleanup(store.Close)

	return store
}

func resetDatabase(t *testing.T, store *Store) {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	const query = `
		TRUNCATE TABLE
			execution_attempts,
			jobs
		RESTART IDENTITY
		CASCADE
	`

	if _, err := store.pool.Exec(ctx, query); err != nil {
		t.Fatalf("reset postgres test database: %v", err)
	}
}

func testJob(id job.ID) *job.Job {
	return &job.Job{
		ID:      id,
		OwnerID: "integration-test-owner",
		Spec: job.Specification{
			ContainerImage: "example.com/test/image:latest",
		},
		Status:    job.StatusQueued,
		CreatedAt: time.Now().UTC(),
	}
}

func TestJobSurvivesStoreRestart(t *testing.T) {
	store := integrationStore(t)

	ctx := context.Background()

	original := testJob("job-restart")

	if err := store.CreateJob(ctx, original); err != nil {
		t.Fatalf("create job: %v", err)
	}

	databaseURL := os.Getenv("DJS_TEST_DATABASE_URL")

	store.Close()

	reopened, err := Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("reopen postgres store: %v", err)
	}
	defer reopened.Close()

	persisted, err := reopened.GetJob(ctx, original.ID)
	if err != nil {
		t.Fatalf("get job after restart: %v", err)
	}

	if persisted.ID != original.ID {
		t.Fatalf(
			"persisted job ID = %q, want %q",
			persisted.ID,
			original.ID,
		)
	}

	if persisted.Status != job.StatusQueued {
		t.Fatalf(
			"persisted status = %q, want %q",
			persisted.Status,
			job.StatusQueued,
		)
	}

	if persisted.Spec.ContainerImage != original.Spec.ContainerImage {
		t.Fatalf(
			"persisted image = %q, want %q",
			persisted.Spec.ContainerImage,
			original.Spec.ContainerImage,
		)
	}

	if persisted.Version != 1 {
		t.Fatalf(
			"persisted version = %d, want 1",
			persisted.Version,
		)
	}
}

func TestAttemptSurvivesStoreRestart(t *testing.T) {
	store := integrationStore(t)

	ctx := context.Background()

	j := testJob("job-attempt-restart")

	if err := store.CreateJob(ctx, j); err != nil {
		t.Fatalf("create job: %v", err)
	}

	attempt := &job.ExecutionAttempt{
		ID:            "attempt-restart",
		JobID:         j.ID,
		WorkerID:      "worker-1",
		AttemptNumber: 1,
		Status:        job.AttemptStatusAssigned,
	}

	if err := store.CreateAttempt(ctx, attempt); err != nil {
		t.Fatalf("create attempt: %v", err)
	}

	databaseURL := os.Getenv("DJS_TEST_DATABASE_URL")

	store.Close()

	reopened, err := Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("reopen postgres store: %v", err)
	}
	defer reopened.Close()

	persisted, err := reopened.GetAttempt(ctx, attempt.ID)
	if err != nil {
		t.Fatalf("get attempt after restart: %v", err)
	}

	if persisted.ID != attempt.ID {
		t.Fatalf(
			"persisted attempt ID = %q, want %q",
			persisted.ID,
			attempt.ID,
		)
	}

	if persisted.JobID != j.ID {
		t.Fatalf(
			"persisted job ID = %q, want %q",
			persisted.JobID,
			j.ID,
		)
	}

	if persisted.Status != job.AttemptStatusAssigned {
		t.Fatalf(
			"persisted status = %q, want %q",
			persisted.Status,
			job.AttemptStatusAssigned,
		)
	}
}

func TestConcurrentJobTransitionAllowsOneWinner(t *testing.T) {
	store := integrationStore(t)

	ctx := context.Background()

	j := testJob("job-transition-race")

	if err := store.CreateJob(ctx, j); err != nil {
		t.Fatalf("create job: %v", err)
	}

	const contenders = 16

	start := make(chan struct{})

	results := make(chan error, contenders)

	var wg sync.WaitGroup
	wg.Add(contenders)

	for range contenders {
		go func() {
			defer wg.Done()

			<-start

			_, err := store.TransitionJobStatus(
				ctx,
				j.ID,
				job.StatusQueued,
				job.StatusRunning,
				1,
				time.Now().UTC(),
			)

			results <- err
		}()
	}

	close(start)

	wg.Wait()
	close(results)

	var (
		successes int
		conflicts int
	)

	for err := range results {
		switch {
		case err == nil:
			successes++

		case errors.Is(err, persistence.ErrConflict):
			conflicts++

		default:
			t.Fatalf(
				"unexpected transition error: %v",
				err,
			)
		}
	}

	if successes != 1 {
		t.Fatalf(
			"successful transitions = %d, want 1",
			successes,
		)
	}

	if conflicts != contenders-1 {
		t.Fatalf(
			"conflicting transitions = %d, want %d",
			conflicts,
			contenders-1,
		)
	}

	persisted, err := store.GetJob(ctx, j.ID)
	if err != nil {
		t.Fatalf("get transitioned job: %v", err)
	}

	if persisted.Status != job.StatusRunning {
		t.Fatalf(
			"persisted status = %q, want %q",
			persisted.Status,
			job.StatusRunning,
		)
	}

	if persisted.Version != 2 {
		t.Fatalf(
			"persisted version = %d, want 2",
			persisted.Version,
		)
	}
}

func TestConcurrentAttemptTransitionAllowsOneWinner(t *testing.T) {
	store := integrationStore(t)

	ctx := context.Background()

	j := testJob("job-attempt-transition")

	if err := store.CreateJob(ctx, j); err != nil {
		t.Fatalf("create job: %v", err)
	}

	attempt := &job.ExecutionAttempt{
		ID:            "attempt-transition-race",
		JobID:         j.ID,
		WorkerID:      "worker-1",
		AttemptNumber: 1,
		Status:        job.AttemptStatusAssigned,
	}

	if err := store.CreateAttempt(ctx, attempt); err != nil {
		t.Fatalf("create attempt: %v", err)
	}

	const contenders = 16

	start := make(chan struct{})
	results := make(chan error, contenders)

	var wg sync.WaitGroup
	wg.Add(contenders)

	for range contenders {
		go func() {
			defer wg.Done()

			<-start

			_, err := store.TransitionAttemptStatus(
				ctx,
				attempt.ID,
				job.AttemptStatusAssigned,
				job.AttemptStatusRunning,
				1,
				time.Now().UTC(),
				nil,
			)

			results <- err
		}()
	}

	close(start)

	wg.Wait()
	close(results)

	var (
		successes int
		conflicts int
	)

	for err := range results {
		switch {
		case err == nil:
			successes++

		case errors.Is(err, persistence.ErrConflict):
			conflicts++

		default:
			t.Fatalf(
				"unexpected transition error: %v",
				err,
			)
		}
	}

	if successes != 1 {
		t.Fatalf(
			"successful transitions = %d, want 1",
			successes,
		)
	}

	if conflicts != contenders-1 {
		t.Fatalf(
			"conflicting transitions = %d, want %d",
			conflicts,
			contenders-1,
		)
	}
}

func TestConcurrentIdempotentSubmissionCreatesOneJob(t *testing.T) {
	store := integrationStore(t)

	ctx := context.Background()

	const contenders = 16

	start := make(chan struct{})

	type result struct {
		job     *job.Job
		created bool
		err     error
	}

	results := make(chan result, contenders)

	var wg sync.WaitGroup
	wg.Add(contenders)

	for i := range contenders {
		go func() {
			defer wg.Done()

			<-start

			candidate := testJob(
				job.ID(fmt.Sprintf("job-idempotent-%d", i)),
			)
			candidate.IdempotencyKey = "request-123"

			persisted, created, err :=
				store.CreateJobIdempotent(ctx, candidate)

			results <- result{
				job:     persisted,
				created: created,
				err:     err,
			}
		}()
	}

	close(start)

	wg.Wait()
	close(results)

	var (
		createdCount int
		resolvedID   job.ID
	)

	for result := range results {
		if result.err != nil {
			t.Fatalf(
				"idempotent submission failed: %v",
				result.err,
			)
		}

		if result.created {
			createdCount++
		}

		if resolvedID == "" {
			resolvedID = result.job.ID
			continue
		}

		if result.job.ID != resolvedID {
			t.Fatalf(
				"submission resolved to job %q, want %q",
				result.job.ID,
				resolvedID,
			)
		}
	}

	if createdCount != 1 {
		t.Fatalf(
			"created jobs = %d, want 1",
			createdCount,
		)
	}
}
