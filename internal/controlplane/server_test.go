package controlplane

import (
	"context"
	"testing"
	"time"

	workerv1 "github.com/RE110010100/Distributed-Job-Scheduler/internal/gen/worker/v1"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/persistence"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/worker"
)

type fakeWorkerRepository struct {
	worker *worker.Worker
}

func (f *fakeWorkerRepository) RegisterWorker(
	_ context.Context,
	w *worker.Worker,
) (*worker.Worker, error) {
	clone := *w
	f.worker = &clone
	return &clone, nil
}

func (f *fakeWorkerRepository) GetWorker(
	_ context.Context,
	id worker.ID,
) (*worker.Worker, error) {
	if f.worker == nil || f.worker.ID != id {
		return nil, persistence.ErrNotFound
	}

	clone := *f.worker
	return &clone, nil
}

func (f *fakeWorkerRepository) HeartbeatWorker(
	_ context.Context,
	id worker.ID,
	available worker.Capacity,
	at time.Time,
) (*worker.Worker, error) {
	if f.worker == nil || f.worker.ID != id {
		return nil, persistence.ErrNotFound
	}

	f.worker.AvailableCapacity = available
	f.worker.LastHeartbeatAt = at

	clone := *f.worker
	return &clone, nil
}

func TestRegisterWorker(t *testing.T) {
	repository := &fakeWorkerRepository{}
	server := NewServer(repository)

	now := time.Date(
		2026,
		time.September,
		26,
		12,
		0,
		0,
		0,
		time.UTC,
	)
	server.now = func() time.Time {
		return now
	}

	response, err := server.RegisterWorker(
		context.Background(),
		&workerv1.RegisterWorkerRequest{
			WorkerId: "worker-1",
			Capacity: &workerv1.WorkerCapacity{
				CpuMillis:   4000,
				MemoryBytes: 8 << 30,
			},
			Capabilities: &workerv1.WorkerCapabilities{
				ContainerRuntimes: []string{"docker"},
			},
		},
	)
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	if repository.worker == nil {
		t.Fatal("worker was not persisted")
	}

	if repository.worker.ID != "worker-1" {
		t.Fatalf(
			"unexpected worker ID: %q",
			repository.worker.ID,
		)
	}

	if repository.worker.AvailableCapacity.CPUMillis != 4000 {
		t.Fatalf(
			"unexpected available CPU: %d",
			repository.worker.AvailableCapacity.CPUMillis,
		)
	}

	if !response.GetRegisteredAt().AsTime().Equal(now) {
		t.Fatalf(
			"unexpected registration time: %s",
			response.GetRegisteredAt().AsTime(),
		)
	}
}

func TestHeartbeatUpdatesLivenessAndCapacity(t *testing.T) {
	repository := &fakeWorkerRepository{
		worker: &worker.Worker{
			ID: "worker-1",
			Capacity: worker.Capacity{
				CPUMillis:   4000,
				MemoryBytes: 8 << 30,
			},
			AvailableCapacity: worker.Capacity{
				CPUMillis:   4000,
				MemoryBytes: 8 << 30,
			},
		},
	}

	server := NewServer(repository)

	now := time.Date(
		2026,
		time.September,
		26,
		12,
		1,
		0,
		0,
		time.UTC,
	)

	server.now = func() time.Time {
		return now
	}

	response, err := server.Heartbeat(
		context.Background(),
		&workerv1.HeartbeatRequest{
			WorkerId: "worker-1",
			AvailableCapacity: &workerv1.WorkerCapacity{
				CpuMillis:   2500,
				MemoryBytes: 6 << 30,
			},
		},
	)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	if repository.worker.AvailableCapacity.CPUMillis != 2500 {
		t.Fatalf(
			"unexpected available CPU: %d",
			repository.worker.AvailableCapacity.CPUMillis,
		)
	}

	if !repository.worker.LastHeartbeatAt.Equal(now) {
		t.Fatalf(
			"unexpected heartbeat time: %s",
			repository.worker.LastHeartbeatAt,
		)
	}

	if !response.GetAcknowledgedAt().AsTime().Equal(now) {
		t.Fatalf(
			"unexpected acknowledgement time: %s",
			response.GetAcknowledgedAt().AsTime(),
		)
	}
}
