package scheduler

import (
	"testing"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/job"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/worker"
)

func TestRequiredCapacityUsesRequests(t *testing.T) {
	cpuRequest := int64(500)
	cpuLimit := int64(2000)

	memoryRequest := int64(256 << 20)
	memoryLimit := int64(1 << 30)

	j := &job.Job{
		Spec: job.Specification{
			Resources: job.ResourceRequirements{
				CPURequestMillis:   &cpuRequest,
				CPULimitMillis:     &cpuLimit,
				MemoryRequestBytes: &memoryRequest,
				MemoryLimitBytes:   &memoryLimit,
			},
		},
	}

	required, err := requiredCapacity(j)
	if err != nil {
		t.Fatalf("required capacity: %v", err)
	}

	if required.CPUMillis != cpuRequest {
		t.Fatalf(
			"CPU: got=%d want=%d",
			required.CPUMillis,
			cpuRequest,
		)
	}

	if required.MemoryBytes != memoryRequest {
		t.Fatalf(
			"memory: got=%d want=%d",
			required.MemoryBytes,
			memoryRequest,
		)
	}
}

func TestSelectWorkerRequiresAvailableCapacity(
	t *testing.T,
) {
	required := worker.Capacity{
		CPUMillis:   1000,
		MemoryBytes: 2 << 30,
	}

	workers := []*worker.Worker{
		{
			ID:     "unavailable",
			Status: worker.StatusUnavailable,
			AvailableCapacity: worker.Capacity{
				CPUMillis:   8000,
				MemoryBytes: 32 << 30,
			},
		},
		{
			ID:     "too-small",
			Status: worker.StatusAvailable,
			AvailableCapacity: worker.Capacity{
				CPUMillis:   500,
				MemoryBytes: 8 << 30,
			},
		},
		{
			ID:     "selected",
			Status: worker.StatusAvailable,
			AvailableCapacity: worker.Capacity{
				CPUMillis:   2000,
				MemoryBytes: 4 << 30,
			},
		},
	}

	selected, ok := selectWorker(
		workers,
		required,
	)
	if !ok {
		t.Fatal("expected eligible worker")
	}

	if selected.ID != "selected" {
		t.Fatalf(
			"selected worker: got=%q want=%q",
			selected.ID,
			"selected",
		)
	}
}
