package scheduler

import (
	"fmt"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/job"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/worker"
)

func requiredCapacity(
	j *job.Job,
) (worker.Capacity, error) {
	if j == nil {
		return worker.Capacity{},
			fmt.Errorf("job must not be nil")
	}

	var required worker.Capacity

	if j.Spec.Resources.CPURequestMillis != nil {
		required.CPUMillis =
			*j.Spec.Resources.CPURequestMillis
	}

	if j.Spec.Resources.MemoryRequestBytes != nil {
		required.MemoryBytes =
			*j.Spec.Resources.MemoryRequestBytes
	}

	if required.CPUMillis < 0 {
		return worker.Capacity{},
			fmt.Errorf(
				"CPU request must not be negative",
			)
	}

	if required.MemoryBytes < 0 {
		return worker.Capacity{},
			fmt.Errorf(
				"memory request must not be negative",
			)
	}

	return required, nil
}

func selectWorker(
	workers []*worker.Worker,
	required worker.Capacity,
) (*worker.Worker, bool) {
	for _, candidate := range workers {
		if candidate == nil {
			continue
		}

		if candidate.Status != worker.StatusAvailable {
			continue
		}

		if candidate.AvailableCapacity.Fits(required) {
			return candidate, true
		}
	}

	return nil, false
}
