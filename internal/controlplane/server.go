// Package controlplane implements the worker-facing gRPC control plane.
package controlplane

import (
	"context"
	"errors"
	"strings"
	"time"

	workerv1 "github.com/RE110010100/Distributed-Job-Scheduler/internal/gen/worker/v1"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/persistence"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/worker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const defaultHeartbeatInterval = 5 * time.Second

// Server implements the worker control-plane gRPC service.
type Server struct {
	workerv1.UnimplementedWorkerControlServiceServer

	workers           persistence.WorkerRepository
	heartbeatInterval time.Duration
	now               func() time.Time
}

// NewServer creates a worker control-plane server.
func NewServer(
	workers persistence.WorkerRepository,
) *Server {
	return &Server{
		workers:           workers,
		heartbeatInterval: defaultHeartbeatInterval,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

// RegisterWorker registers or refreshes one worker identity.
func (s *Server) RegisterWorker(
	ctx context.Context,
	request *workerv1.RegisterWorkerRequest,
) (*workerv1.RegisterWorkerResponse, error) {
	if request == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"request is required",
		)
	}

	workerID := strings.TrimSpace(request.GetWorkerId())
	if workerID == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"worker_id is required",
		)
	}

	capacity, err := capacityFromProto(request.GetCapacity())
	if err != nil {
		return nil, status.Error(
			codes.InvalidArgument,
			err.Error(),
		)
	}

	runtimes := normalizeRuntimes(
		request.GetCapabilities().GetContainerRuntimes(),
	)

	if len(runtimes) == 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"at least one container runtime capability is required",
		)
	}

	now := s.now()

	persisted, err := s.workers.RegisterWorker(
		ctx,
		&worker.Worker{
			ID:                worker.ID(workerID),
			Status:            worker.StatusAvailable,
			Capacity:          capacity,
			AvailableCapacity: capacity,
			ContainerRuntimes: runtimes,
			RegisteredAt:      now,
			LastHeartbeatAt:   now,
		},
	)
	if err != nil {
		return nil, status.Error(
			codes.Internal,
			"failed to register worker",
		)
	}

	return &workerv1.RegisterWorkerResponse{
		RegisteredAt: timestamppb.New(
			persisted.RegisteredAt,
		),
		HeartbeatInterval: durationpb.New(
			s.heartbeatInterval,
		),
	}, nil
}

func capacityFromProto(
	value *workerv1.WorkerCapacity,
) (worker.Capacity, error) {
	if value == nil {
		return worker.Capacity{},
			errors.New("capacity is required")
	}

	capacity := worker.Capacity{
		CPUMillis:   value.GetCpuMillis(),
		MemoryBytes: value.GetMemoryBytes(),
	}

	if !capacity.Valid() {
		return worker.Capacity{},
			errors.New(
				"capacity CPU and memory must be greater than zero",
			)
	}

	return capacity, nil
}

func normalizeRuntimes(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))

	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}

		if _, exists := seen[value]; exists {
			continue
		}

		seen[value] = struct{}{}
		result = append(result, value)
	}

	return result
}

// Heartbeat records worker liveness and currently available capacity.
func (s *Server) Heartbeat(
	ctx context.Context,
	request *workerv1.HeartbeatRequest,
) (*workerv1.HeartbeatResponse, error) {
	if request == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"request is required",
		)
	}

	workerID := strings.TrimSpace(request.GetWorkerId())
	if workerID == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"worker_id is required",
		)
	}

	available := request.GetAvailableCapacity()
	if available == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"available_capacity is required",
		)
	}

	capacity := worker.Capacity{
		CPUMillis:   available.GetCpuMillis(),
		MemoryBytes: available.GetMemoryBytes(),
	}

	if capacity.CPUMillis < 0 ||
		capacity.MemoryBytes < 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"available capacity must not be negative",
		)
	}

	now := s.now()

	_, err := s.workers.HeartbeatWorker(
		ctx,
		worker.ID(workerID),
		capacity,
		now,
	)

	switch {
	case errors.Is(err, persistence.ErrNotFound):
		return nil, status.Error(
			codes.NotFound,
			"worker is not registered",
		)

	case errors.Is(err, persistence.ErrConflict):
		return nil, status.Error(
			codes.FailedPrecondition,
			"available capacity exceeds registered capacity",
		)

	case err != nil:
		return nil, status.Error(
			codes.Internal,
			"failed to record worker heartbeat",
		)
	}

	return &workerv1.HeartbeatResponse{
		AcknowledgedAt: timestamppb.New(now),
	}, nil
}
