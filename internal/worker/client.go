package worker

import (
	"context"
	"fmt"
	"time"

	workerv1 "github.com/RE110010100/Distributed-Job-Scheduler/internal/gen/worker/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ClientConfig configures one worker's control-plane connection.
type ClientConfig struct {
	ID                ID
	SchedulerAddress  string
	Capacity          Capacity
	ContainerRuntimes []string
}

// Run registers the worker and sends heartbeats until ctx is cancelled.
func Run(
	ctx context.Context,
	cfg ClientConfig,
) error {
	if cfg.ID == "" {
		return fmt.Errorf("worker ID must not be empty")
	}

	if cfg.SchedulerAddress == "" {
		return fmt.Errorf(
			"scheduler address must not be empty",
		)
	}

	if !cfg.Capacity.Valid() {
		return fmt.Errorf(
			"worker capacity must contain positive CPU and memory",
		)
	}

	connection, err := grpc.NewClient(
		cfg.SchedulerAddress,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		return fmt.Errorf(
			"create scheduler gRPC client: %w",
			err,
		)
	}
	defer func() { _ = connection.Close() }()

	client := workerv1.NewWorkerControlServiceClient(
		connection,
	)

	registerCtx, cancel := context.WithTimeout(
		ctx,
		5*time.Second,
	)

	response, err := client.RegisterWorker(
		registerCtx,
		&workerv1.RegisterWorkerRequest{
			WorkerId: string(cfg.ID),
			Capacity: capacityToProto(cfg.Capacity),
			Capabilities: &workerv1.WorkerCapabilities{
				ContainerRuntimes: cfg.ContainerRuntimes,
			},
		},
	)
	cancel()

	if err != nil {
		return fmt.Errorf("register worker: %w", err)
	}

	heartbeatInterval := response.
		GetHeartbeatInterval().
		AsDuration()

	if heartbeatInterval <= 0 {
		return fmt.Errorf(
			"scheduler returned invalid heartbeat interval",
		)
	}

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
			heartbeatCtx, cancel := context.WithTimeout(
				ctx,
				heartbeatInterval,
			)

			_, err := client.Heartbeat(
				heartbeatCtx,
				&workerv1.HeartbeatRequest{
					WorkerId: string(cfg.ID),

					// No assignments exist yet, so all
					// registered capacity remains available.
					AvailableCapacity: capacityToProto(
						cfg.Capacity,
					),
				},
			)

			cancel()

			if err != nil {
				return fmt.Errorf(
					"send worker heartbeat: %w",
					err,
				)
			}
		}
	}
}

func capacityToProto(
	capacity Capacity,
) *workerv1.WorkerCapacity {
	return &workerv1.WorkerCapacity{
		CpuMillis:   capacity.CPUMillis,
		MemoryBytes: capacity.MemoryBytes,
	}
}
