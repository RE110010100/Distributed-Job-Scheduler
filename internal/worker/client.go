package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/executor"
	workerv1 "github.com/RE110010100/Distributed-Job-Scheduler/internal/gen/worker/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ClientConfig configures one worker's control-plane connection.
type ClientConfig struct {
	ID                  ID
	SchedulerAddress    string
	Capacity            Capacity
	ContainerRuntimes   []string
	AuthenticationToken string

	Executor executor.Executor
}

const (
	workerRPCTimeout   = 3 * time.Second
	jobAcquireInterval = time.Second
)

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

	if cfg.Executor == nil {
		return fmt.Errorf(
			"worker executor must not be nil",
		)
	}

	credentials, err := newTokenCredentials(
		cfg.AuthenticationToken,
	)
	if err != nil {
		return err
	}

	connection, err := grpc.NewClient(
		cfg.SchedulerAddress,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
		grpc.WithPerRPCCredentials(credentials),
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
		workerRPCTimeout,
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

	acquireTicker := time.NewTicker(
		jobAcquireInterval,
	)
	defer acquireTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
			heartbeatCtx, cancel := context.WithTimeout(
				ctx,
				workerRPCTimeout,
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

		case <-acquireTicker.C:
			acquireCtx, cancel := context.WithTimeout(
				ctx,
				workerRPCTimeout,
			)

			response, err := client.AcquireJob(
				acquireCtx,
				&workerv1.AcquireJobRequest{
					WorkerId: string(cfg.ID),
				},
			)

			cancel()

			if err != nil {
				return fmt.Errorf(
					"acquire job: %w",
					err,
				)
			}

			if response.GetAssignment() == nil {
				continue
			}

			assignment := response.GetAssignment()
			if assignment == nil {
				continue
			}

			localAssignment := assignmentFromProto(
				assignment,
			)

			go func() {
				executionResult, err := cfg.Executor.Execute(
					ctx,
					localAssignment,
				)

				if err != nil {
					log.Printf(
						"execution infrastructure error job_id=%s attempt_id=%s error=%v",
						localAssignment.JobID,
						localAssignment.AttemptID,
						err,
					)
				}

				log.Printf(
					"execution finished job_id=%s attempt_id=%s outcome=%s failure_code=%s exit_code=%v oom_killed=%t message=%q",
					executionResult.JobID,
					executionResult.AttemptID,
					executionResult.Outcome,
					executionResult.FailureCode,
					executionResult.ExitCode,
					executionResult.OOMKilled,
					executionResult.Message,
				)
			}()

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

func assignmentFromProto(
	assignment *workerv1.JobAssignment,
) executor.Assignment {
	result := executor.Assignment{
		JobID:          assignment.GetJobId(),
		AttemptID:      assignment.GetAttemptId(),
		ContainerImage: assignment.GetContainerImage(),

		Command: append(
			[]string(nil),
			assignment.GetCommand()...,
		),

		Args: append(
			[]string(nil),
			assignment.GetArgs()...,
		),

		Environment: cloneStringMap(
			assignment.GetEnvironment(),
		),
	}

	resourceLimit := assignment.GetResourceLimit()

	if resourceLimit != nil {
		if resourceLimit.GetCpuMillis() > 0 {
			cpuLimit := resourceLimit.GetCpuMillis()
			result.CPULimitMillis = &cpuLimit
		}

		if resourceLimit.GetMemoryBytes() > 0 {
			memoryLimit := resourceLimit.GetMemoryBytes()
			result.MemoryLimitBytes = &memoryLimit
		}
	}

	if timeout := assignment.GetTimeout(); timeout != nil {
		result.Timeout = timeout.AsDuration()
	}

	return result
}

func cloneStringMap(
	source map[string]string,
) map[string]string {
	if source == nil {
		return nil
	}

	result := make(
		map[string]string,
		len(source),
	)

	for key, value := range source {
		result[key] = value
	}

	return result
}
