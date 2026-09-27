package controlplane

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	workerv1 "github.com/RE110010100/Distributed-Job-Scheduler/internal/gen/worker/v1"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/persistence/postgres"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const integrationWorkerToken = "integration-worker-token"

func TestWorkerJoinHeartbeatTimeoutAndRejoin(
	t *testing.T,
) {
	databaseURL := os.Getenv(
		"DJS_TEST_DATABASE_URL",
	)
	if databaseURL == "" {
		t.Skip(
			"DJS_TEST_DATABASE_URL is not set",
		)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()

	store, err := postgres.Open(
		ctx,
		databaseURL,
	)
	if err != nil {
		t.Fatalf("open postgres store: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate postgres: %v", err)
	}

	workerID := worker.ID(
		"task-027-integration-worker",
	)

	listener, err := net.Listen(
		"tcp",
		"127.0.0.1:0",
	)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			UnaryDeadlineInterceptor(
				2*time.Second,
			),
			UnaryAuthenticationInterceptor(
				integrationWorkerToken,
			),
		),
	)

	workerv1.RegisterWorkerControlServiceServer(
		grpcServer,
		NewServer(store),
	)

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
	})

	connection, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		t.Fatalf("create gRPC client: %v", err)
	}
	defer func() {
		_ = connection.Close()
	}()

	client := workerv1.NewWorkerControlServiceClient(
		connection,
	)

	authCtx := metadata.AppendToOutgoingContext(
		ctx,
		"authorization",
		"Bearer "+integrationWorkerToken,
	)

	// Join.
	_, err = client.RegisterWorker(
		authCtx,
		&workerv1.RegisterWorkerRequest{
			WorkerId: string(workerID),
			Capacity: &workerv1.WorkerCapacity{
				CpuMillis:   4000,
				MemoryBytes: 8 << 30,
			},
			Capabilities: &workerv1.WorkerCapabilities{
				ContainerRuntimes: []string{
					"docker",
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	persisted, err := store.GetWorker(
		ctx,
		workerID,
	)
	if err != nil {
		t.Fatalf("get registered worker: %v", err)
	}

	if persisted.Status != worker.StatusAvailable {
		t.Fatalf(
			"registered worker status: got=%q want=%q",
			persisted.Status,
			worker.StatusAvailable,
		)
	}

	// Heartbeat.
	_, err = client.Heartbeat(
		authCtx,
		&workerv1.HeartbeatRequest{
			WorkerId: string(workerID),
			AvailableCapacity: &workerv1.WorkerCapacity{
				CpuMillis:   2500,
				MemoryBytes: 6 << 30,
			},
		},
	)
	if err != nil {
		t.Fatalf("heartbeat worker: %v", err)
	}

	persisted, err = store.GetWorker(
		ctx,
		workerID,
	)
	if err != nil {
		t.Fatalf(
			"get worker after heartbeat: %v",
			err,
		)
	}

	if persisted.AvailableCapacity.CPUMillis != 2500 {
		t.Fatalf(
			"available CPU: got=%d want=2500",
			persisted.AvailableCapacity.CPUMillis,
		)
	}

	// Timeout.
	monitor, err := NewLivenessMonitor(
		store,
		time.Millisecond,
		time.Millisecond,
	)
	if err != nil {
		t.Fatalf(
			"create liveness monitor: %v",
			err,
		)
	}

	monitor.now = func() time.Time {
		return persisted.LastHeartbeatAt.Add(
			2 * time.Millisecond,
		)
	}

	affected, err := monitor.Reconcile(ctx)
	if err != nil {
		t.Fatalf(
			"reconcile worker liveness: %v",
			err,
		)
	}

	if affected != 1 {
		t.Fatalf(
			"workers marked unavailable: got=%d want=1",
			affected,
		)
	}

	persisted, err = store.GetWorker(
		ctx,
		workerID,
	)
	if err != nil {
		t.Fatalf(
			"get timed-out worker: %v",
			err,
		)
	}

	if persisted.Status != worker.StatusUnavailable {
		t.Fatalf(
			"timed-out worker status: got=%q want=%q",
			persisted.Status,
			worker.StatusUnavailable,
		)
	}

	// Rejoin through a fresh heartbeat.
	_, err = client.Heartbeat(
		authCtx,
		&workerv1.HeartbeatRequest{
			WorkerId: string(workerID),
			AvailableCapacity: &workerv1.WorkerCapacity{
				CpuMillis:   4000,
				MemoryBytes: 8 << 30,
			},
		},
	)
	if err != nil {
		t.Fatalf(
			"heartbeat after timeout: %v",
			err,
		)
	}

	persisted, err = store.GetWorker(
		ctx,
		workerID,
	)
	if err != nil {
		t.Fatalf(
			"get rejoined worker: %v",
			err,
		)
	}

	if persisted.Status != worker.StatusAvailable {
		t.Fatalf(
			"rejoined worker status: got=%q want=%q",
			persisted.Status,
			worker.StatusAvailable,
		)
	}
}

func TestWorkerRPCRejectsUnauthenticatedCaller(
	t *testing.T,
) {
	databaseURL := os.Getenv(
		"DJS_TEST_DATABASE_URL",
	)
	if databaseURL == "" {
		t.Skip(
			"DJS_TEST_DATABASE_URL is not set",
		)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	store, err := postgres.Open(
		ctx,
		databaseURL,
	)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate postgres: %v", err)
	}

	listener, err := net.Listen(
		"tcp",
		"127.0.0.1:0",
	)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := grpc.NewServer(
		grpc.UnaryInterceptor(
			UnaryAuthenticationInterceptor(
				integrationWorkerToken,
			),
		),
	)

	workerv1.RegisterWorkerControlServiceServer(
		server,
		NewServer(store),
	)

	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()

	connection, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	defer func() {
		_ = connection.Close()
	}()

	client := workerv1.NewWorkerControlServiceClient(
		connection,
	)

	_, err = client.RegisterWorker(
		ctx,
		&workerv1.RegisterWorkerRequest{
			WorkerId: "unauthenticated-worker",
			Capacity: &workerv1.WorkerCapacity{
				CpuMillis:   1000,
				MemoryBytes: 1 << 30,
			},
			Capabilities: &workerv1.WorkerCapabilities{
				ContainerRuntimes: []string{
					"docker",
				},
			},
		},
	)

	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf(
			"unexpected status: got=%s want=%s",
			status.Code(err),
			codes.Unauthenticated,
		)
	}
}
