// Package main provides the scheduler service entry point.
package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/config"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/controlplane"
	workerv1 "github.com/RE110010100/Distributed-Job-Scheduler/internal/gen/worker/v1"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/persistence/postgres"
	"google.golang.org/grpc"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load configuration: %v", err)
	}

	if cfg.DatabaseURL == "" {
		log.Fatal("DJS_DATABASE_URL must be set")
	}

	if strings.TrimSpace(
		cfg.WorkerAuthenticationToken,
	) == "" {
		log.Fatal("DJS_WORKER_AUTH_TOKEN must be set")
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	store, err := postgres.Open(
		ctx,
		cfg.DatabaseURL,
	)
	if err != nil {
		log.Fatalf("open postgres store: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(ctx); err != nil {
		log.Fatalf("migrate postgres store: %v", err)
	}

	monitor, err := controlplane.NewLivenessMonitor(
		store,
		cfg.WorkerHeartbeatTimeout,
		cfg.WorkerLivenessCheckInterval,
	)
	if err != nil {
		log.Fatalf(
			"create liveness monitor: %v",
			err,
		)
	}

	listener, err := net.Listen(
		"tcp",
		cfg.SchedulerGRPCAddress,
	)
	if err != nil {
		log.Fatalf(
			"listen on %s: %v",
			cfg.SchedulerGRPCAddress,
			err,
		)
	}

	logger := log.Default()

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			controlplane.UnaryObservabilityInterceptor(
				logger,
			),
			controlplane.UnaryDeadlineInterceptor(
				cfg.GRPCRPCTimeout,
			),
			controlplane.UnaryAuthenticationInterceptor(
				cfg.WorkerAuthenticationToken,
			),
		),
	)

	workerv1.RegisterWorkerControlServiceServer(
		grpcServer,
		controlplane.NewServer(store),
	)

	serverErrors := make(chan error, 1)
	monitorErrors := make(chan error, 1)

	go func() {
		serverErrors <- grpcServer.Serve(listener)
	}()

	go func() {
		monitorErrors <- monitor.Run(ctx)
	}()

	log.Printf(
		"scheduler service started environment=%s grpc_address=%s",
		cfg.Environment,
		cfg.SchedulerGRPCAddress,
	)

	select {
	case <-ctx.Done():

	case err := <-serverErrors:
		if !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("serve gRPC: %v", err)
		}
		return

	case err := <-monitorErrors:
		if err != nil {
			log.Fatalf(
				"worker liveness monitor: %v",
				err,
			)
		}
	}

	stopped := make(chan struct{})

	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:

	case <-time.After(cfg.ShutdownTimeout):
		grpcServer.Stop()
	}

	log.Print("scheduler service stopped")
}
