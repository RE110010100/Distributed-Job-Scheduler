// Package main provides the scheduler service entry point.
package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
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

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	store, err := postgres.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("open postgres store: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(ctx); err != nil {
		log.Fatalf("migrate postgres store: %v", err)
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

	grpcServer := grpc.NewServer()

	workerv1.RegisterWorkerControlServiceServer(
		grpcServer,
		controlplane.NewServer(store),
	)

	serverErrors := make(chan error, 1)

	go func() {
		log.Printf(
			"scheduler service started environment=%s grpc_address=%s",
			cfg.Environment,
			cfg.SchedulerGRPCAddress,
		)

		serverErrors <- grpcServer.Serve(listener)
	}()

	select {
	case <-ctx.Done():

	case err := <-serverErrors:
		if !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("serve gRPC: %v", err)
		}

		return
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
