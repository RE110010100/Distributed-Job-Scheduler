// Package main provides the worker service entry point.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/config"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/executor"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/worker"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load configuration: %v", err)
	}

	workerID := requiredString("DJS_WORKER_ID")

	schedulerAddress := requiredString(
		"DJS_SCHEDULER_ADDRESS",
	)

	authenticationToken := requiredString(
		"DJS_WORKER_AUTH_TOKEN",
	)

	cpuMillis := requiredPositiveInt64(
		"DJS_WORKER_CPU_MILLIS",
	)

	memoryBytes := requiredPositiveInt64(
		"DJS_WORKER_MEMORY_BYTES",
	)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	log.Printf(
		"worker service started environment=%s worker_id=%s",
		cfg.Environment,
		workerID,
	)

	dockerExecutor, err := executor.NewDockerExecutor()
	if err != nil {
		log.Fatalf(
			"initialize Docker executor: %v",
			err,
		)
	}

	defer func() {
		if err := dockerExecutor.Close(); err != nil {
			log.Printf(
				"close Docker executor: %v",
				err,
			)
		}
	}()

	err = worker.Run(
		ctx,
		worker.ClientConfig{
			ID: worker.ID(workerID),

			SchedulerAddress: schedulerAddress,

			AuthenticationToken: authenticationToken,

			Capacity: worker.Capacity{
				CPUMillis:   cpuMillis,
				MemoryBytes: memoryBytes,
			},

			ContainerRuntimes: []string{
				"docker",
			},

			Executor: dockerExecutor,
		},
	)
	if err != nil {
		log.Fatalf("run worker: %v", err)
	}

	log.Print("worker service stopped")
}

func requiredString(name string) string {
	value := strings.TrimSpace(
		os.Getenv(name),
	)

	if value == "" {
		log.Fatalf("%s must be set", name)
	}

	return value
}

func requiredPositiveInt64(name string) int64 {
	value := requiredString(name)

	parsed, err := strconv.ParseInt(
		value,
		10,
		64,
	)
	if err != nil || parsed <= 0 {
		log.Fatalf(
			"%s must contain a positive integer",
			name,
		)
	}

	return parsed
}
