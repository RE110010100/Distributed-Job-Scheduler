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
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/worker"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load configuration: %v", err)
	}

	workerID := strings.TrimSpace(
		os.Getenv("DJS_WORKER_ID"),
	)
	if workerID == "" {
		log.Fatal("DJS_WORKER_ID must be set")
	}

	schedulerAddress := strings.TrimSpace(
		os.Getenv("DJS_SCHEDULER_ADDRESS"),
	)
	if schedulerAddress == "" {
		log.Fatal("DJS_SCHEDULER_ADDRESS must be set")
	}

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

	err = worker.Run(
		ctx,
		worker.ClientConfig{
			ID:               worker.ID(workerID),
			SchedulerAddress: schedulerAddress,
			Capacity: worker.Capacity{
				CPUMillis:   cpuMillis,
				MemoryBytes: memoryBytes,
			},
			ContainerRuntimes: []string{
				"docker",
			},
		},
	)
	if err != nil {
		log.Fatalf("run worker: %v", err)
	}

	log.Print("worker service stopped")
}

func requiredPositiveInt64(name string) int64 {
	value := strings.TrimSpace(
		os.Getenv(name),
	)

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
