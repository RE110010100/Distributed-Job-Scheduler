// Package main provides the API server entry point.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load configuration: %v", err)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	log.Printf("api service started environment=%s", cfg.Environment)

	<-ctx.Done()

	log.Printf(
		"api service stopping shutdown_timeout=%s",
		cfg.ShutdownTimeout,
	)
}
