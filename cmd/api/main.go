// Package main provides the API server entry point.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/api"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/config"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/persistence/postgres"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load configuration: %v", err)
	}

	if cfg.DatabaseURL == "" {
		log.Fatal("DJS_DATABASE_URL must be set for the API service")
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

	handler := api.NewServer(store)

	server := &http.Server{
		Addr:              cfg.APIAddress,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErrors := make(chan error, 1)

	go func() {
		log.Printf(
			"api service started environment=%s address=%s",
			cfg.Environment,
			cfg.APIAddress,
		)

		serverErrors <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve HTTP: %v", err)
		}
		return
	}

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		cfg.ShutdownTimeout,
	)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown HTTP server: %v", err)
	}

	log.Printf(
		"api service stopped shutdown_timeout=%s",
		cfg.ShutdownTimeout,
	)
}
