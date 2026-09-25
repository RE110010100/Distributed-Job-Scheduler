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

	databaseURL := os.Getenv("DJS_DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DJS_DATABASE_URL must be set")
	}

	credentials, err := api.ParseServiceCredentials(
		os.Getenv("DJS_INTERNAL_API_CREDENTIALS"),
	)
	if err != nil {
		log.Fatalf("load internal API credentials: %v", err)
	}

	authenticator, err := api.NewAuthenticator(credentials)
	if err != nil {
		log.Fatalf("create authenticator: %v", err)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	store, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		log.Fatalf("open postgres store: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(ctx); err != nil {
		log.Fatalf("migrate postgres store: %v", err)
	}

	handler := api.NewServer(
		store,
		authenticator,
	)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErrors := make(chan error, 1)

	go func() {
		log.Printf(
			"api service started environment=%s address=%s",
			cfg.Environment,
			server.Addr,
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

	log.Print("api service stopped")
}
