// Package config provides configuration loading and validation for the application.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// Environment represents the runtime environment of the service.
type Environment string

// Supported application environments.
const (
	EnvironmentDevelopment Environment = "development"
	EnvironmentTest        Environment = "test"
	EnvironmentStaging     Environment = "staging"
	EnvironmentProduction  Environment = "production"
)

const (
	defaultEnvironment                 = EnvironmentDevelopment
	defaultShutdownTimeout             = 10 * time.Second
	defaultAPIAddress                  = ":8080"
	defaultSchedulerGRPCAddress        = ":9090"
	defaultWorkerHeartbeatTimeout      = 15 * time.Second
	defaultWorkerLivenessCheckInterval = 5 * time.Second
	defaultGRPCRPCTimeout              = 5 * time.Second
)

// Config contains the application's runtime configuration.
type Config struct {
	Environment                 Environment
	ShutdownTimeout             time.Duration
	APIAddress                  string
	DatabaseURL                 string
	SchedulerGRPCAddress        string
	WorkerHeartbeatTimeout      time.Duration
	WorkerLivenessCheckInterval time.Duration
	GRPCRPCTimeout              time.Duration
	WorkerAuthenticationToken   string
}

// Load loads and validates the application configuration.
func Load() (Config, error) {
	cfg := Config{
		Environment:                 defaultEnvironment,
		ShutdownTimeout:             defaultShutdownTimeout,
		SchedulerGRPCAddress:        defaultSchedulerGRPCAddress,
		WorkerHeartbeatTimeout:      defaultWorkerHeartbeatTimeout,
		WorkerLivenessCheckInterval: defaultWorkerLivenessCheckInterval,
		GRPCRPCTimeout:              defaultGRPCRPCTimeout,
	}

	cfg.APIAddress = defaultAPIAddress

	if value, ok := os.LookupEnv("DJS_ENVIRONMENT"); ok {
		cfg.Environment = Environment(strings.TrimSpace(value))
	}

	if value, ok := os.LookupEnv("DJS_SHUTDOWN_TIMEOUT"); ok {
		duration, err := time.ParseDuration(strings.TrimSpace(value))
		if err != nil {
			return Config{},
				fmt.Errorf("parse DJS_SHUTDOWN_TIMEOUT: %w", err)
		}

		cfg.ShutdownTimeout = duration
	}

	if value, ok := os.LookupEnv("DJS_API_ADDRESS"); ok {
		cfg.APIAddress = strings.TrimSpace(value)
	}

	if value, ok := os.LookupEnv("DJS_DATABASE_URL"); ok {
		cfg.DatabaseURL = strings.TrimSpace(value)
	}

	if value, ok := os.LookupEnv(
		"DJS_SCHEDULER_GRPC_ADDRESS",
	); ok {
		cfg.SchedulerGRPCAddress = strings.TrimSpace(value)
	}

	if value, ok := os.LookupEnv(
		"DJS_WORKER_HEARTBEAT_TIMEOUT",
	); ok {
		duration, err := time.ParseDuration(
			strings.TrimSpace(value),
		)
		if err != nil {
			return Config{}, fmt.Errorf(
				"parse DJS_WORKER_HEARTBEAT_TIMEOUT: %w",
				err,
			)
		}

		cfg.WorkerHeartbeatTimeout = duration
	}

	if value, ok := os.LookupEnv(
		"DJS_WORKER_LIVENESS_CHECK_INTERVAL",
	); ok {
		duration, err := time.ParseDuration(
			strings.TrimSpace(value),
		)
		if err != nil {
			return Config{}, fmt.Errorf(
				"parse DJS_WORKER_LIVENESS_CHECK_INTERVAL: %w",
				err,
			)
		}

		cfg.WorkerLivenessCheckInterval = duration
	}

	if value, ok := os.LookupEnv(
		"DJS_GRPC_RPC_TIMEOUT",
	); ok {
		duration, err := time.ParseDuration(
			strings.TrimSpace(value),
		)
		if err != nil {
			return Config{}, fmt.Errorf(
				"parse DJS_GRPC_RPC_TIMEOUT: %w",
				err,
			)
		}

		cfg.GRPCRPCTimeout = duration
	}

	if value, ok := os.LookupEnv(
		"DJS_WORKER_AUTH_TOKEN",
	); ok {
		cfg.WorkerAuthenticationToken = strings.TrimSpace(
			value,
		)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// Validate checks whether the configuration is valid.
func (c Config) Validate() error {
	switch c.Environment {
	case EnvironmentDevelopment,
		EnvironmentTest,
		EnvironmentStaging,
		EnvironmentProduction:
	default:
		return fmt.Errorf(
			"DJS_ENVIRONMENT must be one of development, test, staging, production: got %q",
			c.Environment,
		)
	}

	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf(
			"DJS_SHUTDOWN_TIMEOUT must be greater than zero: got %s",
			c.ShutdownTimeout,
		)
	}

	if c.WorkerHeartbeatTimeout <= 0 {
		return fmt.Errorf(
			"DJS_WORKER_HEARTBEAT_TIMEOUT must be greater than zero",
		)
	}

	if c.WorkerLivenessCheckInterval <= 0 {
		return fmt.Errorf(
			"DJS_WORKER_LIVENESS_CHECK_INTERVAL must be greater than zero",
		)
	}

	if c.GRPCRPCTimeout <= 0 {
		return fmt.Errorf(
			"DJS_GRPC_RPC_TIMEOUT must be greater than zero",
		)
	}

	if strings.TrimSpace(c.APIAddress) == "" {
		return fmt.Errorf("DJS_API_ADDRESS must not be empty")
	}

	if strings.TrimSpace(c.SchedulerGRPCAddress) == "" {
		return fmt.Errorf(
			"DJS_SCHEDULER_GRPC_ADDRESS must not be empty",
		)
	}

	return nil
}
