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
	defaultEnvironment     = EnvironmentDevelopment
	defaultShutdownTimeout = 10 * time.Second
)

// Config contains the application's runtime configuration.
type Config struct {
	Environment     Environment
	ShutdownTimeout time.Duration
}

// Load loads and validates the application configuration.
func Load() (Config, error) {
	cfg := Config{
		Environment:     defaultEnvironment,
		ShutdownTimeout: defaultShutdownTimeout,
	}

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

	return nil
}
