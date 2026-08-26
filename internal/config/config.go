package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type Environment string

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

type Config struct {
	Environment     Environment
	ShutdownTimeout time.Duration
}

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
