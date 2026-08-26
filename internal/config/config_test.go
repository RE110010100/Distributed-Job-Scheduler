package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	unsetEnv(t, "DJS_ENVIRONMENT")
	unsetEnv(t, "DJS_SHUTDOWN_TIMEOUT")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Environment != EnvironmentDevelopment {
		t.Fatalf(
			"Environment = %q, want %q",
			cfg.Environment,
			EnvironmentDevelopment,
		)
	}

	if cfg.ShutdownTimeout != 10*time.Second {
		t.Fatalf(
			"ShutdownTimeout = %s, want 10s",
			cfg.ShutdownTimeout,
		)
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("DJS_ENVIRONMENT", "production")
	t.Setenv("DJS_SHUTDOWN_TIMEOUT", "30s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Environment != EnvironmentProduction {
		t.Fatalf(
			"Environment = %q, want %q",
			cfg.Environment,
			EnvironmentProduction,
		)
	}

	if cfg.ShutdownTimeout != 30*time.Second {
		t.Fatalf(
			"ShutdownTimeout = %s, want 30s",
			cfg.ShutdownTimeout,
		)
	}
}

func TestLoadRejectsInvalidEnvironment(t *testing.T) {
	t.Setenv("DJS_ENVIRONMENT", "local")
	unsetEnv(t, "DJS_SHUTDOWN_TIMEOUT")

	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want error")
	}
}

func TestLoadRejectsInvalidShutdownTimeout(t *testing.T) {
	unsetEnv(t, "DJS_ENVIRONMENT")
	t.Setenv("DJS_SHUTDOWN_TIMEOUT", "forever")

	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want error")
	}
}

func TestLoadRejectsNonPositiveShutdownTimeout(t *testing.T) {
	unsetEnv(t, "DJS_ENVIRONMENT")
	t.Setenv("DJS_SHUTDOWN_TIMEOUT", "0s")

	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want error")
	}
}

func unsetEnv(t *testing.T, key string) {
	t.Helper()

	value, existed := os.LookupEnv(key)

	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("Unsetenv(%q): %v", key, err)
	}

	t.Cleanup(func() {
		if existed {
			if err := os.Setenv(key, value); err != nil {
				t.Errorf("restore %q: %v", key, err)
			}
			return
		}

		if err := os.Unsetenv(key); err != nil {
			t.Errorf("cleanup %q: %v", key, err)
		}
	})
}
