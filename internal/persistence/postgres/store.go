// Package postgres implements the persistence repositories on top of PostgreSQL.
package postgres

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

// Store is a PostgreSQL-backed implementation of persistence.Repository.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to the database at databaseURL and verifies connectivity.
func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &Store{pool: pool}, nil
}

// Close releases all pooled database connections.
func (s *Store) Close() {
	s.pool.Close()
}

// Migrate applies the embedded database schema.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("apply postgres schema: %w", err)
	}

	return nil
}
