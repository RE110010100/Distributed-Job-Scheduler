package postgres

import (
	"testing"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/persistence"
)

func TestStoreImplementsRepository(_ *testing.T) {
	var _ persistence.Repository = (*Store)(nil)
}
