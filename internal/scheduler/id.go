package scheduler

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/job"
)

func newAttemptID() (job.AttemptID, error) {
	var value [16]byte

	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf(
			"read cryptographic randomness: %w",
			err,
		)
	}

	return job.AttemptID(
		"attempt_" + hex.EncodeToString(value[:]),
	), nil
}
