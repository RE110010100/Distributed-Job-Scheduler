package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/job"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/persistence"
)

const (
	defaultListLimit = 50
	maxListLimit     = 100
)

type cursorPayload struct {
	CreatedAt time.Time `json:"created_at"`
	JobID     job.ID    `json:"job_id"`
}

func encodeCursor(
	cursor *persistence.JobCursor,
) (string, error) {
	if cursor == nil {
		return "", nil
	}

	payload := cursorPayload{
		CreatedAt: cursor.CreatedAt,
		JobID:     cursor.JobID,
	}

	value, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(value), nil
}

func decodeCursor(
	value string,
) (*persistence.JobCursor, error) {
	if value == "" {
		return nil, nil
	}

	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, errors.New("cursor is invalid")
	}

	var payload cursorPayload

	if err := json.Unmarshal(decoded, &payload); err != nil {
		return nil, errors.New("cursor is invalid")
	}

	if payload.CreatedAt.IsZero() || payload.JobID == "" {
		return nil, errors.New("cursor is invalid")
	}

	return &persistence.JobCursor{
		CreatedAt: payload.CreatedAt,
		JobID:     payload.JobID,
	}, nil
}
