package job

import "time"

// AttemptID uniquely identifies one physical execution attempt.
type AttemptID string

// AttemptStatus represents the lifecycle state of one physical execution.
type AttemptStatus string

// Attempt statuses represent the lifecycle states of a job execution attempt.
const (
	AttemptStatusAssigned  AttemptStatus = "ASSIGNED"
	AttemptStatusRunning   AttemptStatus = "RUNNING"
	AttemptStatusSucceeded AttemptStatus = "SUCCEEDED"
	AttemptStatusFailed    AttemptStatus = "FAILED"
	AttemptStatusCancelled AttemptStatus = "CANCELLED"
)

// ExecutionAttempt represents one physical attempt to execute a logical job.
type ExecutionAttempt struct {
	ID                 AttemptID
	JobID              ID
	WorkerID           string
	AttemptNumber      uint32
	Status             AttemptStatus
	StartedAt          *time.Time
	CompletedAt        *time.Time
	FailureInformation *FailureInformation
}

// FailureInformation records why an execution attempt failed.
//
// Code is intended to be a stable machine-readable classification.
// Message is diagnostic text and must not be used for programmatic decisions.
//
// Concrete retry classification is intentionally deferred because the V1
// specification leaves retryability policy as an open question.
type FailureInformation struct {
	Code    string
	Message string
}
