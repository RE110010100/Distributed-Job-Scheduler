// Package job defines the domain model for jobs and their execution attempts.
package job

import "time"

// DefaultMaxAttempts is the default maximum number of execution attempts for a job.
const DefaultMaxAttempts uint32 = 3

// ID uniquely identifies a logical job.
type ID string

// Status represents the logical lifecycle state of a job.
type Status string

// Job statuses represent the lifecycle states of a job.
const (
	StatusQueued    Status = "QUEUED"
	StatusRunning   Status = "RUNNING"
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
	StatusCancelled Status = "CANCELLED"
)

// Job represents one logical unit of work submitted to the platform.
//
// Job is intentionally persistence-agnostic. Persistence implementations
// introduced in later tasks are responsible for enforcing atomic state
// transitions and durability.
type Job struct {
	ID                    ID
	OwnerID               string
	IdempotencyKey        string
	Spec                  Specification
	Status                Status
	CreatedAt             time.Time
	StartedAt             *time.Time
	CompletedAt           *time.Time
	CancellationRequested bool
}

// Specification describes the container and execution policy for a job.
//
// Optional policy fields remain nil when omitted. Platform defaults are
// applied by the appropriate application/service layer rather than being
// embedded in the domain model.
type Specification struct {
	ContainerImage string
	Command        []string
	Args           []string
	Environment    map[string]string
	Resources      ResourceRequirements
	Timeout        *time.Duration
	RetryPolicy    RetryPolicy
}

// ResourceRequirements contains the V1 CPU and memory scheduling and
// enforcement policy.
//
// CPU values are represented in millicores to avoid floating-point resource
// accounting. For example, 1000 millicores represents one CPU core.
//
// Memory values are represented in bytes.
type ResourceRequirements struct {
	CPURequestMillis   *int64
	CPULimitMillis     *int64
	MemoryRequestBytes *int64
	MemoryLimitBytes   *int64
}

// RetryPolicy describes the bounded number of physical execution attempts.
//
// A nil MaxAttempts means the centrally managed platform default applies.
// The V1 specification defines that default as three attempts.
type RetryPolicy struct {
	MaxAttempts *uint32
}

// EffectiveMaxAttempts returns the configured maximum number of attempts or
// the V1 platform default when no explicit value was supplied.
func (p RetryPolicy) EffectiveMaxAttempts() uint32 {
	if p.MaxAttempts == nil {
		return DefaultMaxAttempts
	}

	return *p.MaxAttempts
}
