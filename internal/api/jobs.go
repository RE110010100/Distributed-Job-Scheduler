package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/apierrors"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/job"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/persistence"
)

const maxRequestBodyBytes = 1 << 20

type createJobRequest struct {
	IdempotencyKey string               `json:"idempotency_key,omitempty"`
	ContainerImage string               `json:"container_image"`
	Command        []string             `json:"command,omitempty"`
	Args           []string             `json:"args,omitempty"`
	Environment    map[string]string    `json:"environment,omitempty"`
	Resources      resourceRequirements `json:"resources,omitempty"`
	TimeoutSeconds *int64               `json:"timeout_seconds,omitempty"`
	RetryPolicy    retryPolicy          `json:"retry_policy,omitempty"`
}

type resourceRequirements struct {
	CPURequestMillis   *int64 `json:"cpu_request_millis,omitempty"`
	CPULimitMillis     *int64 `json:"cpu_limit_millis,omitempty"`
	MemoryRequestBytes *int64 `json:"memory_request_bytes,omitempty"`
	MemoryLimitBytes   *int64 `json:"memory_limit_bytes,omitempty"`
}

type retryPolicy struct {
	MaxAttempts *uint32 `json:"max_attempts,omitempty"`
}

type jobResponse struct {
	JobID                 job.ID               `json:"job_id"`
	OwnerID               string               `json:"owner_id"`
	IdempotencyKey        string               `json:"idempotency_key,omitempty"`
	ContainerImage        string               `json:"container_image"`
	Command               []string             `json:"command,omitempty"`
	Args                  []string             `json:"args,omitempty"`
	Environment           map[string]string    `json:"environment,omitempty"`
	Resources             resourceRequirements `json:"resources"`
	TimeoutSeconds        *int64               `json:"timeout_seconds,omitempty"`
	RetryPolicy           retryPolicy          `json:"retry_policy"`
	Status                job.Status           `json:"status"`
	CreatedAt             time.Time            `json:"created_at"`
	StartedAt             *time.Time           `json:"started_at,omitempty"`
	CompletedAt           *time.Time           `json:"completed_at,omitempty"`
	CancellationRequested bool                 `json:"cancellation_requested"`
}

type listJobsResponse struct {
	Jobs       []jobResponse `json:"jobs"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

func (s *Server) handleCreateJob(
	w http.ResponseWriter,
	r *http.Request,
) {
	request, err := decodeCreateJobRequest(w, r)
	if err != nil {
		writeAPIError(
			w,
			r,
			http.StatusBadRequest,
			apierrors.CodeInvalidArgument,
			err.Error(),
		)
		return
	}

	if err := validateCreateJobRequest(request); err != nil {
		writeAPIError(
			w,
			r,
			http.StatusBadRequest,
			apierrors.CodeInvalidArgument,
			err.Error(),
		)
		return
	}

	ownerID, ok := ownerIDFromContext(r.Context())
	if !ok {
		writeAPIError(
			w,
			r,
			http.StatusUnauthorized,
			apierrors.CodeUnauthenticated,
			"authentication required",
		)
		return
	}

	id, err := job.NewID()
	if err != nil {
		writeAPIError(
			w,
			r,
			http.StatusInternalServerError,
			apierrors.CodeInternal,
			"failed to create job",
		)
		return
	}

	now := time.Now().UTC()

	j := &job.Job{
		ID:             id,
		OwnerID:        ownerID,
		IdempotencyKey: strings.TrimSpace(request.IdempotencyKey),
		Spec:           request.specification(),
		Status:         job.StatusQueued,
		CreatedAt:      now,
	}

	persisted, created, err :=
		s.jobs.CreateJobIdempotent(r.Context(), j)
	if err != nil {
		writeAPIError(
			w,
			r,
			http.StatusInternalServerError,
			apierrors.CodeInternal,
			"failed to persist job",
		)
		return
	}

	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}

	writeJSON(w, status, newJobResponse(persisted))
}

func decodeCreateJobRequest(
	w http.ResponseWriter,
	r *http.Request,
) (createJobRequest, error) {
	r.Body = http.MaxBytesReader(
		w,
		r.Body,
		maxRequestBodyBytes,
	)

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var request createJobRequest

	if err := decoder.Decode(&request); err != nil {
		return createJobRequest{},
			errors.New("request body must contain valid JSON")
	}

	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return createJobRequest{},
			errors.New("request body must contain one JSON object")
	}

	return request, nil
}

func validateCreateJobRequest(request createJobRequest) error {

	if strings.TrimSpace(request.ContainerImage) == "" {
		return errors.New("container_image is required")
	}

	if request.TimeoutSeconds != nil &&
		*request.TimeoutSeconds <= 0 {
		return errors.New(
			"timeout_seconds must be greater than zero",
		)
	}

	if request.RetryPolicy.MaxAttempts != nil &&
		*request.RetryPolicy.MaxAttempts == 0 {
		return errors.New(
			"retry_policy.max_attempts must be greater than zero",
		)
	}

	if err := validateResources(request.Resources); err != nil {
		return err
	}

	return nil
}

func validateResources(resources resourceRequirements) error {
	if resources.CPURequestMillis != nil &&
		*resources.CPURequestMillis <= 0 {
		return errors.New(
			"resources.cpu_request_millis must be greater than zero",
		)
	}

	if resources.CPULimitMillis != nil &&
		*resources.CPULimitMillis <= 0 {
		return errors.New(
			"resources.cpu_limit_millis must be greater than zero",
		)
	}

	if resources.MemoryRequestBytes != nil &&
		*resources.MemoryRequestBytes <= 0 {
		return errors.New(
			"resources.memory_request_bytes must be greater than zero",
		)
	}

	if resources.MemoryLimitBytes != nil &&
		*resources.MemoryLimitBytes <= 0 {
		return errors.New(
			"resources.memory_limit_bytes must be greater than zero",
		)
	}

	if resources.CPURequestMillis != nil &&
		resources.CPULimitMillis != nil &&
		*resources.CPURequestMillis > *resources.CPULimitMillis {
		return errors.New(
			"resources.cpu_request_millis must not exceed cpu_limit_millis",
		)
	}

	if resources.MemoryRequestBytes != nil &&
		resources.MemoryLimitBytes != nil &&
		*resources.MemoryRequestBytes > *resources.MemoryLimitBytes {
		return errors.New(
			"resources.memory_request_bytes must not exceed memory_limit_bytes",
		)
	}

	return nil
}

func (r createJobRequest) specification() job.Specification {
	var timeout *time.Duration

	if r.TimeoutSeconds != nil {
		value := time.Duration(*r.TimeoutSeconds) * time.Second
		timeout = &value
	}

	return job.Specification{
		ContainerImage: strings.TrimSpace(r.ContainerImage),
		Command:        r.Command,
		Args:           r.Args,
		Environment:    r.Environment,
		Resources: job.ResourceRequirements{
			CPURequestMillis:   r.Resources.CPURequestMillis,
			CPULimitMillis:     r.Resources.CPULimitMillis,
			MemoryRequestBytes: r.Resources.MemoryRequestBytes,
			MemoryLimitBytes:   r.Resources.MemoryLimitBytes,
		},
		Timeout: timeout,
		RetryPolicy: job.RetryPolicy{
			MaxAttempts: r.RetryPolicy.MaxAttempts,
		},
	}
}

func newJobResponse(j *job.Job) jobResponse {
	var timeoutSeconds *int64

	if j.Spec.Timeout != nil {
		value := int64(*j.Spec.Timeout / time.Second)
		timeoutSeconds = &value
	}

	return jobResponse{
		JobID:          j.ID,
		OwnerID:        j.OwnerID,
		IdempotencyKey: j.IdempotencyKey,
		ContainerImage: j.Spec.ContainerImage,
		Command:        j.Spec.Command,
		Args:           j.Spec.Args,
		Environment:    j.Spec.Environment,
		Resources: resourceRequirements{
			CPURequestMillis:   j.Spec.Resources.CPURequestMillis,
			CPULimitMillis:     j.Spec.Resources.CPULimitMillis,
			MemoryRequestBytes: j.Spec.Resources.MemoryRequestBytes,
			MemoryLimitBytes:   j.Spec.Resources.MemoryLimitBytes,
		},
		TimeoutSeconds: timeoutSeconds,
		RetryPolicy: retryPolicy{
			MaxAttempts: j.Spec.RetryPolicy.MaxAttempts,
		},
		Status:                j.Status,
		CreatedAt:             j.CreatedAt,
		StartedAt:             j.StartedAt,
		CompletedAt:           j.CompletedAt,
		CancellationRequested: j.CancellationRequested,
	}
}

func (s *Server) handleGetJob(
	w http.ResponseWriter,
	r *http.Request,
) {
	id := job.ID(
		strings.TrimSpace(r.PathValue("job_id")),
	)

	if id == "" {
		writeAPIError(
			w,
			r,
			http.StatusBadRequest,
			apierrors.CodeInvalidArgument,
			"job_id is required",
		)
		return
	}

	ownerID, ok := ownerIDFromContext(r.Context())
	if !ok {
		writeAPIError(
			w,
			r,
			http.StatusUnauthorized,
			apierrors.CodeUnauthenticated,
			"authentication required",
		)
		return
	}

	j, err := s.jobs.GetJobForOwner(
		r.Context(),
		id,
		ownerID,
	)

	if errors.Is(err, persistence.ErrNotFound) {
		writeAPIError(
			w,
			r,
			http.StatusNotFound,
			apierrors.CodeNotFound,
			"job not found",
		)
		return
	}

	if err != nil {
		writeAPIError(
			w,
			r,
			http.StatusInternalServerError,
			apierrors.CodeInternal,
			"failed to retrieve job",
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		newJobResponse(j),
	)
}

func (s *Server) handleListJobs(
	w http.ResponseWriter,
	r *http.Request,
) {
	if err := validateListQuery(r); err != nil {
		writeAPIError(
			w,
			r,
			http.StatusBadRequest,
			apierrors.CodeInvalidArgument,
			err.Error(),
		)
		return
	}

	status, err := parseStatus(
		r.URL.Query().Get("status"),
	)
	if err != nil {
		writeAPIError(
			w,
			r,
			http.StatusBadRequest,
			apierrors.CodeInvalidArgument,
			err.Error(),
		)
		return
	}

	limit, err := parseListLimit(
		r.URL.Query().Get("limit"),
	)
	if err != nil {
		writeAPIError(
			w,
			r,
			http.StatusBadRequest,
			apierrors.CodeInvalidArgument,
			err.Error(),
		)
		return
	}

	cursor, err := decodeCursor(
		r.URL.Query().Get("cursor"),
	)
	if err != nil {
		writeAPIError(
			w,
			r,
			http.StatusBadRequest,
			apierrors.CodeInvalidArgument,
			err.Error(),
		)
		return
	}

	ownerID, ok := ownerIDFromContext(r.Context())
	if !ok {
		writeAPIError(
			w,
			r,
			http.StatusUnauthorized,
			apierrors.CodeUnauthenticated,
			"authentication required",
		)
		return
	}

	page, err := s.jobs.ListJobs(
		r.Context(),
		persistence.ListJobsOptions{
			OwnerID: ownerID,
			Status:  status,
			Limit:   limit,
			Cursor:  cursor,
		},
	)
	if err != nil {
		writeAPIError(
			w,
			r,
			http.StatusInternalServerError,
			apierrors.CodeInternal,
			"failed to list jobs",
		)
		return
	}

	response := listJobsResponse{
		Jobs: make([]jobResponse, 0, len(page.Jobs)),
	}

	for _, j := range page.Jobs {
		response.Jobs = append(
			response.Jobs,
			newJobResponse(j),
		)
	}

	response.NextCursor, err = encodeCursor(page.NextCursor)
	if err != nil {
		writeAPIError(
			w,
			r,
			http.StatusInternalServerError,
			apierrors.CodeInternal,
			"failed to encode pagination cursor",
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		response,
	)
}

func parseStatus(value string) (*job.Status, error) {
	if value == "" {
		return nil, nil
	}

	status := job.Status(strings.ToUpper(value))

	if !status.Valid() {
		return nil, fmt.Errorf(
			"invalid status %q",
			value,
		)
	}

	return &status, nil
}

func parseListLimit(value string) (int, error) {
	if value == "" {
		return defaultListLimit, nil
	}

	limit, err := strconv.Atoi(value)
	if err != nil {
		return 0, errors.New("limit must be an integer")
	}

	if limit <= 0 || limit > maxListLimit {
		return 0, fmt.Errorf(
			"limit must be between 1 and %d",
			maxListLimit,
		)
	}

	return limit, nil
}

func validateListQuery(r *http.Request) error {
	allowed := map[string]struct{}{
		"status": {},
		"limit":  {},
		"cursor": {},
	}

	for key := range r.URL.Query() {
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf(
				"unsupported query parameter %q",
				key,
			)
		}
	}

	return nil
}
