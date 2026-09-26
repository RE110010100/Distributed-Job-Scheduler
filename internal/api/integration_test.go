package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/job"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/persistence/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	testOwnerA = "integration-service-a"
	testOwnerB = "integration-service-b"
	testTokenA = "integration-token-a"
	testTokenB = "integration-token-b"
)

type integrationEnvironment struct {
	server *httptest.Server
	store  *postgres.Store
	client *http.Client
}

func newIntegrationEnvironment(t *testing.T) *integrationEnvironment {
	t.Helper()

	databaseURL := os.Getenv("DJS_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DJS_TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	store, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open postgres store: %v", err)
	}

	if err := store.Migrate(ctx); err != nil {
		store.Close()
		t.Fatalf("migrate postgres store: %v", err)
	}

	resetIntegrationDatabase(t, databaseURL)

	authenticator, err := NewAuthenticator(
		[]ServiceCredential{
			{
				OwnerID: testOwnerA,
				Token:   testTokenA,
			},
			{
				OwnerID: testOwnerB,
				Token:   testTokenB,
			},
		},
	)
	if err != nil {
		store.Close()
		t.Fatalf("create authenticator: %v", err)
	}

	server := httptest.NewServer(
		NewServer(store, authenticator),
	)

	env := &integrationEnvironment{
		server: server,
		store:  store,
		client: server.Client(),
	}

	t.Cleanup(func() {
		server.Close()

		resetIntegrationDatabase(t, databaseURL)

		store.Close()
	})

	return env
}

func resetIntegrationDatabase(
	t *testing.T,
	databaseURL string,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf(
			"create integration cleanup pool: %v",
			err,
		)
	}
	defer pool.Close()

	const statement = `
		TRUNCATE TABLE
			execution_attempts,
			jobs
		RESTART IDENTITY CASCADE
	`

	if _, err := pool.Exec(ctx, statement); err != nil {
		t.Fatalf(
			"reset integration database: %v",
			err,
		)
	}
}

func (e *integrationEnvironment) request(
	t *testing.T,
	method string,
	path string,
	token string,
	body string,
) *http.Response {
	t.Helper()

	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}

	request, err := http.NewRequest(
		method,
		e.server.URL+path,
		reader,
	)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	if token != "" {
		request.Header.Set(
			"Authorization",
			"Bearer "+token,
		)
	}

	if body != "" {
		request.Header.Set(
			"Content-Type",
			"application/json",
		)
	}

	response, err := e.client.Do(request)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}

	return response
}

func closeBody(t *testing.T, response *http.Response) {
	t.Helper()

	if err := response.Body.Close(); err != nil {
		t.Errorf("close response body: %v", err)
	}
}

func decodeResponse[T any](
	t *testing.T,
	response *http.Response,
) T {
	t.Helper()
	defer closeBody(t, response)

	var value T

	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	return value
}

func requireStatus(
	t *testing.T,
	response *http.Response,
	expected int,
) {
	t.Helper()

	if response.StatusCode == expected {
		return
	}

	defer closeBody(t, response)

	body, _ := io.ReadAll(response.Body)

	t.Fatalf(
		"unexpected status: got=%d want=%d body=%s",
		response.StatusCode,
		expected,
		string(body),
	)
}

func submitJob(
	t *testing.T,
	env *integrationEnvironment,
	token string,
	idempotencyKey string,
	image string,
) jobResponse {
	t.Helper()

	body := fmt.Sprintf(
		`{
			"idempotency_key": %q,
			"container_image": %q,
			"command": ["echo"],
			"args": ["hello"],
			"resources": {
				"cpu_request_millis": 250,
				"cpu_limit_millis": 500,
				"memory_request_bytes": 134217728,
				"memory_limit_bytes": 268435456
			},
			"timeout_seconds": 60,
			"retry_policy": {
				"max_attempts": 3
			}
		}`,
		idempotencyKey,
		image,
	)

	response := env.request(
		t,
		http.MethodPost,
		"/v1/jobs",
		token,
		body,
	)

	requireStatus(t, response, http.StatusCreated)

	return decodeResponse[jobResponse](t, response)
}

func TestAPIRequiresAuthentication(t *testing.T) {
	env := newIntegrationEnvironment(t)

	response := env.request(
		t,
		http.MethodGet,
		"/v1/jobs",
		"",
		"",
	)

	requireStatus(
		t,
		response,
		http.StatusUnauthorized,
	)
	closeBody(t, response)

	response = env.request(
		t,
		http.MethodGet,
		"/v1/jobs",
		"invalid-token",
		"",
	)

	requireStatus(
		t,
		response,
		http.StatusUnauthorized,
	)
	closeBody(t, response)
}

func TestAPISubmitsAndRetrievesJob(t *testing.T) {
	env := newIntegrationEnvironment(t)

	created := submitJob(
		t,
		env,
		testTokenA,
		"submit-and-get",
		"alpine:3.22",
	)

	if created.JobID == "" {
		t.Fatal("submitted job has empty job_id")
	}

	if created.OwnerID != testOwnerA {
		t.Fatalf(
			"unexpected owner: got=%q want=%q",
			created.OwnerID,
			testOwnerA,
		)
	}

	if created.Status != job.StatusQueued {
		t.Fatalf(
			"unexpected status: got=%q want=%q",
			created.Status,
			job.StatusQueued,
		)
	}

	if created.ContainerImage != "alpine:3.22" {
		t.Fatalf(
			"unexpected image: got=%q",
			created.ContainerImage,
		)
	}

	response := env.request(
		t,
		http.MethodGet,
		"/v1/jobs/"+string(created.JobID),
		testTokenA,
		"",
	)

	requireStatus(t, response, http.StatusOK)

	retrieved := decodeResponse[jobResponse](
		t,
		response,
	)

	if retrieved.JobID != created.JobID {
		t.Fatalf(
			"unexpected job ID: got=%q want=%q",
			retrieved.JobID,
			created.JobID,
		)
	}

	if retrieved.OwnerID != testOwnerA {
		t.Fatalf(
			"unexpected owner: got=%q want=%q",
			retrieved.OwnerID,
			testOwnerA,
		)
	}
}

func TestAPISubmissionIsIdempotent(t *testing.T) {
	env := newIntegrationEnvironment(t)

	const body = `{
		"idempotency_key": "same-request",
		"container_image": "alpine:3.22"
	}`

	firstResponse := env.request(
		t,
		http.MethodPost,
		"/v1/jobs",
		testTokenA,
		body,
	)

	requireStatus(
		t,
		firstResponse,
		http.StatusCreated,
	)

	first := decodeResponse[jobResponse](
		t,
		firstResponse,
	)

	secondResponse := env.request(
		t,
		http.MethodPost,
		"/v1/jobs",
		testTokenA,
		body,
	)

	requireStatus(
		t,
		secondResponse,
		http.StatusOK,
	)

	second := decodeResponse[jobResponse](
		t,
		secondResponse,
	)

	if first.JobID != second.JobID {
		t.Fatalf(
			"idempotent submission returned different jobs: first=%q second=%q",
			first.JobID,
			second.JobID,
		)
	}
}

func TestAPIIdempotencyIsScopedByOwner(t *testing.T) {
	env := newIntegrationEnvironment(t)

	const body = `{
		"idempotency_key": "shared-key",
		"container_image": "alpine:3.22"
	}`

	responseA := env.request(
		t,
		http.MethodPost,
		"/v1/jobs",
		testTokenA,
		body,
	)
	requireStatus(t, responseA, http.StatusCreated)

	jobA := decodeResponse[jobResponse](
		t,
		responseA,
	)

	responseB := env.request(
		t,
		http.MethodPost,
		"/v1/jobs",
		testTokenB,
		body,
	)
	requireStatus(t, responseB, http.StatusCreated)

	jobB := decodeResponse[jobResponse](
		t,
		responseB,
	)

	if jobA.JobID == jobB.JobID {
		t.Fatalf(
			"different owners received same job ID: %q",
			jobA.JobID,
		)
	}

	if jobA.OwnerID != testOwnerA {
		t.Fatalf(
			"unexpected owner A: got=%q",
			jobA.OwnerID,
		)
	}

	if jobB.OwnerID != testOwnerB {
		t.Fatalf(
			"unexpected owner B: got=%q",
			jobB.OwnerID,
		)
	}
}

func TestAPICannotReadAnotherOwnersJob(t *testing.T) {
	env := newIntegrationEnvironment(t)

	created := submitJob(
		t,
		env,
		testTokenA,
		"owner-isolation",
		"alpine:3.22",
	)

	response := env.request(
		t,
		http.MethodGet,
		"/v1/jobs/"+string(created.JobID),
		testTokenB,
		"",
	)

	requireStatus(
		t,
		response,
		http.StatusNotFound,
	)

	closeBody(t, response)
}

func TestAPIListIsScopedByOwner(t *testing.T) {
	env := newIntegrationEnvironment(t)

	jobA := submitJob(
		t,
		env,
		testTokenA,
		"owner-a",
		"alpine:3.22",
	)

	submitJob(
		t,
		env,
		testTokenB,
		"owner-b",
		"busybox:latest",
	)

	response := env.request(
		t,
		http.MethodGet,
		"/v1/jobs",
		testTokenA,
		"",
	)

	requireStatus(t, response, http.StatusOK)

	list := decodeResponse[listJobsResponse](
		t,
		response,
	)

	if len(list.Jobs) != 1 {
		t.Fatalf(
			"unexpected job count: got=%d want=1",
			len(list.Jobs),
		)
	}

	if list.Jobs[0].JobID != jobA.JobID {
		t.Fatalf(
			"unexpected listed job: got=%q want=%q",
			list.Jobs[0].JobID,
			jobA.JobID,
		)
	}
}

func TestAPIListFiltersByStatus(t *testing.T) {
	env := newIntegrationEnvironment(t)

	queued := submitJob(
		t,
		env,
		testTokenA,
		"queued-job",
		"alpine:3.22",
	)

	running := submitJob(
		t,
		env,
		testTokenA,
		"running-job",
		"busybox:latest",
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	_, err := env.store.TransitionJobStatus(
		ctx,
		running.JobID,
		job.StatusQueued,
		job.StatusRunning,
		1,
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf(
			"transition job to running: %v",
			err,
		)
	}

	response := env.request(
		t,
		http.MethodGet,
		"/v1/jobs?status=RUNNING",
		testTokenA,
		"",
	)

	requireStatus(t, response, http.StatusOK)

	list := decodeResponse[listJobsResponse](
		t,
		response,
	)

	if len(list.Jobs) != 1 {
		t.Fatalf(
			"unexpected running job count: got=%d want=1",
			len(list.Jobs),
		)
	}

	if list.Jobs[0].JobID != running.JobID {
		t.Fatalf(
			"unexpected running job: got=%q want=%q",
			list.Jobs[0].JobID,
			running.JobID,
		)
	}

	if list.Jobs[0].JobID == queued.JobID {
		t.Fatalf(
			"queued job %q appeared in RUNNING filter",
			queued.JobID,
		)
	}
}

func TestAPIListRejectsInvalidStatus(t *testing.T) {
	env := newIntegrationEnvironment(t)

	response := env.request(
		t,
		http.MethodGet,
		"/v1/jobs?status=NOT_A_STATUS",
		testTokenA,
		"",
	)

	requireStatus(
		t,
		response,
		http.StatusBadRequest,
	)

	closeBody(t, response)
}

func TestAPIListUsesCursorPagination(t *testing.T) {
	env := newIntegrationEnvironment(t)

	const jobCount = 5

	for i := 0; i < jobCount; i++ {
		submitJob(
			t,
			env,
			testTokenA,
			fmt.Sprintf("page-%d", i),
			"alpine:3.22",
		)

		// PostgreSQL timestamps normally distinguish these anyway,
		// but keeping creation times apart makes the test easier to
		// reason about while job_id remains the deterministic tie-breaker.
		time.Sleep(time.Millisecond)
	}

	firstResponse := env.request(
		t,
		http.MethodGet,
		"/v1/jobs?limit=2",
		testTokenA,
		"",
	)

	requireStatus(
		t,
		firstResponse,
		http.StatusOK,
	)

	firstPage := decodeResponse[listJobsResponse](
		t,
		firstResponse,
	)

	if len(firstPage.Jobs) != 2 {
		t.Fatalf(
			"unexpected first page size: got=%d want=2",
			len(firstPage.Jobs),
		)
	}

	if firstPage.NextCursor == "" {
		t.Fatal("first page did not return next_cursor")
	}

	secondResponse := env.request(
		t,
		http.MethodGet,
		"/v1/jobs?limit=2&cursor="+firstPage.NextCursor,
		testTokenA,
		"",
	)

	requireStatus(
		t,
		secondResponse,
		http.StatusOK,
	)

	secondPage := decodeResponse[listJobsResponse](
		t,
		secondResponse,
	)

	if len(secondPage.Jobs) != 2 {
		t.Fatalf(
			"unexpected second page size: got=%d want=2",
			len(secondPage.Jobs),
		)
	}

	seen := make(map[job.ID]struct{})

	for _, listedJob := range firstPage.Jobs {
		seen[listedJob.JobID] = struct{}{}
	}

	for _, listedJob := range secondPage.Jobs {
		if _, exists := seen[listedJob.JobID]; exists {
			t.Fatalf(
				"job %q appeared on multiple pages",
				listedJob.JobID,
			)
		}

		seen[listedJob.JobID] = struct{}{}
	}

	thirdResponse := env.request(
		t,
		http.MethodGet,
		"/v1/jobs?limit=2&cursor="+secondPage.NextCursor,
		testTokenA,
		"",
	)

	requireStatus(
		t,
		thirdResponse,
		http.StatusOK,
	)

	thirdPage := decodeResponse[listJobsResponse](
		t,
		thirdResponse,
	)

	if len(thirdPage.Jobs) != 1 {
		t.Fatalf(
			"unexpected third page size: got=%d want=1",
			len(thirdPage.Jobs),
		)
	}

	if thirdPage.NextCursor != "" {
		t.Fatalf(
			"final page unexpectedly returned cursor %q",
			thirdPage.NextCursor,
		)
	}

	for _, listedJob := range thirdPage.Jobs {
		if _, exists := seen[listedJob.JobID]; exists {
			t.Fatalf(
				"job %q appeared on multiple pages",
				listedJob.JobID,
			)
		}

		seen[listedJob.JobID] = struct{}{}
	}

	if len(seen) != jobCount {
		t.Fatalf(
			"pagination returned %d unique jobs; want %d",
			len(seen),
			jobCount,
		)
	}
}

func TestAPIListRejectsInvalidPagination(t *testing.T) {
	env := newIntegrationEnvironment(t)

	tests := []string{
		"/v1/jobs?limit=0",
		"/v1/jobs?limit=101",
		"/v1/jobs?limit=abc",
		"/v1/jobs?cursor=not-a-valid-cursor",
	}

	for _, path := range tests {
		t.Run(path, func(t *testing.T) {
			response := env.request(
				t,
				http.MethodGet,
				path,
				testTokenA,
				"",
			)

			requireStatus(
				t,
				response,
				http.StatusBadRequest,
			)

			closeBody(t, response)
		})
	}
}

func TestAPISubmitRejectsInvalidJob(t *testing.T) {
	env := newIntegrationEnvironment(t)

	tests := []struct {
		name string
		body string
	}{
		{
			name: "missing image",
			body: `{}`,
		},
		{
			name: "zero timeout",
			body: `{
				"container_image": "alpine:3.22",
				"timeout_seconds": 0
			}`,
		},
		{
			name: "zero attempts",
			body: `{
				"container_image": "alpine:3.22",
				"retry_policy": {
					"max_attempts": 0
				}
			}`,
		},
		{
			name: "cpu request exceeds limit",
			body: `{
				"container_image": "alpine:3.22",
				"resources": {
					"cpu_request_millis": 1000,
					"cpu_limit_millis": 500
				}
			}`,
		},
		{
			name: "unknown field",
			body: `{
				"container_image": "alpine:3.22",
				"unknown_field": true
			}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := env.request(
				t,
				http.MethodPost,
				"/v1/jobs",
				testTokenA,
				test.body,
			)

			requireStatus(
				t,
				response,
				http.StatusBadRequest,
			)

			closeBody(t, response)
		})
	}
}

func TestAPIRejectsClientSuppliedOwner(t *testing.T) {
	env := newIntegrationEnvironment(t)

	body := []byte(`{
		"owner_id": "spoofed-service",
		"container_image": "alpine:3.22"
	}`)

	request, err := http.NewRequest(
		http.MethodPost,
		env.server.URL+"/v1/jobs",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	request.Header.Set(
		"Authorization",
		"Bearer "+testTokenA,
	)
	request.Header.Set(
		"Content-Type",
		"application/json",
	)

	response, err := env.client.Do(request)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}

	requireStatus(
		t,
		response,
		http.StatusBadRequest,
	)

	closeBody(t, response)
}
