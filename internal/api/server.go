// Package api implements the V1 HTTP job-management API.
package api

import (
	"net/http"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/persistence"
)

// Server implements the HTTP job-management API.
type Server struct {
	jobs persistence.JobRepository
	auth *Authenticator
	mux  *http.ServeMux
}

// NewServer creates an API server backed by jobs.
func NewServer(
	jobs persistence.JobRepository,
	auth *Authenticator,
) *Server {
	s := &Server{
		jobs: jobs,
		auth: auth,
		mux:  http.NewServeMux(),
	}

	s.routes()

	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /v1/jobs", s.handleCreateJob)
	s.mux.HandleFunc("GET /v1/jobs", s.handleListJobs)
	s.mux.HandleFunc("GET /v1/jobs/{job_id}", s.handleGetJob)
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(
	w http.ResponseWriter,
	r *http.Request,
) {
	handler := s.withAuthentication(s.mux)
	handler = s.withRequestID(handler)

	handler.ServeHTTP(w, r)
}
