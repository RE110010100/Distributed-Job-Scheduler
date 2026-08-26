# Generic Distributed Job Scheduler

Production-oriented implementation of the V1 generic distributed job
scheduler specification.

## Service boundaries

- `cmd/api`: client-facing job-management service. REST conventions are
  introduced in TASK-005 and the job API is implemented in Milestone 3.
- `cmd/scheduler`: scheduling/control-plane process. Authoritative state
  must not depend on this process's in-memory state.
- `cmd/worker`: worker process responsible for executing assigned
  containerized jobs.
- `internal/config`: shared environment-based configuration loading and
  validation.

These are deployment/process boundaries, not separate Go modules.

Keeping one Go module allows domain and infrastructure packages to evolve
without premature cross-module versioning or duplicated shared types.

## Prerequisites

- Go 1.26+
- Docker with Docker Compose v2 for the containerized local environment

## Local setup

Optionally create your local environment file:

```bash
cp .env.example .env