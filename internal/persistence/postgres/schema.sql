CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    idempotency_key TEXT,
    specification JSONB NOT NULL,
    status TEXT NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    cancellation_requested BOOLEAN NOT NULL DEFAULT FALSE,

    CONSTRAINT jobs_status_check CHECK (
        status IN (
            'QUEUED',
            'RUNNING',
            'SUCCEEDED',
            'FAILED',
            'CANCELLED'
        )
    ),

    CONSTRAINT jobs_version_positive CHECK (version > 0)
);

CREATE TABLE IF NOT EXISTS execution_attempts (
    attempt_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    worker_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    failure_information JSONB,

    CONSTRAINT execution_attempts_number_positive
        CHECK (attempt_number > 0),

    CONSTRAINT execution_attempts_status_check CHECK (
        status IN (
            'ASSIGNED',
            'RUNNING',
            'SUCCEEDED',
            'FAILED',
            'CANCELLED'
        )
    ),

    CONSTRAINT execution_attempts_version_positive
        CHECK (version > 0),

    CONSTRAINT execution_attempts_job_number_unique
        UNIQUE (job_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,

    status TEXT NOT NULL,

    cpu_millis BIGINT NOT NULL,
    memory_bytes BIGINT NOT NULL,

    available_cpu_millis BIGINT NOT NULL,
    available_memory_bytes BIGINT NOT NULL,

    container_runtimes TEXT[] NOT NULL DEFAULT '{}',

    registered_at TIMESTAMPTZ NOT NULL,
    last_heartbeat_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT workers_status_check CHECK (
        status IN ('AVAILABLE', 'UNAVAILABLE')
    ),

    CONSTRAINT workers_cpu_positive
        CHECK (cpu_millis > 0),

    CONSTRAINT workers_memory_positive
        CHECK (memory_bytes > 0),

    CONSTRAINT workers_available_cpu_nonnegative
        CHECK (available_cpu_millis >= 0),

    CONSTRAINT workers_available_memory_nonnegative
        CHECK (available_memory_bytes >= 0),

    CONSTRAINT workers_available_cpu_bounded
        CHECK (available_cpu_millis <= cpu_millis),

    CONSTRAINT workers_available_memory_bounded
        CHECK (available_memory_bytes <= memory_bytes)
);

-- Upgrade workers tables created before the status column existed.
-- Existing rows start UNAVAILABLE until the worker registers again.
ALTER TABLE workers
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'UNAVAILABLE'
        CONSTRAINT workers_status_check CHECK (
            status IN ('AVAILABLE', 'UNAVAILABLE')
        );

ALTER TABLE workers ALTER COLUMN status DROP DEFAULT;

CREATE INDEX IF NOT EXISTS workers_status_heartbeat_idx
    ON workers (status, last_heartbeat_at);

CREATE INDEX IF NOT EXISTS workers_last_heartbeat_idx
    ON workers (last_heartbeat_at);

CREATE INDEX IF NOT EXISTS jobs_status_created_at_idx
    ON jobs (status, created_at, job_id);

CREATE INDEX IF NOT EXISTS execution_attempts_job_id_idx
    ON execution_attempts (job_id, attempt_number);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_owner_idempotency_key_idx
    ON jobs (owner_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS jobs_owner_created_at_idx
	ON jobs (owner_id, created_at DESC, job_id DESC);

CREATE INDEX IF NOT EXISTS jobs_owner_status_created_at_idx
	ON jobs (owner_id, status, created_at DESC, job_id DESC);

CREATE INDEX IF NOT EXISTS jobs_queued_created_at_idx
    ON jobs (created_at ASC, job_id ASC)
    WHERE status = 'QUEUED';

CREATE INDEX IF NOT EXISTS workers_available_capacity_idx
    ON workers (
        available_cpu_millis,
        available_memory_bytes,
        worker_id
    )
    WHERE status = 'AVAILABLE';