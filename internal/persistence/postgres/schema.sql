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

CREATE INDEX IF NOT EXISTS jobs_status_created_at_idx
    ON jobs (status, created_at, job_id);

CREATE INDEX IF NOT EXISTS execution_attempts_job_id_idx
    ON execution_attempts (job_id, attempt_number);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_owner_idempotency_key_idx
    ON jobs (owner_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;