# API and RPC Conventions

This document defines the protocol conventions for the distributed job
scheduler.

It applies to:

- client -> API REST communication
- API -> control-plane gRPC communication
- control-plane <-> worker gRPC communication

Concrete job REST endpoints are defined in Milestone 3.

Concrete worker protobuf services and messages are defined in TASK-021.

## 1. Versioning

### REST

Public REST resources are versioned in the URL:

    /v1/...

Breaking API changes require a new major version.

Backward-compatible additions do not require a new version. Examples
include:

- adding optional request fields
- adding response fields
- adding endpoints

Clients must ignore response fields they do not understand.

### gRPC / Protobuf

Protobuf packages use a version suffix:

    djs.worker.v1
    djs.controlplane.v1

Breaking wire or semantic changes require a new package version.

## 2. Request IDs

Every inbound REST request and gRPC call must have a request ID.

The canonical REST header is:

    X-Request-ID

If a valid request ID is supplied by the caller, it is propagated.

If one is absent or invalid, the receiving service generates one.

Request IDs must:

- contain only printable ASCII characters
- be between 1 and 128 characters
- not contain whitespace or control characters
- be treated as opaque correlation identifiers

REST responses return the effective request ID using `X-Request-ID`.

For gRPC, the request ID is propagated through metadata using:

    x-request-id

Request IDs are for observability and correlation only. They are not
idempotency keys and must never be used to deduplicate operations.

## 3. REST Errors

REST errors use a stable JSON envelope:

    {
      "error": {
        "code": "not_found",
        "message": "job not found",
        "request_id": "..."
      }
    }

`code` is intended for programmatic handling.

`message` is human-readable and must not be parsed by clients.

The V1 error codes are:

| Code | HTTP Status |
|---|---|
| invalid_argument | 400 |
| unauthenticated | 401 |
| permission_denied | 403 |
| not_found | 404 |
| conflict | 409 |
| resource_exhausted | 429 |
| internal | 500 |
| unavailable | 503 |
| deadline_exceeded | 504 |

Internal implementation details, stack traces, database errors, and
sensitive information must not be exposed to callers.

## 4. gRPC Errors

Internal RPCs use canonical gRPC status codes.

The semantic mapping is:

| Application code | gRPC status |
|---|---|
| invalid_argument | INVALID_ARGUMENT |
| unauthenticated | UNAUTHENTICATED |
| permission_denied | PERMISSION_DENIED |
| not_found | NOT_FOUND |
| conflict | ALREADY_EXISTS or FAILED_PRECONDITION |
| resource_exhausted | RESOURCE_EXHAUSTED |
| deadline_exceeded | DEADLINE_EXCEEDED |
| unavailable | UNAVAILABLE |
| internal | INTERNAL |

Services must not expose raw dependency errors over RPC boundaries.

## 5. Deadlines and Cancellation

Every outbound gRPC call must have a finite deadline.

Services must propagate caller cancellation and must not extend an
incoming deadline.

An internal service may apply a shorter deadline when required by its
own latency budget.

Long-running job execution is not represented by keeping a request RPC
open for the duration of the job.

Job execution timeout and RPC deadlines are separate concepts.

REST handlers must propagate request cancellation through
`context.Context`.

Exact per-RPC deadline values are defined when the corresponding RPC is
introduced and must reflect the operation's expected latency.

## 6. Retries

RPC retries are allowed only when the operation is known to be
idempotent or carries an explicit idempotency mechanism.

Clients must not blindly retry mutating operations.

Retryable transport failures may include:

- UNAVAILABLE
- selected transient connection failures

DEADLINE_EXCEEDED must not automatically be interpreted as proof that
the server did not perform the operation.

Retries must be bounded and use backoff.

Operation-specific retry behavior is defined with the operation.

## 7. Protobuf Compatibility

The following rules apply to all protobuf schemas:

1. Never change the numeric tag of an existing field.
2. Never reuse a deleted field number.
3. Never reuse a deleted field name.
4. Removed fields must have both their number and name reserved.
5. New fields should normally be optional from a compatibility
   perspective.
6. Do not change a field to an incompatible wire type.
7. Do not change the semantic meaning of an existing field.
8. New enum values may be added, so consumers must tolerate unknown
   values.
9. Enum zero values must represent an unspecified/unknown state rather
   than a meaningful business state.
10. Breaking changes require a new versioned protobuf package.
11. Message fields should use explicit units in names where ambiguity is
    possible, for example `timeout_seconds`.
12. Timestamps should use `google.protobuf.Timestamp` rather than custom
    timestamp strings.

Generated protobuf code must not be manually edited.

## 8. Pagination

Collection APIs use opaque cursor-based pagination.

Clients must not inspect or construct cursor values.

Concrete cursor encoding and persistence ordering are deferred until
TASK-018.

## 9. Idempotency

Request IDs and idempotency keys have different purposes.

- request ID: tracing and correlation
- idempotency key: logical operation deduplication

Job-submission idempotency semantics are implemented in TASK-012 and
exposed through the REST API in TASK-014.

## 10. Compatibility Principle

Changes should be additive whenever possible.

Servers may be deployed independently during rolling upgrades, so a
newer server must not assume that every caller or peer has upgraded at
the same time.