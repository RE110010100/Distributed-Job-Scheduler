GO ?= go

.PHONY: \
	build \
	test \
	test-race \
	vet \
	fmt \
	lint \
	ci \
	proto \
	run-api \
	run-scheduler \
	run-worker \
	dev-up \
	dev-down

build:
	$(GO) build ./...

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

run-api:
	$(GO) run ./cmd/api

run-scheduler:
	$(GO) run ./cmd/scheduler

run-worker:
	$(GO) run ./cmd/worker

dev-up:
	docker compose up --build

dev-down:
	docker compose down --remove-orphans

lint:
	golangci-lint run

ci: build test test-race vet lint

proto:
	protoc \
		--go_out=. \
		--go_opt=module=github.com/RE110010100/Distributed-Job-Scheduler \
		--go-grpc_out=. \
		--go-grpc_opt=module=github.com/RE110010100/Distributed-Job-Scheduler \
		proto/v1/worker_control.proto