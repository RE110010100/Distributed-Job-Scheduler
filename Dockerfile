# syntax=docker/dockerfile:1

ARG GO_VERSION=1.27.0
FROM golang:${GO_VERSION}-bookworm AS build

WORKDIR /src

COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal

ARG SERVICE

RUN test -n "${SERVICE}" \
    && CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -o /out/service \
        "./cmd/${SERVICE}"

FROM scratch

USER 65532:65532

COPY --from=build /out/service /service

ENTRYPOINT ["/service"]