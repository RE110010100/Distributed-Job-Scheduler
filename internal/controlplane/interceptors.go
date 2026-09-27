package controlplane

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	authorizationMetadata = "authorization"
	requestIDMetadata     = "x-request-id"
)

type requestIDContextKey struct{}

// UnaryAuthenticationInterceptor authenticates worker RPCs.
func UnaryAuthenticationInterceptor(
	expectedToken string,
) grpc.UnaryServerInterceptor {
	expectedToken = strings.TrimSpace(expectedToken)

	return func(
		ctx context.Context,
		req any,
		_ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		token, ok := bearerToken(ctx)
		if !ok ||
			!constantTimeEqual(token, expectedToken) {
			return nil, status.Error(
				codes.Unauthenticated,
				"worker authentication required",
			)
		}

		return handler(ctx, req)
	}
}

// UnaryDeadlineInterceptor supplies a bounded server deadline when the
// caller does not provide a stricter one.
func UnaryDeadlineInterceptor(
	timeout time.Duration,
) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		_ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if _, ok := ctx.Deadline(); ok {
			return handler(ctx, req)
		}

		boundedCtx, cancel := context.WithTimeout(
			ctx,
			timeout,
		)
		defer cancel()

		return handler(boundedCtx, req)
	}
}

// UnaryObservabilityInterceptor records request correlation, duration,
// method, and final gRPC status.
func UnaryObservabilityInterceptor(
	logger *log.Logger,
) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		requestID := requestIDFromMetadata(ctx)

		ctx = context.WithValue(
			ctx,
			requestIDContextKey{},
			requestID,
		)

		started := time.Now()

		response, err := handler(
			ctx,
			req,
		)

		logger.Printf(
			"grpc_request request_id=%s method=%s code=%s duration=%s",
			requestID,
			info.FullMethod,
			status.Code(err),
			time.Since(started),
		)

		return response, err
	}
}

func bearerToken(
	ctx context.Context,
) (string, bool) {
	values := metadata.ValueFromIncomingContext(
		ctx,
		authorizationMetadata,
	)

	if len(values) != 1 {
		return "", false
	}

	const prefix = "Bearer "

	if !strings.HasPrefix(values[0], prefix) {
		return "", false
	}

	token := strings.TrimSpace(
		strings.TrimPrefix(values[0], prefix),
	)

	return token, token != ""
}

func constantTimeEqual(
	provided string,
	expected string,
) bool {
	if expected == "" {
		return false
	}

	providedHash := sha256.Sum256(
		[]byte(provided),
	)
	expectedHash := sha256.Sum256(
		[]byte(expected),
	)

	return subtle.ConstantTimeCompare(
		providedHash[:],
		expectedHash[:],
	) == 1
}

func requestIDFromMetadata(
	ctx context.Context,
) string {
	values := metadata.ValueFromIncomingContext(
		ctx,
		requestIDMetadata,
	)

	if len(values) == 1 {
		value := strings.TrimSpace(values[0])
		if value != "" {
			return value
		}
	}

	return "unknown"
}
