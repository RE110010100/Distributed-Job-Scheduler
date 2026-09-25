package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/apierrors"
)

type ownerContextKey struct{}

// ServiceCredential identifies one trusted internal service.
type ServiceCredential struct {
	OwnerID string
	Token   string
}

// Authenticator authenticates internal HTTP callers.
type Authenticator struct {
	credentials []ServiceCredential
}

// NewAuthenticator creates an internal-service authenticator.
func NewAuthenticator(
	credentials []ServiceCredential,
) (*Authenticator, error) {
	if len(credentials) == 0 {
		return nil, errors.New(
			"at least one internal service credential is required",
		)
	}

	copied := make([]ServiceCredential, 0, len(credentials))

	for _, credential := range credentials {
		credential.OwnerID = strings.TrimSpace(credential.OwnerID)
		credential.Token = strings.TrimSpace(credential.Token)

		if credential.OwnerID == "" {
			return nil, errors.New(
				"internal service owner ID must not be empty",
			)
		}

		if credential.Token == "" {
			return nil, errors.New(
				"internal service token must not be empty",
			)
		}

		copied = append(copied, credential)
	}

	return &Authenticator{
		credentials: copied,
	}, nil
}

func (a *Authenticator) authenticate(
	token string,
) (string, bool) {
	provided := sha256.Sum256([]byte(token))

	for _, credential := range a.credentials {
		expected := sha256.Sum256(
			[]byte(credential.Token),
		)

		if subtle.ConstantTimeCompare(
			provided[:],
			expected[:],
		) == 1 {
			return credential.OwnerID, true
		}
	}

	return "", false
}

func (s *Server) withAuthentication(
	next http.Handler,
) http.Handler {
	return http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		header := r.Header.Get("Authorization")

		const prefix = "Bearer "

		if !strings.HasPrefix(header, prefix) {
			writeAPIError(
				w,
				r,
				http.StatusUnauthorized,
				apierrors.CodeUnauthenticated,
				"authentication required",
			)
			return
		}

		token := strings.TrimSpace(
			strings.TrimPrefix(header, prefix),
		)

		ownerID, ok := s.auth.authenticate(token)
		if !ok {
			writeAPIError(
				w,
				r,
				http.StatusUnauthorized,
				apierrors.CodeUnauthenticated,
				"invalid authentication credentials",
			)
			return
		}

		ctx := context.WithValue(
			r.Context(),
			ownerContextKey{},
			ownerID,
		)

		next.ServeHTTP(
			w,
			r.WithContext(ctx),
		)
	})
}

func ownerIDFromContext(
	ctx context.Context,
) (string, bool) {
	ownerID, ok := ctx.Value(ownerContextKey{}).(string)

	return ownerID, ok && ownerID != ""
}
