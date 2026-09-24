package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"

	"github.com/RE110010100/Distributed-Job-Scheduler/internal/apierrors"
	"github.com/RE110010100/Distributed-Job-Scheduler/internal/requestid"
)

type requestIDContextKey struct{}

type errorEnvelope struct {
	Error errorResponse `json:"error"`
}

type errorResponse struct {
	Code      apierrors.Code `json:"code"`
	Message   string         `json:"message"`
	RequestID string         `json:"request_id"`
}

func (s *Server) withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		id := r.Header.Get(requestid.HTTPHeader)

		if !requestid.Valid(id) {
			var err error
			id, err = newRequestID()
			if err != nil {
				log.Printf("generate request ID: %v", err)
				http.Error(
					w,
					"internal server error",
					http.StatusInternalServerError,
				)
				return
			}
		}

		w.Header().Set(requestid.HTTPHeader, id)

		ctx := context.WithValue(
			r.Context(),
			requestIDContextKey{},
			id,
		)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func newRequestID() (string, error) {
	var value [16]byte

	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}

	return hex.EncodeToString(value[:]), nil
}

func requestIDFromContext(ctx context.Context) string {
	value, _ := ctx.Value(requestIDContextKey{}).(string)
	return value
}

func writeJSON(
	w http.ResponseWriter,
	status int,
	value any,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("encode HTTP response: %v", err)
	}
}

func writeAPIError(
	w http.ResponseWriter,
	r *http.Request,
	status int,
	code apierrors.Code,
	message string,
) {
	writeJSON(
		w,
		status,
		errorEnvelope{
			Error: errorResponse{
				Code:      code,
				Message:   message,
				RequestID: requestIDFromContext(r.Context()),
			},
		},
	)
}
