package worker

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/credentials"
)

type tokenCredentials struct {
	token string
}

var _ credentials.PerRPCCredentials = tokenCredentials{}

func newTokenCredentials(
	token string,
) (credentials.PerRPCCredentials, error) {
	token = strings.TrimSpace(token)

	if token == "" {
		return nil, fmt.Errorf(
			"worker authentication token must not be empty",
		)
	}

	return tokenCredentials{
		token: token,
	}, nil
}

func (c tokenCredentials) GetRequestMetadata(
	_ context.Context,
	_ ...string,
) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + c.token,
	}, nil
}

func (tokenCredentials) RequireTransportSecurity() bool {
	// TASK-064 introduces encrypted transport.
	return false
}
