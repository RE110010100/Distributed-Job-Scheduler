package api

import (
	"errors"
	"fmt"
	"strings"
)

// ParseServiceCredentials parses comma-separated owner:token pairs.
func ParseServiceCredentials(
	value string,
) ([]ServiceCredential, error) {
	value = strings.TrimSpace(value)

	if value == "" {
		return nil, errors.New(
			"internal API credentials must not be empty",
		)
	}

	entries := strings.Split(value, ",")

	credentials := make(
		[]ServiceCredential,
		0,
		len(entries),
	)

	owners := make(map[string]struct{})

	for _, entry := range entries {
		parts := strings.SplitN(entry, ":", 2)

		if len(parts) != 2 {
			return nil, fmt.Errorf(
				"invalid internal API credential entry",
			)
		}

		ownerID := strings.TrimSpace(parts[0])
		token := strings.TrimSpace(parts[1])

		if ownerID == "" || token == "" {
			return nil, errors.New(
				"internal API credential owner and token must not be empty",
			)
		}

		if _, exists := owners[ownerID]; exists {
			return nil, fmt.Errorf(
				"duplicate internal API owner %q",
				ownerID,
			)
		}

		owners[ownerID] = struct{}{}

		credentials = append(
			credentials,
			ServiceCredential{
				OwnerID: ownerID,
				Token:   token,
			},
		)
	}

	return credentials, nil
}
