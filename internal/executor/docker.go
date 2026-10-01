package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
)

// DockerExecutor executes assignments using Docker Engine.
type DockerExecutor struct {
	client *client.Client
}

// NewDockerExecutor connects to Docker using the standard Docker
// environment variables and API-version negotiation.
func NewDockerExecutor() (*DockerExecutor, error) {
	dockerClient, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"create Docker client: %w",
			err,
		)
	}

	return &DockerExecutor{
		client: dockerClient,
	}, nil
}

// Close releases resources owned by the Docker client.
func (e *DockerExecutor) Close() error {
	if e == nil || e.client == nil {
		return nil
	}

	return e.client.Close()
}

func (e *DockerExecutor) pullImage(
	ctx context.Context,
	imageReference string,
) error {
	if imageReference == "" {
		return errors.New(
			"container image must not be empty",
		)
	}

	reader, err := e.client.ImagePull(
		ctx,
		imageReference,
		image.PullOptions{},
	)
	if err != nil {
		return fmt.Errorf(
			"pull image %q: %w",
			imageReference,
			err,
		)
	}
	defer func() {
		_ = reader.Close()
	}()

	// Docker's pull stream must be consumed to completion. ImagePull
	// returning successfully only means the pull request was accepted.
	if _, err := io.Copy(
		io.Discard,
		reader,
	); err != nil {
		return fmt.Errorf(
			"consume image pull response for %q: %w",
			imageReference,
			err,
		)
	}

	return nil
}

// Execute pulls the requested image and runs the assignment in its own
// Docker container with the configured CPU and memory limits.
func (e *DockerExecutor) Execute(
	ctx context.Context,
	assignment Assignment,
) error {
	if assignment.JobID == "" {
		return errors.New(
			"job ID must not be empty",
		)
	}

	if assignment.AttemptID == "" {
		return errors.New(
			"attempt ID must not be empty",
		)
	}

	if err := e.pullImage(
		ctx,
		assignment.ContainerImage,
	); err != nil {
		return err
	}

	config := &container.Config{
		Image: assignment.ContainerImage,
		Env: environmentSlice(
			assignment.Environment,
		),
	}

	if len(assignment.Command) != 0 {
		config.Entrypoint = append(
			[]string(nil),
			assignment.Command...,
		)
	}

	if len(assignment.Args) != 0 {
		config.Cmd = append(
			[]string(nil),
			assignment.Args...,
		)
	}

	hostConfig, err := hostConfigFor(
		assignment,
	)
	if err != nil {
		return err
	}

	created, err := e.client.ContainerCreate(
		ctx,
		config,
		hostConfig,
		nil,
		nil,
		containerName(assignment),
	)
	if err != nil {
		return fmt.Errorf(
			"create container for attempt %q: %w",
			assignment.AttemptID,
			err,
		)
	}

	containerID := created.ID

	defer func() {
		cleanupCtx := context.WithoutCancel(ctx)

		_ = e.client.ContainerRemove(
			cleanupCtx,
			containerID,
			container.RemoveOptions{
				Force:         true,
				RemoveVolumes: true,
			},
		)
	}()

	if err := e.client.ContainerStart(
		ctx,
		containerID,
		container.StartOptions{},
	); err != nil {
		return fmt.Errorf(
			"start container for attempt %q: %w",
			assignment.AttemptID,
			err,
		)
	}

	waitResult, waitErr := e.client.ContainerWait(
		ctx,
		containerID,
		container.WaitConditionNotRunning,
	)

	select {
	case err := <-waitErr:
		if err != nil {
			return fmt.Errorf(
				"wait for attempt %q container: %w",
				assignment.AttemptID,
				err,
			)
		}

	case result := <-waitResult:
		if result.Error != nil {
			return fmt.Errorf(
				"container for attempt %q failed: %s",
				assignment.AttemptID,
				result.Error.Message,
			)
		}

		if result.StatusCode != 0 {
			return fmt.Errorf(
				"container for attempt %q exited with status %d",
				assignment.AttemptID,
				result.StatusCode,
			)
		}

	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func hostConfigFor(
	assignment Assignment,
) (*container.HostConfig, error) {
	hostConfig := &container.HostConfig{
		AutoRemove: false,

		// No privileged execution in V1.
		Privileged: false,

		Resources: container.Resources{},
	}

	if assignment.CPULimitMillis != nil {
		cpuMillis := *assignment.CPULimitMillis

		if cpuMillis <= 0 {
			return nil, fmt.Errorf(
				"CPU limit must be greater than zero",
			)
		}

		const period int64 = 100000

		hostConfig.CPUPeriod = period
		hostConfig.CPUQuota =
			(cpuMillis * period) / 1000
	}

	if assignment.MemoryLimitBytes != nil {
		memoryBytes := *assignment.MemoryLimitBytes

		if memoryBytes <= 0 {
			return nil, fmt.Errorf(
				"memory limit must be greater than zero",
			)
		}

		hostConfig.Memory =
			memoryBytes
	}

	return hostConfig, nil
}

func environmentSlice(
	environment map[string]string,
) []string {
	if len(environment) == 0 {
		return nil
	}

	keys := make(
		[]string,
		0,
		len(environment),
	)

	for key := range environment {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	result := make(
		[]string,
		0,
		len(keys),
	)

	for _, key := range keys {
		result = append(
			result,
			key+"="+environment[key],
		)
	}

	return result
}

func containerName(
	assignment Assignment,
) string {
	return fmt.Sprintf(
		"djs-%s-%s",
		sanitizeContainerName(assignment.JobID),
		sanitizeContainerName(assignment.AttemptID),
	)
}

func sanitizeContainerName(
	value string,
) string {
	var builder strings.Builder

	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)

		case r >= 'A' && r <= 'Z':
			builder.WriteRune(r)

		case r >= '0' && r <= '9':
			builder.WriteRune(r)

		case r == '-', r == '_', r == '.':
			builder.WriteRune(r)

		default:
			builder.WriteByte('-')
		}
	}

	return builder.String()
}
