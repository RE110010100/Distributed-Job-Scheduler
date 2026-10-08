package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

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
) (result ExecutionResult, err error) {
	result = ExecutionResult{
		JobID:     assignment.JobID,
		AttemptID: assignment.AttemptID,
	}

	defer func() {
		result.CompletedAt = time.Now().UTC()
	}()

	if assignment.JobID == "" || assignment.AttemptID == "" {
		result.Outcome = OutcomeFailed
		result.FailureCode = FailureInvalidSpec
		result.Message = "job ID and attempt ID are required"
		return result, nil
	}

	execCtx, cancel, contextErr := executionContext(
		ctx,
		assignment.Timeout,
	)
	if contextErr != nil {
		result.Outcome = OutcomeFailed
		result.FailureCode = FailureInvalidSpec
		result.Message = contextErr.Error()
		return result, nil
	}
	defer cancel()

	setContextOutcome := func() {
		if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
			result.Outcome = OutcomeTimedOut
			result.FailureCode = FailureTimeout
			result.Message = "execution deadline exceeded"
			return
		}

		result.Outcome = OutcomeCancelled
		result.FailureCode = FailureCancelled
		result.Message = "execution context cancelled"
	}

	if pullErr := e.pullImage(
		execCtx,
		assignment.ContainerImage,
	); pullErr != nil {
		if execCtx.Err() != nil {
			setContextOutcome()
			return result, nil
		}

		result.Outcome = OutcomeInfrastructure
		result.FailureCode = FailureImagePull
		result.Message = pullErr.Error()
		return result, pullErr
	}

	config := &container.Config{
		Image: assignment.ContainerImage,
		Env:   environmentSlice(assignment.Environment),
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

	hostConfig, configErr := hostConfigFor(assignment)
	if configErr != nil {
		result.Outcome = OutcomeFailed
		result.FailureCode = FailureInvalidSpec
		result.Message = configErr.Error()
		return result, nil
	}

	created, createErr := e.client.ContainerCreate(
		execCtx,
		config,
		hostConfig,
		nil,
		nil,
		containerName(assignment),
	)
	if createErr != nil {
		if execCtx.Err() != nil {
			setContextOutcome()
			return result, nil
		}

		result.Outcome = OutcomeInfrastructure
		result.FailureCode = FailureContainerCreate
		result.Message = createErr.Error()
		return result, createErr
	}

	containerID := created.ID
	result.ContainerID = containerID

	defer func() {
		if cleanupErr := e.removeContainer(containerID); cleanupErr != nil {
			// Do not erase the execution outcome. Surface cleanup
			// failure separately through the returned error.
			err = errors.Join(
				err,
				fmt.Errorf(
					"remove container %q: %w",
					containerID,
					cleanupErr,
				),
			)
		}
	}()

	if startErr := e.client.ContainerStart(
		execCtx,
		containerID,
		container.StartOptions{},
	); startErr != nil {
		if execCtx.Err() != nil {
			setContextOutcome()
			return result, nil
		}

		result.Outcome = OutcomeInfrastructure
		result.FailureCode = FailureContainerStart
		result.Message = startErr.Error()
		return result, startErr
	}

	result.StartedAt = time.Now().UTC()

	waitResults, waitErrors := e.client.ContainerWait(
		execCtx,
		containerID,
		container.WaitConditionNotRunning,
	)

	select {
	case waitErr, ok := <-waitErrors:
		if !ok {
			waitErr = errors.New("container wait error channel closed")
		}
		if waitErr != nil {
			if execCtx.Err() != nil {
				setContextOutcome()
				if stopErr := e.terminateContainer(containerID); stopErr != nil {
					err = errors.Join(err, stopErr)
				}
				return result, err
			}

			result.Outcome = OutcomeInfrastructure
			result.FailureCode = FailureContainerWait
			result.Message = waitErr.Error()

			// An uncertain wait result is not evidence that the
			// container stopped. Terminate before cleanup.
			stopErr := e.terminateContainer(containerID)
			return result, errors.Join(waitErr, stopErr)
		}

	case waitResult, ok := <-waitResults:
		if !ok {
			waitErr := errors.New("container wait result channel closed")
			result.Outcome = OutcomeInfrastructure
			result.FailureCode = FailureContainerWait
			result.Message = waitErr.Error()
			return result, errors.Join(
				waitErr,
				e.terminateContainer(containerID),
			)
		}

		if waitResult.Error != nil {
			waitErr := errors.New(waitResult.Error.Message)
			result.Outcome = OutcomeInfrastructure
			result.FailureCode = FailureContainerWait
			result.Message = waitErr.Error()
			return result, errors.Join(
				waitErr,
				e.terminateContainer(containerID),
			)
		}

		exitCode := waitResult.StatusCode
		result.ExitCode = &exitCode

	case <-execCtx.Done():
		setContextOutcome()

		if stopErr := e.terminateContainer(containerID); stopErr != nil {
			err = errors.Join(err, stopErr)
		}

		return result, err
	}

	inspectCtx, inspectCancel := context.WithTimeout(
		context.Background(),
		cleanupTimeout,
	)
	defer inspectCancel()

	inspected, inspectErr := e.client.ContainerInspect(
		inspectCtx,
		containerID,
	)
	if inspectErr != nil {
		result.Outcome = OutcomeInfrastructure
		result.FailureCode = FailureContainerWait
		result.Message = fmt.Sprintf(
			"inspect container outcome: %v",
			inspectErr,
		)
		return result, inspectErr
	}

	if inspected.State == nil {
		inspectErr := errors.New("container state is unavailable")
		result.Outcome = OutcomeInfrastructure
		result.FailureCode = FailureContainerWait
		result.Message = inspectErr.Error()
		return result, inspectErr
	}

	result.OOMKilled = inspected.State.OOMKilled

	if result.ExitCode == nil {
		exitCode := int64(inspected.State.ExitCode)
		result.ExitCode = &exitCode
	}

	result.Outcome, result.FailureCode, result.Message =
		classifyExit(*result.ExitCode, result.OOMKilled)

	return result, nil
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

const (
	defaultExecutionTimeout = 30 * time.Minute
	stopGracePeriod         = 5 * time.Second
	terminationTimeout      = 15 * time.Second
	cleanupTimeout          = 10 * time.Second
)

func executionContext(
	parent context.Context,
	timeout time.Duration,
) (context.Context, context.CancelFunc, error) {
	if timeout < 0 {
		return nil, nil, fmt.Errorf("execution timeout cannot be negative")
	}

	if timeout == 0 {
		timeout = defaultExecutionTimeout
	}

	ctx, cancel := context.WithTimeout(parent, timeout)
	return ctx, cancel, nil
}

func classifyExit(
	exitCode int64,
	oomKilled bool,
) (Outcome, FailureCode, string) {
	switch {
	case oomKilled:
		return OutcomeResourceExceeded,
			FailureOOMKilled,
			"container was killed by the out-of-memory controller"

	case exitCode != 0:
		return OutcomeFailed,
			FailureNonzeroExit,
			fmt.Sprintf("container exited with status %d", exitCode)

	default:
		return OutcomeSucceeded, FailureNone, ""
	}
}

func (e *DockerExecutor) terminateContainer(
	containerID string,
) error {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		terminationTimeout,
	)
	defer cancel()

	graceSeconds := int(stopGracePeriod.Seconds())

	if err := e.client.ContainerStop(
		ctx,
		containerID,
		container.StopOptions{
			Timeout: &graceSeconds,
		},
	); err == nil {
		return nil
	}

	if err := e.client.ContainerKill(
		ctx,
		containerID,
		"SIGKILL",
	); err != nil {
		return fmt.Errorf(
			"force-kill container %q: %w",
			containerID,
			err,
		)
	}

	return nil
}

func (e *DockerExecutor) removeContainer(
	containerID string,
) error {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		cleanupTimeout,
	)
	defer cancel()

	return e.client.ContainerRemove(
		ctx,
		containerID,
		container.RemoveOptions{
			Force:         true,
			RemoveVolumes: true,
		},
	)
}
