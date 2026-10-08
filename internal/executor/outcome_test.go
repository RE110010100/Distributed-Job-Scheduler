package executor

import (
	"context"
	"testing"
	"time"
)

func TestClassifyExit(t *testing.T) {
	tests := []struct {
		name     string
		exitCode int64
		oom      bool
		outcome  Outcome
		code     FailureCode
	}{
		{
			name:     "success",
			exitCode: 0,
			outcome:  OutcomeSucceeded,
			code:     FailureNone,
		},
		{
			name:     "nonzero exit",
			exitCode: 2,
			outcome:  OutcomeFailed,
			code:     FailureNonzeroExit,
		},
		{
			name:     "OOM killed",
			exitCode: 137,
			oom:      true,
			outcome:  OutcomeResourceExceeded,
			code:     FailureOOMKilled,
		},
		{
			name:     "137 without OOM flag",
			exitCode: 137,
			outcome:  OutcomeFailed,
			code:     FailureNonzeroExit,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outcome, code, _ := classifyExit(
				tt.exitCode,
				tt.oom,
			)

			if outcome != tt.outcome {
				t.Fatalf(
					"outcome: got %q, want %q",
					outcome,
					tt.outcome,
				)
			}

			if code != tt.code {
				t.Fatalf(
					"failure code: got %q, want %q",
					code,
					tt.code,
				)
			}
		})
	}
}

func TestExecutionContextDefaultTimeout(t *testing.T) {
	ctx, cancel, err := executionContext(
		context.Background(),
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected finite execution deadline")
	}

	remaining := time.Until(deadline)

	if remaining <= 0 || remaining > defaultExecutionTimeout {
		t.Fatalf("unexpected remaining timeout: %v", remaining)
	}
}

func TestExecutionContextRejectsNegativeTimeout(t *testing.T) {
	_, _, err := executionContext(
		context.Background(),
		-time.Second,
	)
	if err == nil {
		t.Fatal("expected invalid timeout error")
	}
}
