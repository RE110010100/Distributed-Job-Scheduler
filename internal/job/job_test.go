package job

import "testing"

func TestRetryPolicyEffectiveMaxAttemptsUsesDefault(t *testing.T) {
	policy := RetryPolicy{}

	if got := policy.EffectiveMaxAttempts(); got != DefaultMaxAttempts {
		t.Fatalf(
			"EffectiveMaxAttempts() = %d, want %d",
			got,
			DefaultMaxAttempts,
		)
	}
}

func TestRetryPolicyEffectiveMaxAttemptsUsesConfiguredValue(t *testing.T) {
	maxAttempts := uint32(5)

	policy := RetryPolicy{
		MaxAttempts: &maxAttempts,
	}

	if got := policy.EffectiveMaxAttempts(); got != maxAttempts {
		t.Fatalf(
			"EffectiveMaxAttempts() = %d, want %d",
			got,
			maxAttempts,
		)
	}
}
