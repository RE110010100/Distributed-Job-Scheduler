package executor

import "testing"

func TestHostConfigAppliesCPUAndMemoryLimits(
	t *testing.T,
) {
	cpu := int64(500)
	memory := int64(512 << 20)

	config, err := hostConfigFor(
		Assignment{
			CPULimitMillis:   &cpu,
			MemoryLimitBytes: &memory,
		},
	)
	if err != nil {
		t.Fatalf(
			"host config: %v",
			err,
		)
	}

	if config.CPUPeriod != 100000 {
		t.Fatalf(
			"CPU period: got=%d want=%d",
			config.CPUPeriod,
			100000,
		)
	}

	if config.CPUQuota != 50000 {
		t.Fatalf(
			"CPU quota: got=%d want=%d",
			config.CPUQuota,
			50000,
		)
	}

	if config.Memory != memory {
		t.Fatalf(
			"memory: got=%d want=%d",
			config.Memory,
			memory,
		)
	}
}

func TestHostConfigWithoutLimits(
	t *testing.T,
) {
	config, err := hostConfigFor(
		Assignment{},
	)
	if err != nil {
		t.Fatalf(
			"host config: %v",
			err,
		)
	}

	if config.CPUQuota != 0 {
		t.Fatalf(
			"unexpected CPU quota: %d",
			config.CPUQuota,
		)
	}

	if config.Memory != 0 {
		t.Fatalf(
			"unexpected memory limit: %d",
			config.Memory,
		)
	}
}

func TestEnvironmentSliceIsDeterministic(
	t *testing.T,
) {
	got := environmentSlice(
		map[string]string{
			"Z": "3",
			"A": "1",
			"M": "2",
		},
	)

	want := []string{
		"A=1",
		"M=2",
		"Z=3",
	}

	for index := range want {
		if got[index] != want[index] {
			t.Fatalf(
				"environment[%d]: got=%q want=%q",
				index,
				got[index],
				want[index],
			)
		}
	}
}
