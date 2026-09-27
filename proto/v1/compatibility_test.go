package proto_test

import (
	"testing"

	workerv1 "github.com/RE110010100/Distributed-Job-Scheduler/internal/gen/worker/v1"
	"google.golang.org/protobuf/proto"
)

func TestRegisterWorkerRequestWireRoundTrip(
	t *testing.T,
) {
	original := &workerv1.RegisterWorkerRequest{
		WorkerId: "worker-1",
		Capacity: &workerv1.WorkerCapacity{
			CpuMillis:   4000,
			MemoryBytes: 8 << 30,
		},
		Capabilities: &workerv1.WorkerCapabilities{
			ContainerRuntimes: []string{
				"docker",
			},
		},
	}

	encoded, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	var decoded workerv1.RegisterWorkerRequest

	if err := proto.Unmarshal(
		encoded,
		&decoded,
	); err != nil {
		t.Fatalf("unmarshal request: %v", err)
	}

	if !proto.Equal(original, &decoded) {
		t.Fatalf(
			"wire round trip changed message: got=%v want=%v",
			&decoded,
			original,
		)
	}
}
