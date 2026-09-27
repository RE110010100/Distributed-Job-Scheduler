// Package worker defines worker-node state shared by the control plane
// and persistence layers.
package worker

import "time"

// ID uniquely identifies a worker across registrations and heartbeats.
type ID string

// Capacity describes CPU and memory capacity in V1 scheduling units.
type Capacity struct {
	CPUMillis   int64
	MemoryBytes int64
}

// Valid reports whether capacity contains usable positive values.
func (c Capacity) Valid() bool {
	return c.CPUMillis > 0 && c.MemoryBytes > 0
}

// Fits reports whether required capacity can be satisfied.
func (c Capacity) Fits(required Capacity) bool {
	return c.CPUMillis >= required.CPUMillis &&
		c.MemoryBytes >= required.MemoryBytes
}

// Worker contains durable control-plane state for a worker.
type Worker struct {
	ID                ID
	Capacity          Capacity
	AvailableCapacity Capacity
	ContainerRuntimes []string
	RegisteredAt      time.Time
	LastHeartbeatAt   time.Time
}
