package clickhouse

import (
	"sync"
	"sync/atomic"

	"go.k6.io/k6/metrics"
)

// DropPolicy determines which samples to drop when the buffer overflows.
type DropPolicy string

const (
	// DropOldest removes oldest samples to make room for new ones.
	// This preserves the most recent test data during extended outages.
	DropOldest DropPolicy = "oldest"

	// DropNewest rejects new samples when the buffer is full.
	// This preserves historical data from the start of the outage.
	DropNewest DropPolicy = "newest"
)

// SampleBuffer is a thread-safe ring buffer for storing metric samples
// during ClickHouse connection failures. It supports configurable overflow
// policies and provides metrics for monitoring buffer state.
type SampleBuffer struct {
	mu       sync.Mutex
	items    []metrics.SampleContainer
	head     int // Index of the oldest item
	tail     int // Index where next item will be inserted
	count    int // Current number of items
	capacity int
	policy   DropPolicy

	// Metrics (atomic for lock-free reads)
	dropped atomic.Uint64 // Total samples dropped due to overflow
}

// NewSampleBuffer creates a new ring buffer with the specified capacity and overflow policy.
// If capacity is <= 0, it defaults to 10000.
// If policy is invalid, it defaults to DropOldest.
func NewSampleBuffer(capacity int, policy DropPolicy) *SampleBuffer {
	if capacity <= 0 {
		capacity = 10000
	}

	if policy != DropOldest && policy != DropNewest {
		policy = DropOldest
	}

	return &SampleBuffer{
		items:    make([]metrics.SampleContainer, capacity),
		capacity: capacity,
		policy:   policy,
	}
}

// Push adds sample containers to the buffer.
// Returns the number of samples dropped due to overflow.
// Thread-safe.
func (b *SampleBuffer) Push(samples []metrics.SampleContainer) int {
	if len(samples) == 0 {
		return 0
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	dropped := 0

	for _, sample := range samples {
		if b.count >= b.capacity {
			// Buffer is full - apply overflow policy
			switch b.policy {
			case DropOldest:
				// Remove oldest item to make room
				b.items[b.head] = nil // Help GC
				b.head = (b.head + 1) % b.capacity
				b.count--
				dropped++
			case DropNewest:
				// Reject new sample
				dropped++
				continue
			}
		}

		// Add new sample
		b.items[b.tail] = sample
		b.tail = (b.tail + 1) % b.capacity
		b.count++
	}

	if dropped > 0 {
		b.dropped.Add(uint64(dropped))
	}

	return dropped
}

// PopAll removes and returns all samples from the buffer in FIFO order.
// Returns nil if the buffer is empty.
// Thread-safe.
func (b *SampleBuffer) PopAll() []metrics.SampleContainer {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.count == 0 {
		return nil
	}

	// Extract all items in FIFO order
	result := make([]metrics.SampleContainer, b.count)
	for i := 0; i < b.count; i++ {
		idx := (b.head + i) % b.capacity
		result[i] = b.items[idx]
		b.items[idx] = nil // Help GC
	}

	// Reset buffer state
	b.head = 0
	b.tail = 0
	b.count = 0

	return result
}

// Len returns the current number of items in the buffer.
// Thread-safe.
func (b *SampleBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.count
}

// Capacity returns the maximum capacity of the buffer.
func (b *SampleBuffer) Capacity() int {
	return b.capacity
}

// DroppedCount returns the total number of samples dropped due to overflow.
// Thread-safe and lock-free.
func (b *SampleBuffer) DroppedCount() uint64 {
	return b.dropped.Load()
}

// Reset clears the buffer and resets all counters.
// Thread-safe.
func (b *SampleBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i := range b.items {
		b.items[i] = nil
	}
	b.head = 0
	b.tail = 0
	b.count = 0
	b.dropped.Store(0)
}
