package clickhouse

import (
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/metrics"
)

// mockSampleContainer implements metrics.SampleContainer for testing
type mockSampleContainer struct {
	samples []metrics.Sample
}

func (m *mockSampleContainer) GetSamples() []metrics.Sample {
	return m.samples
}

func newMockContainer(id int) metrics.SampleContainer {
	return &mockSampleContainer{
		samples: []metrics.Sample{
			{Value: float64(id)},
		},
	}
}

func TestNewSampleBuffer(t *testing.T) {
	t.Run("default capacity", func(t *testing.T) {
		buf := NewSampleBuffer(0, DropOldest)
		assert.Equal(t, 10000, buf.Capacity())
	})

	t.Run("custom capacity", func(t *testing.T) {
		buf := NewSampleBuffer(100, DropOldest)
		assert.Equal(t, 100, buf.Capacity())
	})

	t.Run("default policy for invalid input", func(t *testing.T) {
		buf := NewSampleBuffer(100, "invalid")
		assert.Equal(t, DropOldest, buf.policy)
	})

	t.Run("drop oldest policy", func(t *testing.T) {
		buf := NewSampleBuffer(100, DropOldest)
		assert.Equal(t, DropOldest, buf.policy)
	})

	t.Run("drop newest policy", func(t *testing.T) {
		buf := NewSampleBuffer(100, DropNewest)
		assert.Equal(t, DropNewest, buf.policy)
	})
}

func TestSampleBuffer_PushPop(t *testing.T) {
	t.Run("push and pop single item", func(t *testing.T) {
		buf := NewSampleBuffer(10, DropOldest)

		samples := []metrics.SampleContainer{newMockContainer(1)}
		dropped := buf.Push(samples)

		assert.Equal(t, 0, dropped)
		assert.Equal(t, 1, buf.Len())

		result := buf.PopAll()
		assert.Len(t, result, 1)
		assert.Equal(t, 0, buf.Len())
	})

	t.Run("push and pop multiple items", func(t *testing.T) {
		buf := NewSampleBuffer(10, DropOldest)

		samples := []metrics.SampleContainer{
			newMockContainer(1),
			newMockContainer(2),
			newMockContainer(3),
		}
		dropped := buf.Push(samples)

		assert.Equal(t, 0, dropped)
		assert.Equal(t, 3, buf.Len())

		result := buf.PopAll()
		assert.Len(t, result, 3)
		assert.Equal(t, 0, buf.Len())
	})

	t.Run("pop from empty buffer", func(t *testing.T) {
		buf := NewSampleBuffer(10, DropOldest)

		result := buf.PopAll()
		assert.Nil(t, result)
	})

	t.Run("push empty slice", func(t *testing.T) {
		buf := NewSampleBuffer(10, DropOldest)

		dropped := buf.Push(nil)
		assert.Equal(t, 0, dropped)
		assert.Equal(t, 0, buf.Len())

		dropped = buf.Push([]metrics.SampleContainer{})
		assert.Equal(t, 0, dropped)
		assert.Equal(t, 0, buf.Len())
	})

	t.Run("FIFO order preserved", func(t *testing.T) {
		buf := NewSampleBuffer(10, DropOldest)

		for i := 1; i <= 5; i++ {
			buf.Push([]metrics.SampleContainer{newMockContainer(i)})
		}

		result := buf.PopAll()
		require.Len(t, result, 5)

		// Verify FIFO order
		for i, container := range result {
			samples := container.GetSamples()
			assert.Equal(t, float64(i+1), samples[0].Value)
		}
	})
}

func TestSampleBuffer_OverflowDropOldest(t *testing.T) {
	buf := NewSampleBuffer(3, DropOldest)

	// Fill buffer
	buf.Push([]metrics.SampleContainer{
		newMockContainer(1),
		newMockContainer(2),
		newMockContainer(3),
	})
	assert.Equal(t, 3, buf.Len())

	// Push more, should drop oldest
	dropped := buf.Push([]metrics.SampleContainer{
		newMockContainer(4),
		newMockContainer(5),
	})

	assert.Equal(t, 2, dropped)
	assert.Equal(t, 3, buf.Len())
	assert.Equal(t, uint64(2), buf.DroppedCount())

	// Verify oldest were dropped (1, 2 gone; 3, 4, 5 remain)
	result := buf.PopAll()
	require.Len(t, result, 3)
	assert.Equal(t, float64(3), result[0].GetSamples()[0].Value)
	assert.Equal(t, float64(4), result[1].GetSamples()[0].Value)
	assert.Equal(t, float64(5), result[2].GetSamples()[0].Value)
}

func TestSampleBuffer_OverflowDropNewest(t *testing.T) {
	buf := NewSampleBuffer(3, DropNewest)

	// Fill buffer
	buf.Push([]metrics.SampleContainer{
		newMockContainer(1),
		newMockContainer(2),
		newMockContainer(3),
	})
	assert.Equal(t, 3, buf.Len())

	// Push more, should reject new items
	dropped := buf.Push([]metrics.SampleContainer{
		newMockContainer(4),
		newMockContainer(5),
	})

	assert.Equal(t, 2, dropped)
	assert.Equal(t, 3, buf.Len())
	assert.Equal(t, uint64(2), buf.DroppedCount())

	// Verify newest were dropped (1, 2, 3 remain; 4, 5 rejected)
	result := buf.PopAll()
	require.Len(t, result, 3)
	assert.Equal(t, float64(1), result[0].GetSamples()[0].Value)
	assert.Equal(t, float64(2), result[1].GetSamples()[0].Value)
	assert.Equal(t, float64(3), result[2].GetSamples()[0].Value)
}

func TestSampleBuffer_Reset(t *testing.T) {
	buf := NewSampleBuffer(10, DropOldest)

	buf.Push([]metrics.SampleContainer{
		newMockContainer(1),
		newMockContainer(2),
	})
	assert.Equal(t, 2, buf.Len())

	// Force some drops
	buf2 := NewSampleBuffer(1, DropOldest)
	buf2.Push([]metrics.SampleContainer{newMockContainer(1)})
	buf2.Push([]metrics.SampleContainer{newMockContainer(2)})
	assert.Equal(t, uint64(1), buf2.DroppedCount())

	buf2.Reset()
	assert.Equal(t, 0, buf2.Len())
	assert.Equal(t, uint64(0), buf2.DroppedCount())
}

func TestSampleBuffer_Concurrency(t *testing.T) {
	buf := NewSampleBuffer(1000, DropOldest)
	var wg sync.WaitGroup
	numGoroutines := 10
	itemsPerGoroutine := 100

	// Concurrent pushes
	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range itemsPerGoroutine {
				buf.Push([]metrics.SampleContainer{newMockContainer(id*1000 + j)})
			}
		}(i)
	}

	wg.Wait()

	// All items should be present (buffer large enough)
	assert.Equal(t, numGoroutines*itemsPerGoroutine, buf.Len())
	assert.Equal(t, uint64(0), buf.DroppedCount())

	// Pop all should return all items
	result := buf.PopAll()
	assert.Len(t, result, numGoroutines*itemsPerGoroutine)
	assert.Equal(t, 0, buf.Len())
}

func TestSampleBuffer_WrapAround(t *testing.T) {
	buf := NewSampleBuffer(3, DropOldest)

	// First fill
	buf.Push([]metrics.SampleContainer{
		newMockContainer(1),
		newMockContainer(2),
		newMockContainer(3),
	})

	// Pop all
	result := buf.PopAll()
	assert.Len(t, result, 3)

	// Second fill (tests wrap-around after reset)
	buf.Push([]metrics.SampleContainer{
		newMockContainer(4),
		newMockContainer(5),
	})

	result = buf.PopAll()
	require.Len(t, result, 2)
	assert.Equal(t, float64(4), result[0].GetSamples()[0].Value)
	assert.Equal(t, float64(5), result[1].GetSamples()[0].Value)
}

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "connection refused",
			err:      &mockError{msg: "dial tcp: connection refused"},
			expected: true,
		},
		{
			name:     "connection reset",
			err:      &mockError{msg: "read: connection reset by peer"},
			expected: true,
		},
		{
			name:     "i/o timeout",
			err:      &mockError{msg: "read: i/o timeout"},
			expected: true,
		},
		{
			name:     "broken pipe",
			err:      &mockError{msg: "write: broken pipe"},
			expected: true,
		},
		{
			name:     "EOF",
			err:      fmt.Errorf("read: %w", io.ErrUnexpectedEOF),
			expected: true,
		},
		{
			name:     "generic error",
			err:      &mockError{msg: "some random error"},
			expected: false,
		},
		{
			name:     "conversion error",
			err:      &mockError{msg: "failed to parse buildId"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRetryableError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

type mockError struct {
	msg string
}

func (e *mockError) Error() string {
	return e.msg
}
