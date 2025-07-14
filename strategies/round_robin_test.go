package strategies_test

import (
	"sync"
	"testing"

	"github.com/picodata/picodata-go/strategies"
	"github.com/stretchr/testify/assert"
)

func TestRoundRobinStrategy(t *testing.T) {
	t.Run("TestType", func(t *testing.T) {
		strategy := strategies.NewRoundRobinStrategy()
		assert.Equal(t, strategy.Type(), "RoundRobin")
	})

	t.Run("TestNext", func(t *testing.T) {
		var current uint64 = 0
		strategy := strategies.NewRoundRobinStrategy()
		poolSize := uint64(4)

		expected := []uint64{0, 1, 2, 3, 0, 1, 2, 3}
		for _, want := range expected {
			got := strategy.Next(&current, poolSize)
			assert.Equal(t, got, want)
		}
	})

	t.Run("TestNextConcurrent", func(t *testing.T) {
		var current uint64 = 0
		strategy := strategies.NewRoundRobinStrategy()
		poolSize := uint64(2)
		const goroutines = 100

		results := make(chan uint64, goroutines)

		var wg sync.WaitGroup
		for range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results <- strategy.Next(&current, poolSize)
			}()
		}

		go func() {
			wg.Wait()
			close(results)
		}()

		// Collect all results
		resultsSlice := make([]uint64, 0, goroutines)
		for v := range results {
			resultsSlice = append(resultsSlice, v)
		}

		assert.Equal(t, len(resultsSlice), goroutines)

		// Count occurrences of each port
		count1, count2 := uint64(0), uint64(0)
		for _, port := range resultsSlice {
			switch port {
			case uint64(0):
				count1++
			case uint64(1):
				count2++
			}
		}

		// Verify each port is used exactly 500 times
		expected := goroutines / poolSize
		if count1 != expected || count2 != expected {
			t.Errorf("Expected %d each, got %d: %d, %d: %d", expected, 0, count1, 1, count2)
		}
	})

	t.Run("TestNextAfterMaxIndex", func(t *testing.T) {
		var current uint64 = ^uint64(0) // Max uint64
		strategy := strategies.NewRoundRobinStrategy()
		poolSize := uint64(2)

		got := strategy.Next(&current, poolSize)

		assert.LessOrEqual(t, got, poolSize)
	})
}
