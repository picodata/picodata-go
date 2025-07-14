package strategies_test

import (
	"sync"
	"testing"

	"github.com/picodata/picodata-go/strategies"
	"github.com/stretchr/testify/assert"
)

func TestRandomStrategy(t *testing.T) {
	t.Run("TestType", func(t *testing.T) {
		strategy := strategies.NewRandomStrategy()
		assert.Equal(t, strategy.Type(), "Random")
	})

	t.Run("TestNext", func(t *testing.T) {
		var current uint64 = 0
		strategy := strategies.NewRandomStrategy()
		poolSize := uint64(4)

		for range 50 {
			got := strategy.Next(&current, poolSize)
			assert.LessOrEqual(t, got, poolSize-1)
		}
	})

	t.Run("TestNextConcurrent", func(t *testing.T) {
		var current uint64 = 0
		strategy := strategies.NewRandomStrategy()
		poolSize := uint64(5)
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

		for _, got := range resultsSlice {
			assert.LessOrEqual(t, got, poolSize-1)
		}
	})
}
