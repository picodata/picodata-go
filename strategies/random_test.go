package strategies_test

import (
	"sync"
	"testing"

	"git.picodata.io/core/drivers/picodata-go/strategies"
	"github.com/stretchr/testify/assert"
)

func TestRandomStrategy(t *testing.T) {
	t.Run("TestType", func(t *testing.T) {
		assert.Equal(t, "Random", strategies.NewRandomStrategy().Type())
	})

	t.Run("TestNext", func(t *testing.T) {
		strategy := strategies.NewRandomStrategy()

		// The selector is raw, so what holds is that it varies and that reducing
		// it covers the whole pool.
		seen := map[uint64]struct{}{}
		reduced := map[uint64]struct{}{}

		for range 200 {
			got := strategy.Next()

			seen[got] = struct{}{}
			reduced[got%4] = struct{}{}
		}

		assert.Greater(t, len(seen), 190, "the selector repeats far too often")
		assert.Len(t, reduced, 4, "every index must come up")
	})

	t.Run("TestNextConcurrent", func(t *testing.T) {
		strategy := strategies.NewRandomStrategy()

		const goroutines = 100

		results := make(chan uint64, goroutines)

		var wg sync.WaitGroup
		for range goroutines {
			wg.Add(1)

			go func() {
				defer wg.Done()

				results <- strategy.Next()
			}()
		}

		wg.Wait()
		close(results)

		seen := make(map[uint64]struct{}, goroutines)
		for got := range results {
			seen[got] = struct{}{}
		}

		assert.Len(t, seen, goroutines, "a selector was handed out twice")
	})
}
