package strategies_test

import (
	"sync"
	"testing"

	"git.picodata.io/core/drivers/picodata-go/strategies"
	"github.com/stretchr/testify/assert"
)

func TestRoundRobinStrategy(t *testing.T) {
	t.Run("TestType", func(t *testing.T) {
		assert.Equal(t, "RoundRobin", strategies.NewRoundRobinStrategy().Type())
	})

	t.Run("TestNext", func(t *testing.T) {
		strategy := strategies.NewRoundRobinStrategy()

		// The selector is raw and the start is seeded, so what holds is the step.
		first := strategy.Next()
		for i := uint64(1); i < 12; i++ {
			assert.Equal(t, first+i, strategy.Next())
		}
	})

	t.Run("TestStartsAtARandomOffset", func(t *testing.T) {
		const tries = 32

		seen := make(map[uint64]struct{}, tries)
		for range tries {
			seen[strategies.NewRoundRobinStrategy().Next()] = struct{}{}
		}

		assert.Greater(t, len(seen), 1, "every strategy started from the same counter")
	})

	t.Run("TestAdvanceIndependently", func(t *testing.T) {
		a, b := strategies.NewRoundRobinStrategy(), strategies.NewRoundRobinStrategy()

		first := a.Next()
		for range 3 {
			b.Next()
		}

		// One client's traffic must not move another client's rotation.
		assert.Equal(t, first+1, a.Next())
	})

	t.Run("TestNextConcurrent", func(t *testing.T) {
		strategy := strategies.NewRoundRobinStrategy()

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

		// Every goroutine gets its own step, so the selectors come out distinct
		// and contiguous however they interleave.
		seen := make(map[uint64]struct{}, goroutines)
		low, high := ^uint64(0), uint64(0)

		for got := range results {
			seen[got] = struct{}{}
			low = min(low, got)
			high = max(high, got)
		}

		assert.Len(t, seen, goroutines, "a selector was handed out twice")
		assert.Equal(t, uint64(goroutines-1), high-low)
	})
}
