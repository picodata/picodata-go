package picodata

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// staleIndexStrategy always returns an index that is out of range for the
// current pool count.
type staleIndexStrategy struct{ index uint64 }

func (s staleIndexStrategy) Next(_ *uint64, _ uint64) uint64 {
	return s.index
}

func (s staleIndexStrategy) Type() string {
	return "StaleIndex"
}

func TestRouter(t *testing.T) {
	t.Run("TestNextConnection", func(t *testing.T) {
		pool1, pool2 := newMockPool("127.0.0.1", 5432), newMockPool("127.0.0.1", 5433)
		r := newRouter(newTestPoolManager(
			instance{"uuid-1", pool1},
			instance{"uuid-2", pool2},
		))

		// Default strategy is RoundRobin
		assert.Equal(t, pool1, r.nextConnection())
		assert.Equal(t, pool2, r.nextConnection())
		assert.Equal(t, pool1, r.nextConnection())
	})

	t.Run("TestStaleIndexFromShrunkPool", func(t *testing.T) {
		pool1, pool2 := newMockPool("127.0.0.1", 5432), newMockPool("127.0.0.1", 5433)

		// randomStrategy.Next returns the value it swapped out, so it can hand
		// back an index taken modulo a pool count that no longer holds. Emulate
		// a cluster that shrank from 5 pools to 2 right after the strategy ran.
		r := newRouter(newTestPoolManager(
			instance{"uuid-1", pool1},
			instance{"uuid-2", pool2},
		), withBalanceStrategy(staleIndexStrategy{index: 4}))

		conn := r.nextConnection()
		assert.NotNil(t, conn, "a stale index must not fail a query while pools are alive")
		assert.Equal(t, pool1, conn, "index 4 wraps to 4 %% 2 == 0")
	})

	t.Run("TestNextConnectionEmptyReleasesLock", func(t *testing.T) {
		pm := newPoolManager(newMockConfig("127.0.0.1", 5432), 1)

		r := newRouter(pm)

		assert.Nil(t, r.nextConnection(), "an empty pool set has nothing to route to")

		// pick takes pm.mu before it checks the size, so the empty-set nil is a
		// return out of a locked section - the shape that leaked the lock in
		// GL-21. Probe pm.mu as a writer: a leaked RLock would let another
		// reader straight through and prove nothing.
		lockDone := make(chan struct{})
		go func() {
			pm.removePool("unknown-uuid")
			close(lockDone)
		}()

		select {
		case <-lockDone:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("pick did not release pm.mu on the empty-pool path")
		}
	})

	t.Run("TestNextConnectionConcurrent", func(t *testing.T) {
		pool1, pool2, pool3 := newMockPool("addr", 1), newMockPool("addr", 2), newMockPool("addr", 3)

		pm := newTestPoolManager(
			instance{"uuid-1", pool1},
			instance{"uuid-2", pool2},
			instance{"uuid-3", pool3},
		)
		r := newRouter(pm)

		wg := sync.WaitGroup{}

		// Readers race the churn below: the set never empties, so every pick
		// must land on a live pool rather than nil or an out-of-range index.
		wg.Add(200)
		for range 200 {
			go func() {
				defer wg.Done()
				assert.NotNil(t, r.nextConnection())
			}()
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			pm.removePool("uuid-3")
			pm.removePool("uuid-2")
			_ = pm.addPool("uuid-3", "addr:3")
			pm.removePool("uuid-1")
		}()

		wg.Wait()

		require.Len(t, pm.instances, 1)
		assert.Equal(t, "uuid-3", pm.instances[0].uuid, "the re-added pool is the only survivor")
		assert.Equal(t, 0, pm.poolsMap["uuid-3"])
		assert.Equal(t, 1, pm.PoolSize())
	})
}
