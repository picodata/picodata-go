package picodata

import (
	"log/slog"
	"sync"
	"testing"

	tpm "git.picodata.io/core/drivers/picodata-go/topology"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// staleIndexStrategy always returns an index that is out of range for the
// current pool count.
type staleIndexStrategy struct{ index uint64 }

func (s staleIndexStrategy) Next() uint64 {
	return s.index
}

func (s staleIndexStrategy) Type() string {
	return "StaleIndex"
}

func TestRouter(t *testing.T) {
	t.Run("NextConnection", func(t *testing.T) {
		pm := newTestPoolManager(t)
		pm.reconcile(snapshot(1,
			node("uuid-1", "127.0.0.1:5432"),
			node("uuid-2", "127.0.0.1:5433"),
			node("uuid-3", "127.0.0.1:5434"),
		))
		pools := rotation(pm)

		r := newRouter(pm)

		// RoundRobin from a seeded offset: the cycle holds, the start does not.
		// By identity: assert.Equal deep-walks pgxpool internals.
		seen := map[*pgxpool.Pool]int{}
		for range 3 {
			seen[r.nextConnection()]++
		}

		require.Len(t, seen, 3, "a full cycle must visit every pool exactly once")
		for _, inst := range pools {
			assert.Equal(t, 1, seen[inst.pool])
		}
	})

	t.Run("StaleIndexFromShrunkPool", func(t *testing.T) {
		pm := newTestPoolManager(t)
		pm.reconcile(snapshot(1,
			node("uuid-1", "127.0.0.1:5432"),
			node("uuid-2", "127.0.0.1:5433"),
		))
		pools := rotation(pm)
		pool1 := pools[0].pool

		// randomStrategy.Next can return an index reduced against a pool count
		// that no longer holds.
		r := newRouter(pm, withBalanceStrategy(staleIndexStrategy{index: 4}))

		conn := r.nextConnection()
		assert.NotNil(t, conn, "a stale index must not fail a query while pools are alive")
		assert.Same(t, pool1, conn, "index 4 wraps to 4 %% 2 == 0")
	})

	t.Run("IndexEqualToPoolCountWraps", func(t *testing.T) {
		pm := newTestPoolManager(t)
		pm.reconcile(snapshot(1,
			node("uuid-1", "127.0.0.1:5432"),
			node("uuid-2", "127.0.0.1:5433"),
		))
		pools := rotation(pm)

		// pick reduces only out-of-range indexes, and size is the boundary.
		r := newRouter(pm, withBalanceStrategy(staleIndexStrategy{index: 2}))

		assert.Same(t, pools[0].pool, r.nextConnection(), "index 2 wraps to 2 %% 2 == 0")
	})

	t.Run("NoPoolsRouteToNothing", func(t *testing.T) {
		var warned int
		r := newRouter(newTestPoolManager(t), withRouterLogger(slog.New(recordHandler{
			onRecord: func(msg string) {
				if msg == "connection not found in a pool" {
					warned++
				}
			},
		})))

		assert.Nil(t, r.nextConnection(), "an empty rotation has nothing to route to")
		assert.Equal(t, 1, warned, "the caller gets nil, the operator gets a reason")
	})

	t.Run("ARoutedQueryIsNotWarnedAbout", func(t *testing.T) {
		pm := newTestPoolManager(t)
		pm.reconcile(snapshot(1, node("uuid-1", "127.0.0.1:5432")))

		var warned int
		r := newRouter(pm, withRouterLogger(slog.New(recordHandler{
			onRecord: func(msg string) {
				if msg == "connection not found in a pool" {
					warned++
				}
			},
		})))

		assert.NotNil(t, r.nextConnection())
		assert.Equal(t, 0, warned)
	})

	t.Run("NextConnectionConcurrent", func(t *testing.T) {
		pm := newTestPoolManager(t)
		pm.reconcile(snapshot(1,
			node("uuid-1", "127.0.0.1:5432"),
			node("uuid-2", "127.0.0.1:5433"),
			node("uuid-3", "127.0.0.1:5434"),
		))
		r := newRouter(pm)

		wg := sync.WaitGroup{}

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

			pm.reconcile(snapshot(2,
				node("uuid-1", "127.0.0.1:5432"),
				node("uuid-2", "127.0.0.1:5433"),
				node("uuid-3", "127.0.0.1:5434", tpm.StateOffline),
			))
			pm.reconcile(snapshot(3,
				node("uuid-1", "127.0.0.1:5432"),
				node("uuid-2", "127.0.0.1:5433", tpm.StateOffline),
				node("uuid-3", "127.0.0.1:5434", tpm.StateOffline),
			))
			pm.reconcile(snapshot(4,
				node("uuid-1", "127.0.0.1:5432", tpm.StateOffline),
				node("uuid-2", "127.0.0.1:5433", tpm.StateOffline),
				node("uuid-3", "127.0.0.1:5434"),
			))
		}()

		wg.Wait()

		require.Len(t, *pm.instances.Load(), 1)
		assert.Equal(t, "uuid-3", (*pm.instances.Load())[0].uuid, "the re-added pool is the only survivor")
		assert.Equal(t, 1, pm.PoolSize())
	})
}
