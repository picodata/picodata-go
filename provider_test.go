package picodata

import (
	"context"
	"testing"
	"time"

	"git.picodata.io/core/drivers/picodata-go/internal/testenv"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMockConfig(host string, port int) *pgxpool.Config {
	cfg, _ := pgxpool.ParseConfig("")
	cfg.ConnConfig.Host = host
	cfg.ConnConfig.Port = uint16(port)
	return cfg
}

func newMockPool(host string, port int) *pgxpool.Pool {
	p, _ := pgxpool.NewWithConfig(context.Background(), newMockConfig(host, port))
	return p
}

// newTestPoolManager seeds a manager with several pools at once. It fills
// pools/poolsMap directly rather than going through addPool, which would build
// a fresh pgxpool out of the config instead of taking the one handed to it.
func newTestPoolManager(pools ...*pgxpool.Pool) *poolManager {
	pm := newPoolManager(pools[0].Config(), 1)

	for _, p := range pools {
		pm.pools = append(pm.pools, p)
		pm.poolsMap[poolAddr(p)] = len(pm.pools) - 1
	}

	return pm
}

func TestPoolManager(t *testing.T) {
	t.Run("TestNewPoolManager", func(t *testing.T) {
		// The manager starts empty: pools are opened from the topology, the
		// bootstrap host of the config included.
		pm := newPoolManager(newMockConfig("127.0.0.1", 5432), 1)

		require.NotNil(t, pm)
		assert.Empty(t, pm.pools)
		assert.Equal(t, 0, pm.PoolSize())
	})

	t.Run("TestAddPool", func(t *testing.T) {
		pm := newPoolManager(newMockConfig("127.0.0.1", 5432), 1)

		require.NoError(t, pm.addPool("127.0.0.1:5433"))

		require.Len(t, pm.pools, 1)
		assert.Equal(t, 1, pm.PoolSize())
		assert.Equal(t, "127.0.0.1:5433", poolAddr(pm.pools[0]))
	})

	t.Run("TestAddPoolInvalidAddress", func(t *testing.T) {
		pm := newPoolManager(newMockConfig("127.0.0.1", 5432), 1)

		assert.Error(t, pm.addPool("127.0.0.1"))
		assert.Equal(t, 0, pm.PoolSize())
	})

	t.Run("TestRemoveUnknownConnectionNoEffect", func(t *testing.T) {
		pm := newTestPoolManager(newMockPool("host", 0), newMockPool("host", 1))

		require.NotPanics(t, func() {
			pm.removePool("unknown:9999")
		})

		assert.Len(t, pm.pools, 2)
		assert.Equal(t, 2, pm.PoolSize())
	})

	t.Run("TestRemoveConnection", func(t *testing.T) {
		pool := newMockPool("127.0.0.1", 5432)
		pm := newTestPoolManager(pool)

		pm.removePool(poolAddr(pool))

		assert.Empty(t, pm.pools)
		assert.Equal(t, 0, pm.PoolSize())
	})

	t.Run("TestRemoveConnectionLastPosition", func(t *testing.T) {
		pool1, pool2, pool3 := newMockPool("host", 0), newMockPool("host", 1), newMockPool("host", 2)
		pm := newTestPoolManager(pool1, pool2, pool3)

		pm.removePool(poolAddr(pool3))

		// Dropping the tail moves nothing: the survivors keep their indexes.
		require.Len(t, pm.pools, 2)
		assert.Equal(t, 2, pm.PoolSize())
		assert.Equal(t, 0, pm.poolsMap[poolAddr(pool1)])
		assert.Equal(t, 1, pm.poolsMap[poolAddr(pool2)])
	})

	t.Run("TestRemoveConnectionMiddlePosition", func(t *testing.T) {
		pool1, pool2, pool3 := newMockPool("host", 0), newMockPool("host", 1), newMockPool("host", 2)
		pm := newTestPoolManager(pool1, pool2, pool3)

		pm.removePool(poolAddr(pool2))

		// The tail is swapped into the hole, so pool3 has to be reindexed to 1
		// in both the slice and the map, or a later lookup routes to nothing.
		require.Len(t, pm.pools, 2)
		assert.Equal(t, 2, pm.PoolSize())
		assert.Equal(t, pool3, pm.pools[1])
		assert.Equal(t, 0, pm.poolsMap[poolAddr(pool1)])
		assert.Equal(t, 1, pm.poolsMap[poolAddr(pool3)])
	})

	t.Run("TestRemovePoolClosesRealConnection", func(t *testing.T) {
		testenv.RequireCluster(t)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		testenv.WaitReady(t, ctx, 1, 1)

		cfg, err := pgxpool.ParseConfig(testenv.URI())
		require.NoError(t, err)

		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		require.NoError(t, err)
		defer pool.Close() // safety cleanup

		_, err = pool.Exec(ctx, "SELECT 1")
		require.NoError(t, err)

		pm := newTestPoolManager(pool)

		// Remove the pool from manager
		addr := poolAddr(pool)

		pm.removePool(addr)

		// Wait for the background goroutine to close the pool.
		// Poll Acquire until it fails, with a deadline.
		deadline := time.Now().Add(2 * time.Second)
		var closed bool
		for time.Now().Before(deadline) {
			conn, err := pool.Acquire(ctx)
			if err != nil {
				closed = true
				break
			}
			conn.Release()
			time.Sleep(50 * time.Millisecond)
		}

		assert.True(t, closed, "pool should be closed after removePool")

		// Also ensure the manager's internal structures no longer contain the pool
		assert.Equal(t, 0, pm.PoolSize())
		assert.Empty(t, pm.pools)
		assert.Empty(t, pm.poolsMap)
	})
}
