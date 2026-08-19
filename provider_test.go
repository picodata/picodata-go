package picodata

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMockPool(host string, port int) *pgxpool.Pool {
	cfg, _ := pgxpool.ParseConfig("")
	cfg.ConnConfig.Host = host
	cfg.ConnConfig.Port = uint16(port)
	p, _ := pgxpool.NewWithConfig(context.Background(), cfg)
	return p
}

// newTestPoolManager seeds a manager with several pools at once. It fills
// pools/poolsMap directly rather than going through addPool, which would build
// a fresh pgxpool out of the config instead of taking the one handed to it.
func newTestPoolManager(pools ...*pgxpool.Pool) *poolManager {
	pm := newPoolManager(pools[0], 1)

	for _, p := range pools[1:] {
		pm.pools = append(pm.pools, p)
		pm.poolsMap[poolAddr(p)] = len(pm.pools) - 1
	}

	return pm
}

func TestPoolManager(t *testing.T) {
	t.Run("TestNewPoolManager", func(t *testing.T) {
		pm := newPoolManager(newMockPool("127.0.0.1", 5432), 1)

		require.NotNil(t, pm)
		assert.Len(t, pm.pools, 1)
		assert.Equal(t, 1, pm.PoolSize())
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
		pm := newPoolManager(pool, 1)

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
}
