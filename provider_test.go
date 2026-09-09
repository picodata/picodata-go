package picodata

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"git.picodata.io/core/drivers/picodata-go/internal/testenv"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordHandler reports every emitted message to onRecord, so a test can tell
// when a background goroutine logged its completion.
type recordHandler struct {
	onRecord func(msg string)
}

func (h recordHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h recordHandler) Handle(_ context.Context, r slog.Record) error {
	h.onRecord(r.Message)
	return nil
}

func (h recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h recordHandler) WithGroup(string) slog.Handler { return h }

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
// The arguments are ordered, so the tests can rely on the resulting indexes.
func newTestPoolManager(instances ...instance) *poolManager {
	pm := newPoolManager(instances[0].pool.Config(), 1)

	for _, in := range instances {
		pm.instances = append(pm.instances, in)
		pm.poolsMap[in.uuid] = len(pm.instances) - 1
	}

	return pm
}

func TestPoolManager(t *testing.T) {
	t.Run("TestNewPoolManager", func(t *testing.T) {
		// The manager starts empty: pools are opened from the topology, the
		// bootstrap host of the config included.
		pm := newPoolManager(newMockConfig("127.0.0.1", 5432), 1)

		require.NotNil(t, pm)
		assert.Empty(t, pm.instances)
		assert.Empty(t, pm.poolsMap)
		assert.Equal(t, 0, pm.PoolSize())
	})

	t.Run("TestAddPool", func(t *testing.T) {
		pm := newPoolManager(newMockConfig("127.0.0.1", 5432), 1)

		require.NoError(t, pm.addPool("uuid-1", "127.0.0.1:5433"))

		require.Len(t, pm.instances, 1)
		assert.Equal(t, 1, pm.PoolSize())
		assert.Equal(t, "uuid-1", pm.instances[0].uuid)
		assert.Equal(t, 0, pm.poolsMap["uuid-1"])

		// The address is no longer the key, so it has to land in the pool config.
		cfg := pm.instances[0].pool.Config().ConnConfig
		assert.Equal(t, "127.0.0.1", cfg.Host)
		assert.Equal(t, uint16(5433), cfg.Port)
	})

	t.Run("TestAddPoolSameUUIDTwice", func(t *testing.T) {
		pm := newPoolManager(newMockConfig("127.0.0.1", 5432), 1)

		require.NoError(t, pm.addPool("uuid-1", "127.0.0.1:5433"))
		require.NoError(t, pm.addPool("uuid-1", "127.0.0.1:5433"))

		// A repeated uuid must not leave a duplicate entry in the pools slice,
		// or the same instance would take two slots in the pick rotation.
		assert.Len(t, pm.instances, 1)
		assert.Equal(t, 1, pm.PoolSize())
	})

	t.Run("TestAddPoolInvalidAddress", func(t *testing.T) {
		pm := newPoolManager(newMockConfig("127.0.0.1", 5432), 1)

		assert.Error(t, pm.addPool("uuid-1", "127.0.0.1"))
		assert.Equal(t, 0, pm.PoolSize())
	})

	t.Run("TestAddPoolPortOutOfRange", func(t *testing.T) {
		pm := newPoolManager(newMockConfig("127.0.0.1", 5432), 1)

		for _, address := range []string{"127.0.0.1:0", "127.0.0.1:70000"} {
			err := pm.addPool("uuid-1", address)

			// A port that parses but is out of range has no cause to wrap, so
			// the message must not carry a formatting artefact from a nil %w.
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "%!w")
			assert.NoError(t, errors.Unwrap(err))
		}

		assert.Equal(t, 0, pm.PoolSize())
	})

	t.Run("TestRemoveUnknownConnectionNoEffect", func(t *testing.T) {
		pm := newTestPoolManager(
			instance{"uuid-1", newMockPool("host", 0)},
			instance{"uuid-2", newMockPool("host", 1)},
		)

		require.NotPanics(t, func() {
			pm.removePool("unknown-uuid")
		})

		assert.Len(t, pm.instances, 2)
		assert.Equal(t, 2, pm.PoolSize())
	})

	t.Run("TestRemoveConnection", func(t *testing.T) {
		pm := newTestPoolManager(
			instance{"uuid-1", newMockPool("127.0.0.1", 5432)},
		)

		pm.removePool("uuid-1")

		assert.Empty(t, pm.instances)
		assert.Empty(t, pm.poolsMap)
		assert.Equal(t, 0, pm.PoolSize())
	})

	t.Run("TestRemoveConnectionLastPosition", func(t *testing.T) {
		pm := newTestPoolManager(
			instance{"uuid-1", newMockPool("host", 0)},
			instance{"uuid-2", newMockPool("host", 1)},
			instance{"uuid-3", newMockPool("host", 2)},
		)

		pm.removePool("uuid-3")

		// Dropping the tail moves nothing: the survivors keep their indexes.
		require.Len(t, pm.instances, 2)
		assert.Equal(t, 2, pm.PoolSize())
		assert.Equal(t, 0, pm.poolsMap["uuid-1"])
		assert.Equal(t, 1, pm.poolsMap["uuid-2"])
		assert.NotContains(t, pm.poolsMap, "uuid-3")
	})

	t.Run("TestRemoveConnectionMiddlePosition", func(t *testing.T) {
		pool1, pool2, pool3 := newMockPool("host", 0), newMockPool("host", 1), newMockPool("host", 2)
		pm := newTestPoolManager(
			instance{"uuid-1", pool1},
			instance{"uuid-2", pool2},
			instance{"uuid-3", pool3},
		)

		pm.removePool("uuid-2")

		// The tail is swapped into the hole, so uuid-3 has to be reindexed to 1
		// in both the slice and the map, or a later lookup routes to nothing.
		require.Len(t, pm.instances, 2)
		assert.Equal(t, 2, pm.PoolSize())
		assert.Equal(t, pool3, pm.instances[1].pool)
		assert.Equal(t, "uuid-3", pm.instances[1].uuid)
		assert.Equal(t, 0, pm.poolsMap["uuid-1"])
		assert.Equal(t, 1, pm.poolsMap["uuid-3"])
		assert.NotContains(t, pm.poolsMap, "uuid-2")
	})

	t.Run("TestPickResolvesEveryPosition", func(t *testing.T) {
		pool1, pool2, pool3 := newMockPool("host", 0), newMockPool("host", 1), newMockPool("host", 2)
		pm := newTestPoolManager(
			instance{"uuid-1", pool1},
			instance{"uuid-2", pool2},
			instance{"uuid-3", pool3},
		)

		pm.removePool("uuid-2")

		// Every index the pick rotation can produce must map to a live pool.
		for i := range pm.PoolSize() {
			got := pm.pick(func(int) uint64 { return uint64(i) })
			assert.NotNil(t, got, "pick returned nil for index %d", i)
		}
	})

	t.Run("TestCloseReleasesTheLockWhileClosingPools", func(t *testing.T) {
		// A listener that accepts and then stays silent leaves pgx stuck in the
		// startup handshake. puddle counts a resource from the moment it starts
		// constructing it, so Pool.Close blocks on that one - the same wait a
		// checked-out user connection causes, without needing a real cluster.
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		accepted := make(chan net.Conn, 4)
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				accepted <- conn
			}
		}()

		host, portStr, err := net.SplitHostPort(ln.Addr().String())
		require.NoError(t, err)
		port, err := strconv.Atoi(portStr)
		require.NoError(t, err)

		cfg := newMockConfig(host, port)
		cfg.MinConns = 1

		pm := newPoolManager(cfg, 1)
		require.NoError(t, pm.addPool("uuid-1", ln.Addr().String()))

		// From here on the pool has a connection in flight, so its Close cannot
		// return until that attempt is resolved.
		var held net.Conn
		select {
		case held = <-accepted:
		case <-time.After(5 * time.Second):
			t.Fatal("the pool never dialled the listener")
		}

		closed := make(chan struct{})
		go func() {
			pm.close()
			close(closed)
		}()

		// close() empties the manager under pm.mu and only then closes the
		// pools, so the emptiness has to become observable while they are still
		// closing. Holding the lock across that wait would block this read -
		// and, in production, the caller returning the very connection close()
		// waits for, which is a deadlock.
		emptied := make(chan struct{})
		go func() {
			defer close(emptied)

			for pm.PoolSize() != 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}()

		select {
		case <-emptied:
		case <-time.After(5 * time.Second):
			t.Fatal("close() held pm.mu while waiting for the pools to close")
		}

		// A pick that races close must find nothing rather than a pool that is
		// already going away.
		assert.Nil(t, pm.pick(func(int) uint64 { return 0 }))

		// Let the handshake fail so the pool can finish closing.
		require.NoError(t, held.Close())
		require.NoError(t, ln.Close())

		select {
		case <-closed:
		case <-time.After(30 * time.Second):
			t.Fatal("close() did not return after the connection was released")
		}
	})

	t.Run("TestCloseWaitsForBackgroundCloses", func(t *testing.T) {
		pm := newTestPoolManager(
			instance{"uuid-1", newMockPool("host", 1)},
			instance{"uuid-2", newMockPool("host", 2)},
		)

		var closed atomic.Bool
		pm.logger = slog.New(recordHandler{onRecord: func(msg string) {
			if msg == "pool closed" {
				closed.Store(true)
			}
		}})

		pm.removePool("uuid-1")

		// close() waits on pm.closing while holding pm.mu. That is only safe as
		// long as the background closers never take pm.mu themselves, so guard
		// the deadlock this would cause rather than trusting the review of it.
		done := make(chan struct{})
		go func() {
			pm.close()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("close() deadlocked waiting for the background pool closes")
		}

		assert.True(t, closed.Load(), "close() returned before the removed pool finished closing")
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

		targetUUID := "real-node-uuid"
		pm := newTestPoolManager(instance{targetUUID, pool})

		// Remove the pool from manager
		pm.removePool(targetUUID)

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
		assert.Empty(t, pm.instances)
		assert.Empty(t, pm.poolsMap)
	})
}
