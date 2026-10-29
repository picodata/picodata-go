package picodata

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"git.picodata.io/core/drivers/picodata-go/internal/filter"
	"git.picodata.io/core/drivers/picodata-go/internal/testenv"
	tpm "git.picodata.io/core/drivers/picodata-go/topology"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type excludeNodeByID string

// Allow implements [filter.Filter] by rejecting the node with the specified ID.
func (id excludeNodeByID) Allow(node tpm.NodeInfo) bool { return node.ID != string(id) }

func TestPoolManagerReconcile(t *testing.T) {
	t.Run("StartsEmpty", func(t *testing.T) {
		pm := newTestPoolManager(t)

		assert.Empty(t, rotation(pm))
		assert.Equal(t, 0, pm.PoolSize())
		assert.Nil(t, pm.pick(0))
	})

	t.Run("OpensPoolsForOnlineNodes", func(t *testing.T) {
		pm := newTestPoolManager(t)

		pm.reconcile(snapshot(1,
			node("uuid-1", "127.0.0.1:5433"),
			node("uuid-2", "127.0.0.1:5434"),
		))
		pools := rotation(pm)

		require.Len(t, pools, 2)
		assert.Equal(t, 2, pm.PoolSize())
		assert.ElementsMatch(t, []string{"uuid-1", "uuid-2"}, uuidsOf(pools))

		// The address is not the key, so it has to land in the pool config.
		for _, inst := range pools {
			cfg := inst.pool.Config().ConnConfig

			assert.Equal(t, "127.0.0.1", cfg.Host)
			if inst.uuid == "uuid-1" {
				assert.Equal(t, uint16(5433), cfg.Port)
			} else {
				assert.Equal(t, uint16(5434), cfg.Port)
			}
		}
	})

	t.Run("DifferentFiltersCombineWithAnd", func(t *testing.T) {
		// A node rejected by either filter must not get a pool.
		pm := newTestPoolManager(t, withPoolManagerFilters(
			filter.NewForbiddenTiers([]string{"arbiter"}), excludeNodeByID("excluded-id"),
		))
		snap := snapshot(1,
			tpm.NodeInfo{ID: "excluded-tier", Tier: "arbiter", State: tpm.StateOnline, Address: "localhost:5432"},
			node("excluded-id", "localhost:5433"),
			node("allowed", "localhost:5434"),
		)
		expected := snap.Clone()

		require.Equal(t, 1, pm.reconcile(snap))
		assert.Equal(t, []string{"allowed"}, uuidsOf(rotation(pm)))
		assert.Equal(t, expected, snap.Clone())
	})

	t.Run("RepeatedFilterOptionsAccumulate", func(t *testing.T) {
		// Repeated options exclude both tiers without changing the full snapshot.
		pm := newTestPoolManager(t,
			withPoolManagerFilters(filter.NewForbiddenTiers([]string{"arbiter"})),
			withPoolManagerFilters(filter.NewForbiddenTiers([]string{"readonly"})),
		)
		snap := snapshot(1,
			tpm.NodeInfo{ID: "node-1", Tier: "default", State: tpm.StateOnline, Address: "localhost:5432"},
			tpm.NodeInfo{ID: "node-2", Tier: "readonly", State: tpm.StateOnline, Address: "localhost:5433"},
			tpm.NodeInfo{ID: "node-3", Tier: "arbiter", State: tpm.StateOnline, Address: "localhost:5434"},
		)
		expected := snap.Clone()
		pm.reconcile(snap)
		require.Equal(t, []string{"node-1"}, uuidsOf(rotation(pm)))
		assert.Equal(t, expected, snap.Clone(), "filtering must not change the source snapshot")
		warm := rotation(pm)[0].pool

		// An excluded node changes; the allowed node keeps its pool.
		next := snap.Clone()
		next.Version++
		n := next.Nodes["node-3"]
		n.State = tpm.StateOffline
		next.Nodes[n.ID] = n
		pm.reconcile(testSnapshot{next})
		require.Equal(t, []string{"node-1"}, uuidsOf(rotation(pm)))
		assert.Same(t, warm, rotation(pm)[0].pool)
	})

	t.Run("WarnsWhenEveryOnlineNodeIsHidden", func(t *testing.T) {
		var warnings int
		logger := slog.New(recordHandler{onRecord: func(msg string) {
			if msg == "every online node is hidden by filter" {
				warnings++
			}
		}})
		pm := newTestPoolManager(t, withPoolManagerFilters(filter.NewForbiddenTiers([]string{"arbiter"})), withPoolManagerLogger(logger))
		pm.reconcile(snapshot(1, node("allowed", "localhost:5432")))
		assert.Zero(t, warnings)

		// Filtering away every online node leaves the driver with nothing to
		// route to, which is worth saying out loud rather than failing silently.
		// An allowed offline node must not suppress the warning.
		pm.reconcile(snapshot(2,
			tpm.NodeInfo{ID: "arbiter", Tier: "arbiter", State: tpm.StateOnline},
			node("allowed", "localhost:5432", tpm.StateOffline),
		))
		assert.Equal(t, 1, warnings)

		// No online nodes and failed pool creation must not be reported as filtering.
		pm.reconcile(snapshot(3, node("offline", "localhost:5432", tpm.StateOffline)))
		pm.reconcile(snapshot(4, node("invalid", "invalid-address")))
		assert.Equal(t, 1, warnings)

		// An obsolete report must not emit a filtering warning.
		pm.reconcile(snapshot(3,
			tpm.NodeInfo{ID: "arbiter", Tier: "arbiter", State: tpm.StateOnline},
		))
		assert.Equal(t, 1, warnings)
	})

	t.Run("StaysSilentWhenSomeOnlineNodeIsAllowed", func(t *testing.T) {
		// An allowed online node suppresses the filtering warning.
		var warnings int
		logger := slog.New(recordHandler{onRecord: func(msg string) {
			if msg == "every online node is hidden by filter" {
				warnings++
			}
		}})
		pm := newTestPoolManager(t, withPoolManagerFilters(filter.NewForbiddenTiers([]string{"arbiter"})), withPoolManagerLogger(logger))
		pm.reconcile(snapshot(1,
			node("allowed", "localhost:5432"),
			tpm.NodeInfo{ID: "arbiter", Tier: "arbiter", State: tpm.StateOnline},
		))
		assert.Zero(t, warnings)
	})

	t.Run("TierChangesUpdatePools", func(t *testing.T) {
		// Changing a node's tier removes and restores its pool eligibility.
		pm := newTestPoolManager(t, withPoolManagerFilters(filter.NewForbiddenTiers([]string{"arbiter"})))
		n := node("node-1", "localhost:5432")
		pm.reconcile(snapshot(1, n))
		require.Equal(t, 1, pm.PoolSize())

		n.Tier = "arbiter"
		pm.reconcile(snapshot(2, n))
		assert.Zero(t, pm.PoolSize())
		assert.Nil(t, pm.pick(0))

		n.Tier = "default"
		pm.reconcile(snapshot(3, n))
		assert.Equal(t, 1, pm.PoolSize())
		assert.NotNil(t, pm.pick(0))
	})

	t.Run("AppliesConnectionsPerInstance", func(t *testing.T) {
		pm := newPoolManager(newMockConfig("127.0.0.1", 5432), 7)
		t.Cleanup(pm.close)

		require.Equal(t, 1, pm.reconcile(snapshot(1, node("uuid-1", "127.0.0.1:5433"))))
		assert.Equal(t, int32(7), rotation(pm)[0].pool.Config().MaxConns)
	})

	t.Run("KeepsConfiguredMaxConnsWhenUnset", func(t *testing.T) {
		cfg := newMockConfig("127.0.0.1", 5432)
		cfg.MaxConns = 11

		// Zero means "not configured", so the pool keeps what the DSN asked for.
		pm := newPoolManager(cfg, 0)
		t.Cleanup(pm.close)

		require.Equal(t, 1, pm.reconcile(snapshot(1, node("uuid-1", "127.0.0.1:5433"))))
		assert.Equal(t, int32(11), rotation(pm)[0].pool.Config().MaxConns)
	})

	t.Run("SkipsOfflineNodes", func(t *testing.T) {
		pm := newTestPoolManager(t)

		pm.reconcile(snapshot(1,
			node("uuid-1", "127.0.0.1:5433"),
			node("uuid-2", "127.0.0.1:5434", tpm.StateOffline),
		))

		assert.Equal(t, []string{"uuid-1"}, uuidsOf(rotation(pm)))
	})

	t.Run("IgnoresUnknownState", func(t *testing.T) {
		// Per state, not one mixed snapshot: counting alone cannot tell "warns
		// for the wrong state" from "warns for the right one".
		warningsFor := func(state string) int {
			pm := newTestPoolManager(t)

			var warned int
			pm.logger = slog.New(recordHandler{onRecord: func(msg string) {
				if msg == "node with unknown state is ignored" {
					warned++
				}
			}})

			pm.reconcile(snapshot(1,
				node("uuid-1", "127.0.0.1:5433"),
				node("uuid-2", "127.0.0.1:5434", state),
			))
			assert.Equal(t, []string{"uuid-1"}, uuidsOf(rotation(pm)))

			return warned
		}

		assert.Equal(t, 0, warningsFor(tpm.StateOffline), "Offline is expected and silent")
		assert.Equal(t, 1, warningsFor("Expelled"), "an unknown state is worth a warning")
	})

	t.Run("DropsNodesMissingFromTheSnapshot", func(t *testing.T) {
		pm := newTestPoolManager(t)

		pm.reconcile(snapshot(1,
			node("uuid-1", "127.0.0.1:5433"),
			node("uuid-2", "127.0.0.1:5434"),
		))
		pm.reconcile(snapshot(2, node("uuid-1", "127.0.0.1:5433")))

		assert.Equal(t, []string{"uuid-1"}, uuidsOf(rotation(pm)))
	})

	t.Run("SurvivorsKeepTheirPoolAndPosition", func(t *testing.T) {
		pm := newTestPoolManager(t)

		pm.reconcile(snapshot(1,
			node("uuid-1", "127.0.0.1:5433"),
			node("uuid-2", "127.0.0.1:5434"),
			node("uuid-3", "127.0.0.1:5435"),
		))
		first := rotation(pm)

		poolFor := map[string]*pgxpool.Pool{}

		var expected []string
		for _, inst := range first {
			poolFor[inst.uuid] = inst.pool
			if inst.uuid != "uuid-2" {
				expected = append(expected, inst.uuid)
			}
		}

		pm.reconcile(snapshot(2,
			node("uuid-1", "127.0.0.1:5433"),
			node("uuid-2", "127.0.0.1:5434", tpm.StateOffline),
			node("uuid-3", "127.0.0.1:5435"),
			node("uuid-4", "127.0.0.1:5436"),
		))
		second := rotation(pm)

		// Warm pools kept: rebuilding them drops every established connection.
		require.Len(t, second, 3)
		assert.Equal(t, append(expected, "uuid-4"), uuidsOf(second))
		for _, inst := range second[:2] {
			assert.Same(t, poolFor[inst.uuid], inst.pool)
		}
	})

	t.Run("RebuildsThePoolWhenTheAddressChanges", func(t *testing.T) {
		pm := newTestPoolManager(t)

		var closes atomic.Int32
		pm.logger = slog.New(recordHandler{onRecord: func(msg string) {
			if msg == "pool closed" {
				closes.Add(1)
			}
		}})

		pm.reconcile(snapshot(1, node("uuid-1", "127.0.0.1:5433")))
		old := rotation(pm)[0].pool

		// Same uuid, new address: keeping the pool would route to the old host.
		pm.reconcile(snapshot(2, node("uuid-1", "127.0.0.1:5444")))
		second := rotation(pm)

		require.Len(t, second, 1)
		assert.NotSame(t, old, second[0].pool)
		assert.Equal(t, "127.0.0.1:5444", second[0].addr)
		assert.Equal(t, uint16(5444), second[0].pool.Config().ConnConfig.Port)

		pm.close()
		assert.Equal(t, int32(1), closes.Load(), "the replaced pool must be closed")
	})

	t.Run("IsIdempotent", func(t *testing.T) {
		pm := newTestPoolManager(t)

		snap := snapshot(1,
			node("uuid-1", "127.0.0.1:5433"),
			node("uuid-2", "127.0.0.1:5434"),
		)

		pm.reconcile(snap)
		first := rotation(pm)
		pm.reconcile(snap)
		second := rotation(pm)

		// Replaying a snapshot must not churn pools; that is how a listener
		// recovers from a missed update.
		assert.Equal(t, uuidsOf(first), uuidsOf(second))
		for index := range first {
			assert.Same(t, first[index].pool, second[index].pool)
		}
	})

	t.Run("SkipsUnusableAddressesAndKeepsTheRest", func(t *testing.T) {
		pm := newTestPoolManager(t)

		pm.reconcile(snapshot(1,
			node("uuid-1", "127.0.0.1"),       // no port
			node("uuid-2", "127.0.0.1:0"),     // port out of range
			node("uuid-3", "127.0.0.1:70000"), // port out of range
			node("uuid-4", "127.0.0.1:5433"),
		))

		// One unusable node must not cost the cluster the healthy ones.
		assert.Equal(t, []string{"uuid-4"}, uuidsOf(rotation(pm)))
	})

	t.Run("ClosesDepartedPools", func(t *testing.T) {
		pm := newTestPoolManager(t)

		var closes atomic.Int32
		pm.logger = slog.New(recordHandler{onRecord: func(msg string) {
			if msg == "pool closed" {
				closes.Add(1)
			}
		}})

		pm.reconcile(snapshot(1,
			node("uuid-1", "127.0.0.1:5433"),
			node("uuid-2", "127.0.0.1:5434"),
		))
		pm.reconcile(snapshot(2,
			node("uuid-1", "127.0.0.1:5433"),
			node("uuid-2", "127.0.0.1:5434", tpm.StateOffline),
		))

		pm.close()
		assert.Equal(t, int32(1), closes.Load())
	})

	t.Run("ReconcileAfterCloseIsRefused", func(t *testing.T) {
		pm := newTestPoolManager(t)
		pm.reconcile(snapshot(1, node("uuid-1", "127.0.0.1:5433")))
		pm.close()

		assert.Equal(t, 0, pm.reconcile(snapshot(2, node("uuid-2", "127.0.0.1:5434"))))
		assert.Equal(t, 0, pm.PoolSize())
		assert.Nil(t, pm.pick(0))
	})

	t.Run("StaleSnapshotIsIgnored", func(t *testing.T) {
		pm := newTestPoolManager(t)

		pm.reconcile(snapshot(5,
			node("uuid-1", "127.0.0.1:5433"),
			node("uuid-2", "127.0.0.1:5434"),
		))
		pm.reconcile(snapshot(4, node("uuid-1", "127.0.0.1:5433")))

		assert.ElementsMatch(t, []string{"uuid-1", "uuid-2"}, uuidsOf(rotation(pm)))
		assert.Equal(t, 2, pm.PoolSize())

		// The same version is not stale: sources may reuse one.
		assert.Equal(t, 1, pm.reconcile(snapshot(5, node("uuid-1", "127.0.0.1:5433"))))
	})

	t.Run("CloseReleasesTheLockWhileClosingPools", func(t *testing.T) {
		// A listener that accepts and stays silent leaves pgx in the startup
		// handshake, and puddle counts a resource from the moment it starts
		// building one - so Pool.Close blocks without needing a cluster.
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		accepted := make(chan net.Conn, 4)
		t.Cleanup(func() {
			// Closing the listener stops the accept goroutine; the channel is
			// left open because that goroutine may still be mid-send.
			_ = ln.Close()

			for {
				select {
				case conn := <-accepted:
					_ = conn.Close()
				default:
					return
				}
			}
		})

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
		require.Equal(t, 1, pm.reconcile(snapshot(1, node("uuid-1", ln.Addr().String()))))

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

		// The rotation is emptied under the lock, so an empty read means close()
		// is past that point and now draining.
		deadline := time.Now().Add(5 * time.Second)
		for pm.PoolSize() != 0 {
			if time.Now().After(deadline) {
				t.Fatal("close() never emptied the rotation")
			}

			time.Sleep(10 * time.Millisecond)
		}

		assert.Nil(t, pm.pick(0))

		// Probe pm.mu itself: PoolSize is lock-free and proves nothing here.
		// A close() holding the lock across the drain never lets this return.
		reconciled := make(chan struct{})
		go func() {
			defer close(reconciled)

			pm.reconcile(snapshot(2, node("uuid-2", "127.0.0.1:5433")))
		}()

		select {
		case <-reconciled:
		case <-time.After(5 * time.Second):
			t.Fatal("close() held pm.mu while waiting for the pools to close")
		}

		require.NoError(t, held.Close())

		select {
		case <-closed:
		case <-time.After(30 * time.Second):
			t.Fatal("close() did not return after the connection was released")
		}
	})

	t.Run("ClosesPoolsConcurrently", func(t *testing.T) {
		// Needs real connections: Pool.Close only blocks while one is checked
		// out, and that is what makes serial closing observable.
		testenv.RequireCluster(t)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		cluster := testenv.WaitReady(t, ctx, 2, 1)

		cfg, err := pgxpool.ParseConfig(testenv.URI())
		require.NoError(t, err)

		pm := newPoolManager(cfg, 2)
		require.GreaterOrEqual(t, pm.reconcile(testSnapshot{cluster}), 2)

		pools := rotation(pm)

		held := make([]*pgxpool.Conn, 0, len(pools))
		for _, inst := range pools {
			conn, err := inst.pool.Acquire(ctx)
			require.NoError(t, err)

			held = append(held, conn)
		}

		closed := make(chan struct{})
		go func() {
			pm.close()
			close(closed)
		}()

		// Every pool refuses new work while the connections are still held.
		// Closed one after another, only the first would.
		for i, inst := range pools {
			require.Eventually(t, func() bool {
				_, err := inst.pool.Acquire(ctx)

				// Matched by text, not by identity: the value lives in puddle,
				// and pgxpool returns it without wrapping or re-exporting it.
				return err != nil && strings.Contains(err.Error(), "closed pool")
			}, 20*time.Second, 20*time.Millisecond, "pool %d was not closing", i)
		}

		for _, conn := range held {
			conn.Release()
		}

		select {
		case <-closed:
		case <-time.After(30 * time.Second):
			t.Fatal("close() did not return after the connections were released")
		}
	})

	t.Run("ClosesRealConnection", func(t *testing.T) {
		testenv.RequireCluster(t)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		cluster := testenv.WaitReady(t, ctx, 1, 1)

		var target tpm.NodeInfo
		for _, n := range cluster.Nodes {
			if n.State == tpm.StateOnline {
				target = n

				break
			}
		}

		require.NotEmpty(t, target.ID, "the test cluster must expose an online node")

		cfg, err := pgxpool.ParseConfig(testenv.URI())
		require.NoError(t, err)

		pm := newPoolManager(cfg, 1)
		require.Equal(t, 1, pm.reconcile(snapshot(1, target)))

		pool := rotation(pm)[0].pool
		_, err = pool.Exec(ctx, "SELECT 1")
		require.NoError(t, err)

		pm.reconcile(snapshot(2, node(target.ID, target.Address, tpm.StateOffline)))

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

		assert.True(t, closed, "pool should be closed after the node left the snapshot")
		assert.Equal(t, 0, pm.PoolSize())
		assert.Empty(t, rotation(pm))
	})
}

func TestPingWithoutPools(t *testing.T) {
	pm := newTestPoolManager(t)

	// A client with no pools is not healthy, so a health check must not pass.
	assert.ErrorIs(t, pm.ping(context.Background()), ErrNoConnections)

	pm.reconcile(snapshot(1, node("uuid-1", "127.0.0.1:5433")))
	pm.close()

	assert.ErrorIs(t, pm.ping(context.Background()), ErrNoConnections)
}

func TestCreatePoolInvalidAddress(t *testing.T) {
	pm := newTestPoolManager(t)

	t.Run("MissingPort", func(t *testing.T) {
		_, err := pm.createPool("uuid-1", "127.0.0.1")
		assert.Error(t, err)
	})

	t.Run("PortBoundariesAreValid", func(t *testing.T) {
		for _, address := range []string{"127.0.0.1:1", "127.0.0.1:65535"} {
			inst, err := pm.createPool("uuid-1", address)

			require.NoError(t, err, address)
			assert.Equal(t, address, inst.addr)

			// createPool does not publish, so the manager's cleanup never sees it.
			inst.pool.Close()
		}
	})

	t.Run("PortOutOfRange", func(t *testing.T) {
		for _, address := range []string{"127.0.0.1:0", "127.0.0.1:70000"} {
			_, err := pm.createPool("uuid-1", address)

			// An out-of-range port has no cause to wrap, so no nil-%w artefact.
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "%!w")
			assert.NoError(t, errors.Unwrap(err))
		}
	})
}
