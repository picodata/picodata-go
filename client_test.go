package picodata

import (
	"context"
	"net/url"
	"testing"
	"time"

	"git.picodata.io/core/drivers/picodata-go/internal/testenv"
	"git.picodata.io/core/drivers/picodata-go/topology"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type topologySourceFunc func(context.Context) (*topology.ClusterTopology, error)

func (f topologySourceFunc) Fetch(ctx context.Context) (*topology.ClusterTopology, error) {
	return f(ctx)
}

type topologyListenerFunc func(topology.Snapshot)

func (f topologyListenerFunc) OnTopologyChanged(snapshot topology.Snapshot) { f(snapshot) }

func TestClientEmptyPoolErrors(t *testing.T) {
	for _, tt := range []struct {
		name      string
		nodes     []topology.NodeInfo
		forbidden []string
		wantError string
	}{
		{"EmptyTopology", nil, nil, "no online nodes discovered"},
		{"OfflineNodesWithFilter", []topology.NodeInfo{
			{ID: "node-1", Tier: "arbiter", State: topology.StateOffline},
			{ID: "node-2", Tier: "default", State: topology.StateOffline},
		}, []string{"arbiter"}, "no online nodes discovered"},
		{
			"NoOnlineNodes",
			[]topology.NodeInfo{node("node-1", "localhost:5432", topology.StateOffline)},
			nil,
			"no online nodes discovered",
		},
		{"AllOnlineNodesFiltered", []topology.NodeInfo{
			{ID: "node-1", Tier: "arbiter", Address: "localhost:5432", State: topology.StateOnline},
			{ID: "node-2", Tier: "default", Address: "localhost:5432", State: topology.StateOffline},
		}, []string{"readonly", "arbiter"}, "no online nodes available after applying filters"},
		{"UnknownStateIsNotAllowedOnline", []topology.NodeInfo{
			{ID: "excluded", Tier: "arbiter", State: topology.StateOnline},
			{ID: "unknown", Tier: "default", Address: "localhost:5432"},
		}, []string{"arbiter"}, "no online nodes available after applying filters"},
		{"PoolCreationFailed", []topology.NodeInfo{
			node("node-1", "invalid-address-1"),
			node("node-2", "invalid-address-2"),
			{ID: "node-3", Tier: "arbiter", Address: "localhost:5432", State: topology.StateOnline},
			node("node-4", "localhost:5432", topology.StateOffline),
		}, []string{"arbiter"}, "failed to open a pool for any allowed online node, see the log for per-node errors"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseConfig("postgres://localhost:5432")
			require.NoError(t, err)
			source := topologySourceFunc(func(context.Context) (*topology.ClusterTopology, error) {
				return snapshot(1, tt.nodes...).Clone(), nil
			})
			client, err := NewWithConfig(context.Background(), config,
				WithTopologySource(source), WithForbiddenTiers(tt.forbidden...))

			assert.Nil(t, client)
			assert.EqualError(t, err, tt.wantError)
		})
	}
}

func TestTopologyUpdateIntervalIgnoredWhenUpdatesDisabled(t *testing.T) {
	config, err := ParseConfig("postgres://localhost:5432")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// This interval would fail validation if polling were enabled.
	interval := time.Second - time.Nanosecond
	for _, tt := range []struct {
		name     string
		opts     []PoolOption
		disabled bool
	}{
		{"UpdatesEnabled", []PoolOption{WithTopologyUpdateInterval(interval)}, false},
		{"DisableFirst", []PoolOption{WithDisableTopologyUpdate(), WithTopologyUpdateInterval(interval)}, true},
		{"DisableLast", []PoolOption{WithTopologyUpdateInterval(interval), WithDisableTopologyUpdate()}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewWithConfig(ctx, config, tt.opts...)
			assert.Nil(t, client)
			if tt.disabled {
				assert.ErrorIs(t, err, context.Canceled)
			} else {
				assert.EqualError(t, err, "topology update interval must be at least one second")
			}
		})
	}
}

func TestPoolNoConnections(t *testing.T) {
	ctx := context.Background()

	t.Run("Query", func(t *testing.T) {
		p := newEmptyPool(t)
		var (
			rows pgx.Rows
			err  error
		)
		require.NotPanics(t, func() {
			rows, err = p.Query(ctx, "SELECT 1")
		})
		assert.Nil(t, rows)
		assert.Error(t, err)
	})

	t.Run("QueryRow", func(t *testing.T) {
		p := newEmptyPool(t)
		var err error
		require.NotPanics(t, func() {
			row := p.QueryRow(ctx, "SELECT 1")
			var v int
			err = row.Scan(&v)
		})
		assert.Error(t, err)
	})

	t.Run("Exec", func(t *testing.T) {
		p := newEmptyPool(t)
		var err error
		require.NotPanics(t, func() {
			_, err = p.Exec(ctx, "SELECT 1")
		})
		assert.Error(t, err)
	})

	t.Run("SendBatch", func(t *testing.T) {
		p := newEmptyPool(t)
		batch := &pgx.Batch{}
		var results pgx.BatchResults
		require.NotPanics(t, func() {
			results = p.SendBatch(ctx, batch)
		})
		require.NotNil(t, results)
		assert.Error(t, results.Close())
	})
}

func TestClientForbiddenTiers(t *testing.T) {
	testenv.RequireCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// The pool is built from the first topology snapshot, so both tiers have to
	// be online before any of this can be asserted on.
	cluster := testenv.WaitReady(t, ctx, 2, 2)

	t.Run("WithoutFilterEveryTierIsPooled", func(t *testing.T) {
		client, err := New(ctx, testenv.URI())
		require.NoError(t, err)
		defer client.Close()

		arbiters := tierNodeIDs(t, cluster, "arbiter")
		require.NotEmpty(t, arbiters, "the test cluster must expose an arbiter tier")

		assert.Subset(t, uuidsOf(*client.pm.instances.Load()), arbiters)
	})

	t.Run("ForbiddenTierIsNotPooled", func(t *testing.T) {
		arbiters := tierNodeIDs(t, cluster, "arbiter")
		allowed := tierNodeIDs(t, cluster, "default")

		// The test cluster contains both tiers, but the pool must exclude arbiters.
		require.NotEmpty(t, arbiters, "the test cluster must expose an arbiter tier")
		require.NotEmpty(t, allowed)

		// Discovery may connect to a forbidden tier even though user pools may not.
		uri, err := url.Parse(testenv.URI())
		require.NoError(t, err)
		uri.Host = cluster.Nodes[arbiters[0]].Address
		client, err := New(ctx, uri.String(), WithForbiddenTiers("arbiter"))
		require.NoError(t, err)
		defer client.Close()

		pooled := uuidsOf(*client.pm.instances.Load())
		snapshots := make(chan topology.Snapshot, 1)
		client.tpm.AddListener(topologyListenerFunc(func(snapshot topology.Snapshot) {
			select {
			case snapshots <- snapshot:
			default:
			}
		}))
		full := (<-snapshots).Clone()

		// Not NotSubset: that passes as soon as one arbiter is missing, and
		// every one of them has to be absent.
		for _, id := range arbiters {
			assert.NotContains(t, pooled, id)
			assert.Contains(t, full.Nodes, id)
		}
		assert.Equal(t, cluster.Replicasets, full.Replicasets)
		assert.Equal(t, cluster.BucketCounts, full.BucketCounts)

		assert.Subset(t, pooled, allowed)
		assert.Equal(t, len(allowed), client.pm.PoolSize())

		var one int
		require.NoError(t, client.QueryRow(ctx, "SELECT 1").Scan(&one))
		assert.Equal(t, 1, one)
	})

	t.Run("EveryTierForbiddenFailsFast", func(t *testing.T) {
		client, err := New(ctx, testenv.URI(), WithForbiddenTiers("default", "arbiter"))

		require.Error(t, err)
		assert.Nil(t, client)
		assert.EqualError(t, err, "no online nodes available after applying filters")
	})

	t.Run("RepeatedForbiddenTierOptionsAccumulate", func(t *testing.T) {
		// Each option adds tiers to exclude rather than replacing the earlier ones.
		client, err := New(ctx, testenv.URI(), WithForbiddenTiers("default"), WithForbiddenTiers("arbiter"))

		require.Error(t, err)
		assert.Nil(t, client)
		assert.EqualError(t, err, "no online nodes available after applying filters")
	})
}
