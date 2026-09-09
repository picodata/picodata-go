package picodata

import (
	"context"
	"testing"
	"time"

	"git.picodata.io/core/drivers/picodata-go/internal/testenv"
	"git.picodata.io/core/drivers/picodata-go/topology"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tierNodeIDs splits the online nodes of a topology by tier. The pool manager
// is keyed by node uuid, so the ids are what the assertions below look up.
func tierNodeIDs(t *testing.T, cluster *topology.ClusterTopology, tier string) []string {
	t.Helper()

	var ids []string
	for _, node := range cluster.Nodes {
		if node.State == topology.StateOnline && node.Tier == tier {
			ids = append(ids, node.ID)
		}
	}

	return ids
}

func newEmptyPool(t *testing.T) *Client {
	t.Helper()
	pm := newPoolManager(newMockConfig("127.0.0.1", 5432), 1)
	return &Client{pools: pm, router: newRouter(pm)}
}

func TestPoolNoConnections(t *testing.T) {
	ctx := context.Background()

	t.Run("TestQuery", func(t *testing.T) {
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

	t.Run("TestQueryRow", func(t *testing.T) {
		p := newEmptyPool(t)
		var err error
		require.NotPanics(t, func() {
			row := p.QueryRow(ctx, "SELECT 1")
			var v int
			err = row.Scan(&v)
		})
		assert.Error(t, err)
	})

	t.Run("TestExec", func(t *testing.T) {
		p := newEmptyPool(t)
		var err error
		require.NotPanics(t, func() {
			_, err = p.Exec(ctx, "SELECT 1")
		})
		assert.Error(t, err)
	})

	t.Run("TestSendBatch", func(t *testing.T) {
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
	testenv.WaitReady(t, ctx, 2, 2)

	t.Run("WithoutFilterEveryTierIsPooled", func(t *testing.T) {
		client, err := New(ctx, testenv.URI())
		require.NoError(t, err)
		defer client.Close()

		arbiters := tierNodeIDs(t, client.manager.GetTopology(), "arbiter")
		require.NotEmpty(t, arbiters, "the test cluster must expose an arbiter tier")

		for _, id := range arbiters {
			assert.Contains(t, client.pools.poolsMap, id)
		}
	})

	t.Run("ForbiddenTierIsNotPooled", func(t *testing.T) {
		client, err := New(ctx, testenv.URI(), WithForbiddenTiers("arbiter"))
		require.NoError(t, err)
		defer client.Close()

		cluster := client.manager.GetTopology()
		arbiters := tierNodeIDs(t, cluster, "arbiter")
		allowed := tierNodeIDs(t, cluster, "default")

		// The raw topology keeps tracking the hidden nodes, so the driver still
		// notices when an allowed node comes back.
		require.NotEmpty(t, arbiters, "the test cluster must expose an arbiter tier")
		require.NotEmpty(t, allowed)

		for _, id := range arbiters {
			assert.NotContains(t, client.pools.poolsMap, id)
		}

		for _, id := range allowed {
			assert.Contains(t, client.pools.poolsMap, id)
		}

		assert.Equal(t, len(allowed), client.pools.PoolSize())

		var one int
		require.NoError(t, client.QueryRow(ctx, "SELECT 1").Scan(&one))
		assert.Equal(t, 1, one)
	})

	t.Run("EveryTierForbiddenFailsFast", func(t *testing.T) {
		client, err := New(ctx, testenv.URI(), WithForbiddenTiers("default", "arbiter"))

		require.Error(t, err)
		assert.Nil(t, client)
		assert.Contains(t, err.Error(), "forbidden tiers filter")
	})
}
