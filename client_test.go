package picodata

import (
	"context"
	"testing"
	"time"

	"git.picodata.io/core/drivers/picodata-go/internal/testenv"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	testenv.WaitReady(t, ctx, 2, 2)

	t.Run("WithoutFilterEveryTierIsPooled", func(t *testing.T) {
		client, err := New(ctx, testenv.URI())
		require.NoError(t, err)
		defer client.Close()

		arbiters := tierNodeIDs(t, client.tpm.GetTopology(), "arbiter")
		require.NotEmpty(t, arbiters, "the test cluster must expose an arbiter tier")

		assert.Subset(t, uuidsOf(*client.pm.instances.Load()), arbiters)
	})

	t.Run("ForbiddenTierIsNotPooled", func(t *testing.T) {
		client, err := New(ctx, testenv.URI(), WithForbiddenTiers("arbiter"))
		require.NoError(t, err)
		defer client.Close()

		cluster := client.tpm.GetTopology()
		arbiters := tierNodeIDs(t, cluster, "arbiter")
		allowed := tierNodeIDs(t, cluster, "default")

		// The raw topology keeps tracking the hidden nodes, so the driver still
		// notices when an allowed node comes back.
		require.NotEmpty(t, arbiters, "the test cluster must expose an arbiter tier")
		require.NotEmpty(t, allowed)

		pooled := uuidsOf(*client.pm.instances.Load())

		// Not NotSubset: that passes as soon as one arbiter is missing, and
		// every one of them has to be absent.
		for _, id := range arbiters {
			assert.NotContains(t, pooled, id)
		}

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
		assert.Contains(t, err.Error(), "forbidden tiers filter")
	})
}
