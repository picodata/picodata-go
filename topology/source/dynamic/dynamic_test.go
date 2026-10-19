package dynamic_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"git.picodata.io/core/drivers/picodata-go/internal/testenv"
	"git.picodata.io/core/drivers/picodata-go/topology/source/dynamic"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Consumers drop reports older than the last applied, so the version has to grow
// on its own rather than follow a clock.
func TestFetchVersionIsMonotonic(t *testing.T) {
	testenv.RequireCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	testenv.WaitReady(t, ctx, 1, 1)

	cfg, err := pgx.ParseConfig(testenv.URI())
	require.NoError(t, err)

	source := dynamic.New(cfg, []string{net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port))})

	first, err := source.Fetch(ctx)
	require.NoError(t, err)

	second, err := source.Fetch(ctx)
	require.NoError(t, err)

	assert.Greater(t, second.Version, first.Version)
	assert.Equal(t, first.Version+1, second.Version, "the version counts fetches")
}

type queryCounter struct {
	count int
}

func (c *queryCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	c.count++
	return ctx
}

func (*queryCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestFetchTopology(t *testing.T) {
	testenv.RequireCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	testenv.WaitReady(t, ctx, 2, 2)

	cfg, err := pgx.ParseConfig(testenv.URI())
	require.NoError(t, err)
	counter := &queryCounter{}
	cfg.Tracer = counter
	source := dynamic.New(cfg, []string{net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port))})
	got, err := source.Fetch(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, counter.count, "one Fetch must read all metadata in one query")
	require.NotEmpty(t, got.Nodes)
	require.GreaterOrEqual(t, len(got.Replicasets), 2)
	assert.Equal(t, map[string]uint64{"default": 3000, "arbiter": 3000}, got.BucketCounts)
	for id, node := range got.Nodes {
		assert.Equal(t, id, node.ID)
		require.Contains(t, got.Replicasets, node.ReplicasetID)
		assert.Equal(t, node.Tier, got.Replicasets[node.ReplicasetID].Tier)
		require.Contains(t, got.BucketCounts, node.Tier)
	}
	for id, replicaset := range got.Replicasets {
		assert.Equal(t, id, replicaset.ID)
		require.NotEmpty(t, replicaset.CurrentMasterID)
		assert.Contains(t, got.Nodes, replicaset.CurrentMasterID)
		assert.Contains(t, got.BucketCounts, replicaset.Tier)
	}
}

func TestFetchFailureDoesNotAdvanceVersion(t *testing.T) {
	testenv.RequireCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	testenv.WaitReady(t, ctx, 1, 1)

	cfg, err := pgx.ParseConfig(testenv.URI())
	require.NoError(t, err)
	source := dynamic.New(cfg, []string{net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port))})
	first, err := source.Fetch(ctx)
	require.NoError(t, err)

	failedCtx, stop := context.WithCancel(ctx)
	stop()
	failed, err := source.Fetch(failedCtx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, failed)

	next, err := source.Fetch(ctx)
	require.NoError(t, err)
	assert.Equal(t, first.Version+1, next.Version)
}

func TestFetchTriesNextHost(t *testing.T) {
	testenv.RequireCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	testenv.WaitReady(t, ctx, 1, 1)

	cfg, err := pgx.ParseConfig(testenv.URI())
	require.NoError(t, err)
	source := dynamic.New(cfg, []string{"127.0.0.1:1", net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port))})
	got, err := source.Fetch(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, got.Nodes)
	assert.NotEmpty(t, got.Replicasets)
	assert.NotEmpty(t, got.BucketCounts)
}
