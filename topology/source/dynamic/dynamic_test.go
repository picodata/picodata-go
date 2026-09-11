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
