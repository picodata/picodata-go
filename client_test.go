package picodata

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newEmptyPool(t *testing.T) *Client {
	t.Helper()
	pool := newMockPool("127.0.0.1", 5432)
	pm := newPoolManager(pool, 1)
	pm.removePool("127.0.0.1:5432")
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
