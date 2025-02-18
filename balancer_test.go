package picodata

import (
	"context"
	"sync"
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

func TestBalancer(t *testing.T) {
	t.Run("TestNewBalancer", func(t *testing.T) {
		pools := []*pgxpool.Pool{
			newMockPool("127.0.0.1", 5432),
			newMockPool("127.0.0.2", 5433),
		}
		bal := newBalancer(pools)

		require.NotNil(t, bal)
		assert.Len(t, bal.Conns(), 2)
		assert.NotNil(t, bal.ConnMap())

	})

	t.Run("TestNextConnection", func(t *testing.T) {
		pools := []*pgxpool.Pool{
			newMockPool("127.0.0.1", 5432),
			newMockPool("127.0.0.2", 5433),
		}
		bal := newBalancer(pools)

		// Default strategy is RoundRobin
		assert.Equal(t, pools[0], bal.NextConnection())
		assert.Equal(t, pools[1], bal.NextConnection())
		assert.Equal(t, pools[0], bal.NextConnection())
	})

	t.Run("TestAddConnection", func(t *testing.T) {
		pools := []*pgxpool.Pool{
			newMockPool("127.0.0.1", 5432),
		}
		bal := newBalancer(pools)
		newPool := newMockPool("127.0.0.1", 5433)
		bal.AddConn("127.0.0.1:5433", newPool)

		assert.Len(t, bal.Conns(), 2)
		assert.Equal(t, newPool, bal.ConnMap()["127.0.0.1:5433"])
	})

	t.Run("TestRemoveConnection", func(t *testing.T) {
		pools := []*pgxpool.Pool{
			newMockPool("127.0.0.1", 5432),
			newMockPool("127.0.0.1", 5433),
		}
		bal := newBalancer(pools)
		bal.RemoveConn("127.0.0.1:5432")
		assert.Len(t, bal.Conns(), 1)
		assert.Equal(t, bal.Conns()[0].Config().ConnConfig.Port, uint16(5433))
		assert.Nil(t, bal.ConnMap()["127.0.0.1:5432"])
	})
	t.Run("TestSetNextFunc", func(t *testing.T) {
		pools := []*pgxpool.Pool{
			newMockPool("127.0.0.1", 5432),
			newMockPool("127.0.0.2", 5433),
		}
		bal := newBalancer(pools)
		bal.SetBalanceStratFunc(func(current *uint64, size uint64) uint64 {
			return 1 // Always returns the second connection
		})

		assert.Equal(t, pools[1], bal.NextConnection())
		assert.Equal(t, pools[1], bal.NextConnection())
	})

	t.Run("TestConcurrentNextConnection", func(t *testing.T) {
		pools := []*pgxpool.Pool{
			newMockPool("127.0.0.1", 0),
			newMockPool("127.0.0.1", 1),
		}
		bal := newBalancer(pools)

		nOfWorkers := 1000
		results := make(chan uint16, nOfWorkers)

		var wg sync.WaitGroup
		for range nOfWorkers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn := bal.NextConnection()
				results <- conn.Config().ConnConfig.Port
			}()
		}

		go func() {
			wg.Wait()
			close(results)
		}()

		// Collect all results
		resultsSlice := make([]uint16, 0, nOfWorkers)
		for v := range results {
			resultsSlice = append(resultsSlice, v)
		}

		assert.Equal(t, len(resultsSlice), nOfWorkers)

		// Count occurrences of each port
		count0, count1 := 0, 0
		for _, port := range resultsSlice {
			switch port {
			case 0:
				count0++
			case 1:
				count1++
			}
		}

		// Verify each port is used exactly 500 times
		expected := nOfWorkers / len(pools)
		if count0 != expected || count1 != expected {
			t.Errorf("Expected %d each, got 0: %d, 1: %d", expected, count0, count1)
		}
	})

}
