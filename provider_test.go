package picodata

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockBalancerStrategy struct{}

func (ms mockBalancerStrategy) Next(current *uint64, size uint64) uint64 {
	return 1
}

func (ms mockBalancerStrategy) Type() string {
	return "MockStrat"
}

func newMockPool(host string, port int) *pgxpool.Pool {
	cfg, _ := pgxpool.ParseConfig("")
	cfg.ConnConfig.Host = host
	cfg.ConnConfig.Port = uint16(port)
	p, _ := pgxpool.NewWithConfig(context.Background(), cfg)
	return p
}

func TestProvider(t *testing.T) {
	t.Run("TestNewConnectionProvider", func(t *testing.T) {
		pool := newMockPool("127.0.0.1", 5432)
		prov := newConnectionProvider(pool)

		require.NotNil(t, prov)
		assert.Len(t, prov.conns(), 1)
		assert.NotNil(t, prov.connsMap())
		assert.Len(t, prov.connsMap(), 1)

	})

	t.Run("TestNextConection", func(t *testing.T) {
		pool1 := newMockPool("127.0.0.1", 5432)
		pool2 := newMockPool("127.0.0.1", 5433)

		prov := newConnectionProvider(pool1)
		// Because we can
		prov.connections = append(prov.connections, pool2)

		// Default strategy is RoundRobin
		assert.Equal(t, pool1, prov.nextConnection())
		assert.Equal(t, pool2, prov.nextConnection())
		assert.Equal(t, pool1, prov.nextConnection())
	})

	t.Run("TestAddConnectionAlreadyAdded", func(t *testing.T) {
		pool := newMockPool("127.0.0.1", 5432)
		prov := newConnectionProvider(pool)

		prov.addConn("127.0.0.1:5432")

		assert.Len(t, prov.conns(), 1)
	})

	t.Run("TestRemoveConnection", func(t *testing.T) {
		pool := newMockPool("127.0.0.1", 5432)
		prov := newConnectionProvider(pool)

		prov.removeConn("127.0.0.1:5432")
		assert.Len(t, prov.conns(), 0)
		assert.Len(t, prov.connsMap(), 0)
	})

	t.Run("TestRemoveConnectionLast", func(t *testing.T) {
		host := "host"
		ports := []int{0, 1, 2}

		pool1 := newMockPool(host, ports[0])
		pool2 := newMockPool(host, ports[1])
		pool3 := newMockPool(host, ports[2])
		prov := newConnectionProvider(pool1)
		prov.connections = append(prov.connections, pool2)
		prov.connectionsMap[fmt.Sprintf("%s:%d", host, ports[1])] = 1
		prov.connections = append(prov.connections, pool3)
		prov.connectionsMap[fmt.Sprintf("%s:%d", host, ports[2])] = 2

		prov.removeConn(fmt.Sprintf("%s:%d", host, ports[2]))
		assert.Len(t, prov.conns(), 2)
		assert.Len(t, prov.connsMap(), 2)

		for addr, index := range prov.connectionsMap {
			assert.Equal(t, addr, fmt.Sprintf("%s:%d", host, ports[index]))
		}
	})

	t.Run("TestRemoveConnectionMiddle", func(t *testing.T) {
		host := "host"
		ports := []int{0, 1, 2}

		pool1 := newMockPool(host, ports[0])
		pool2 := newMockPool(host, ports[1])
		pool3 := newMockPool(host, ports[2])
		prov := newConnectionProvider(pool1)
		prov.connections = append(prov.connections, pool2)
		prov.connectionsMap[fmt.Sprintf("%s:%d", host, ports[1])] = 1
		prov.connections = append(prov.connections, pool3)
		prov.connectionsMap[fmt.Sprintf("%s:%d", host, ports[2])] = 2

		prov.removeConn(fmt.Sprintf("%s:%d", host, ports[1]))
		assert.Len(t, prov.conns(), 2)
		assert.Len(t, prov.connsMap(), 2)
		assert.Equal(t, prov.connectionsMap[fmt.Sprintf("%s:%d", host, ports[0])], 0)
		assert.Equal(t, prov.connectionsMap[fmt.Sprintf("%s:%d", host, ports[2])], 1)
	})

	t.Run("TestSetBalanceStrategy", func(t *testing.T) {
		pool1 := newMockPool("127.0.0.1", 5432)
		pool2 := newMockPool("127.0.0.1", 5433)

		prov := newConnectionProvider(pool1)
		prov.connections = append(prov.connections, pool2)

		prov.setBalanceStrategy(mockBalancerStrategy{})
		// Always return second connection
		assert.Equal(t, pool2, prov.nextConnection())
		assert.Equal(t, pool2, prov.nextConnection())
	})

	t.Run("TestConcurrentnextConection", func(t *testing.T) {
		testPort1 := 0
		testPort2 := 1

		pool1 := newMockPool("127.0.0.1", testPort1)
		pool2 := newMockPool("127.0.0.1", testPort2)
		prov := newConnectionProvider(pool1)
		prov.connections = append(prov.connections, pool2)

		nOfWorkers := 1000
		results := make(chan uint16, nOfWorkers)

		var wg sync.WaitGroup
		for range nOfWorkers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn := prov.nextConnection()
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
		count1, count2 := 0, 0
		for _, port := range resultsSlice {
			switch port {
			case uint16(testPort1):
				count1++
			case uint16(testPort2):
				count2++
			}
		}

		// Verify each port is used exactly 500 times
		expected := nOfWorkers / len(prov.conns())
		if count1 != expected || count2 != expected {
			t.Errorf("Expected %d each, got %d: %d, %d: %d", expected, testPort1, count1, testPort2, count2)
		}
	})

}
