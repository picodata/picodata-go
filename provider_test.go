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
	return "Mock"
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

	t.Run("TestSetBalanceStrategy", func(t *testing.T) {
		pool1 := newMockPool("127.0.0.1", 5432)
		pool2 := newMockPool("127.0.0.1", 5433)

		prov := newConnectionProvider(pool1)
		prov.connections = append(prov.connections, pool2)

		prov.setBalanceStrategy(mockBalancerStrategy{})
		// Always return second connection
		for range 5 {
			assert.Equal(t, pool2, prov.nextConnection())
		}
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

	t.Run("TestNextConnectionConcurrent", func(t *testing.T) {
		addr := "addr"
		ports := []int{0, 1, 2}
		host1 := fmt.Sprintf("%s:%d", addr, ports[0])
		host2 := fmt.Sprintf("%s:%d", addr, ports[1])
		host3 := fmt.Sprintf("%s:%d", addr, ports[2])

		pool1 := newMockPool(addr, ports[0])
		pool2 := newMockPool(addr, ports[1])
		pool3 := newMockPool(addr, ports[2])

		prov := newConnectionProvider(pool1)
		prov.connections = append(prov.connections, pool2)
		prov.connectionsMap[host2] = 1
		prov.connections = append(prov.connections, pool3)
		prov.connectionsMap[host3] = 2

		goroutines := 200

		wg := sync.WaitGroup{}

		wg.Add(goroutines)
		for range goroutines {
			go func() {
				defer wg.Done()
				conn := prov.nextConnection()
				assert.NotNil(t, conn)
			}()
		}

		type action struct {
			host   string
			action string
		}
		removeAction := "remove"
		addAction := "add"
		actions := []action{{host3, removeAction}, {host2, removeAction}, {host3, addAction}, {host1, removeAction}}

		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, action := range actions {
				switch action.action {
				case removeAction:
					switch action.host {
					case host1:
						prov.removeConn(host1)
					case host2:
						prov.removeConn(host2)
					case host3:
						prov.removeConn(host3)
					}
				case addAction:
					switch action.host {
					case host1:
						prov.addConn(host1)
					case host2:
						prov.addConn(host2)
					case host3:
						prov.addConn(host3)
					}
				}
			}
		}()

		wg.Wait()

		assert.Len(t, prov.conns(), 1)
		assert.Equal(t, fmt.Sprintf("%s:%d", prov.conns()[0].Config().ConnConfig.Host, prov.conns()[0].Config().ConnConfig.Port), host3)
		assert.Len(t, prov.connsMap(), 1)
		_, ok := prov.connsMap()[host3]
		assert.True(t, ok)
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

	t.Run("TestRemoveConnectionLastPosition", func(t *testing.T) {
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

	t.Run("TestRemoveConnectionMiddlePosition", func(t *testing.T) {
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
}
