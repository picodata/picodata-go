package picodata

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"git.picodata.io/core/drivers/picodata-go/logger"
	"git.picodata.io/core/drivers/picodata-go/strategies"
	tpm "git.picodata.io/core/drivers/picodata-go/topology"

	"github.com/jackc/pgx/v5/pgxpool"
)

type connectionProvider struct {
	mu                sync.RWMutex
	current           uint64
	connectionsConfig *pgxpool.Config
	connections       []*pgxpool.Pool
	// Key: instance address
	// Value: index of corresponding *pgxpool.Pool in [connectionProvider] connections slice
	connectionsMap        map[string]int
	balanceStrategy       strategies.BalanceStrategy
	connectionPerInstance int32
}

func newConnectionProvider(initConn *pgxpool.Pool, connPerInstance int32) *connectionProvider {
	connPool := make([]*pgxpool.Pool, 0, 1)
	connMap := make(map[string]int, 1)

	connPool = append(connPool, initConn) //
	addr := fmt.Sprintf("%s:%d", initConn.Config().ConnConfig.Host, initConn.Config().ConnConfig.Port)
	connMap[addr] = 0

	return &connectionProvider{
		current:               0,
		connectionsConfig:     initConn.Config().Copy(),
		connections:           connPool,
		connectionsMap:        connMap,
		balanceStrategy:       strategies.NewRoundRobinStrategy(),
		connectionPerInstance: connPerInstance,
	}
}

func (p *connectionProvider) setBalanceStrategy(s strategies.BalanceStrategy) {
	// NOTE: we use this function only in init stage, but
	// -race flag will complain about using provider in different goroutines
	// so the mutex is essential here.
	p.mu.Lock()
	defer p.mu.Unlock()

	p.balanceStrategy = s
}

func (p *connectionProvider) config() *pgxpool.Config {
	return p.connectionsConfig.Copy()
}

func (p *connectionProvider) PoolSize() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	l := len(p.connectionsMap)

	return l
}

func (p *connectionProvider) nextConnection() *pgxpool.Pool {
	p.mu.RLock()

	if len(p.connections) == 0 {
		p.mu.RUnlock()
		logger.Log(logger.LevelWarn, "[provider] connection not found in a pool")
		return nil
	}

	index := p.balanceStrategy.Next(&p.current, uint64(len(p.connections)))

	// Deref not used because it descrise perfomance ~15%
	// ---------------------------------------
	// Current solution   358411162   3.205 ns/op
	// Defer solution     318601726   3.783 ns/op
	p.mu.RUnlock()

	return p.connections[index]
}

func (p *connectionProvider) addConn(address string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Create a new config for connection
	connCfg := p.connectionsConfig.Copy()
	hostAndPort := strings.Split(address, ":")
	connCfg.ConnConfig.Host = hostAndPort[0]
	if p.connectionPerInstance != 0 {
		connCfg.MaxConns = int32(p.connectionPerInstance)
	}

	// Convert port from string to integer
	port, err := strconv.Atoi(hostAndPort[1])
	if err != nil {
		return fmt.Errorf("parse port error [port: %s]: %w", hostAndPort[1], err)
	}

	// Picodata provides valid port, so casting is safe
	connCfg.ConnConfig.Port = uint16(port)

	conn, err := pgxpool.NewWithConfig(context.Background(), connCfg)
	if err != nil {
		return err
	}

	// Add connection to the connection pool
	p.connections = append(p.connections, conn)
	p.connectionsMap[address] = len(p.connections) - 1

	logger.Log(logger.LevelDebug, "[provider] a connection to address %s was appended", address)

	return nil
}

func (p *connectionProvider) removeConn(address string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// If connection with address doesn't exist -> return
	index, ok := p.connectionsMap[address]
	if !ok {
		logger.Log(logger.LevelWarn, "[provider] a connection for address %s not found", address)
		return
	}

	// Get address of last connection in pool
	lastConnAddr := fmt.Sprintf("%s:%d", p.connections[len(p.connections)-1].Config().ConnConfig.Host, p.connections[len(p.connections)-1].Config().ConnConfig.Port)

	// Remove entry about connection from connMap
	delete(p.connectionsMap, address)

	// If deleted connection isn't last in pool
	if index != len(p.connections)-1 {
		// Move last connection to the position of deleted one
		p.connections[index] = p.connections[len(p.connections)-1]
		// Update index of last connection in connMap
		p.connectionsMap[lastConnAddr] = index
	}
	// Delete connection from connSlice by truncating it
	p.connections = p.connections[:len(p.connections)-1]

	logger.Log(logger.LevelDebug, "[provider] a connection to address %s was removed", address)
}

func (p *connectionProvider) ping(ctx context.Context) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, pool := range p.connections {
		if err := pingPool(ctx, pool); err != nil {
			return err
		}
	}
	return nil
}

func (p *connectionProvider) close() {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, pool := range p.connections {
		pool.Close()
	}
}

func (p *connectionProvider) reset() {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, pool := range p.connections {
		pool.Reset()
	}
}

func pingPool(ctx context.Context, p *pgxpool.Pool) error {
	c, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer c.Release()

	if _, err = c.Exec(ctx, "SELECT 1"); err != nil {
		return fmt.Errorf("ping pool error: %w", err)
	}

	return nil
}

func (p *connectionProvider) OnTopologyChanged(topology *tpm.ClusterTopology) {
	added := 0

	for _, nodeInfo := range topology.Nodes {
		switch nodeInfo.State {
		case tpm.StateOnline:
			if _, ok := p.connectionsMap[nodeInfo.Address]; !ok {
				if err := p.addConn(nodeInfo.Address); err != nil {
					logger.Log(logger.LevelError, "[provider] failed to add connection [address: %s]: %v", nodeInfo.Address, err)
					continue
				}
				added++
			}
		case tpm.StateOffline:
			p.removeConn(nodeInfo.Address)
		default:
		}
	}

	logger.Log(logger.LevelDebug, "[provider] discovered %d instances, added %d connections", len(topology.Nodes), added)
}
