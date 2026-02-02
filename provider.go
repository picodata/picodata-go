package picodata

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/picodata/picodata-go/logger"
	"github.com/picodata/picodata-go/strategies"

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

	connPool = append(connPool, initConn)
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
	p.balanceStrategy = s
	p.mu.Unlock()
}

func (p *connectionProvider) config() *pgxpool.Config {
	return p.connectionsConfig.Copy()
}

func (p *connectionProvider) conns() []*pgxpool.Pool {
	return p.connections
}

func (p *connectionProvider) connsMap() map[string]*pgxpool.Pool {
	p.mu.RLock()
	connectionsMap := make(map[string]*pgxpool.Pool, len(p.connectionsMap))
	for address, index := range p.connectionsMap {
		connectionsMap[address] = p.connections[index]
	}
	p.mu.RUnlock()

	return connectionsMap
}

func (p *connectionProvider) nextConnection() *pgxpool.Pool {
	const op = "provider: nextConnection"

	// NOTE: Ran benchmark with defered and sequential mutex
	// ---------------------------------------
	// NextConn        358411162   3.205 ns/op
	// NextConnDefer   318601726   3.783 ns/op
	p.mu.RLock()

	if len(p.connections) == 0 {
		logger.Log(logger.LevelWarn, "%s: connections slice is empty", op)
		return nil
	}

	index := p.balanceStrategy.Next(&p.current, uint64(len(p.connections)))

	p.mu.RUnlock()

	return p.connections[index]
}
func (p *connectionProvider) addConn(address string) error {
	const op = "provider: addConn"

	// NOTE: Ran benchmark with defered and sequential mutex
	// ---------------------------------------
	// AddConn        1000000   2610 ns/op
	// AddConnDefer   1000000   2057 ns/op
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
		return fmt.Errorf("%s: cant convert port %s to int: %w", op, hostAndPort[1], err)
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

	logger.Log(logger.LevelDebug, "%s: %s", op, address)

	return nil
}

func (p *connectionProvider) removeConn(address string) {
	const op = "provider: removeConn"

	p.mu.Lock()

	// If connection with address doesn't exist -> return
	index := p.connectionsMap[address]

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

	p.mu.Unlock()

	logger.Log(logger.LevelDebug, "%s: %s", op, address)
}
