package picodata

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"git.picodata.io/core/picodata-go/logger"
	"git.picodata.io/core/picodata-go/strategies"

	"github.com/jackc/pgx/v5/pgxpool"
)

type connectionProvider struct {
	mu                sync.RWMutex
	current           uint64
	connectionsConfig *pgxpool.Config
	connections       []*pgxpool.Pool
	// Key: instance address
	// Value: index of corresponding *pgxpool.Pool in [connectionProvider] connections slice
	connectionsMap  map[string]int
	balanceStrategy strategies.BalanceStrategy
}

func newConnectionProvider(initConn *pgxpool.Pool) *connectionProvider {
	connPool := make([]*pgxpool.Pool, 0, 1)
	connMap := make(map[string]int, 1)

	connPool = append(connPool, initConn)
	addr := fmt.Sprintf("%s:%d", initConn.Config().ConnConfig.Host, initConn.Config().ConnConfig.Port)
	connMap[addr] = 0

	return &connectionProvider{
		current:           0,
		connectionsConfig: initConn.Config().Copy(),
		connections:       connPool,
		connectionsMap:    connMap,
		balanceStrategy:   strategies.NewRoundRobinStrategy(),
	}
}

func (p *connectionProvider) setBalanceStrategy(s strategies.BalanceStrategy) {
	p.balanceStrategy = s
}

func (p *connectionProvider) nextConnection() *pgxpool.Pool {
	const op = "provider: nextConnection"

	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.connections) == 0 {
		logger.Log(logger.LevelWarn, "%s: connections slice is empty", op)
		return nil
	}

	index := p.balanceStrategy.Next(&p.current, uint64(len(p.connections)))

	return p.connections[index]
}

func (p *connectionProvider) config() *pgxpool.Config {
	return p.connectionsConfig.Copy()
}

func (p *connectionProvider) conns() []*pgxpool.Pool {
	return p.connections
}

func (p *connectionProvider) connsMap() map[string]*pgxpool.Pool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	connectionsMap := make(map[string]*pgxpool.Pool, len(p.connectionsMap))
	for address, index := range p.connectionsMap {
		connectionsMap[address] = p.connections[index]
	}

	return connectionsMap
}

func (p *connectionProvider) addConn(address string) error {
	const op = "provider: addConn"

	p.mu.Lock()
	defer p.mu.Unlock()

	// If connection already in pool - return
	if _, exists := p.connectionsMap[address]; exists {
		return nil
	}

	// Create a new config for connection
	connCfg := p.connectionsConfig.Copy()
	hostAndPort := strings.Split(address, ":")
	connCfg.ConnConfig.Host = hostAndPort[0]

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
	defer p.mu.Unlock()

	// If connection with address doesn't exist -> return
	index, ok := p.connectionsMap[address]
	if !ok {
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

	logger.Log(logger.LevelDebug, "%s: %s", op, address)
}
