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
	connectionsMap    map[string]int
	balanceStrategy   strategies.BalanceStrategy
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
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.connections) == 0 {
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
	for name, index := range p.connectionsMap {
		connectionsMap[name] = p.connections[index]
	}

	return connectionsMap
}

func (p *connectionProvider) addConn(address string) error {
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
		return fmt.Errorf("provider: addConn: cant convert port %s to int: %v", hostAndPort[1], err)
	}

	connCfg.ConnConfig.Port = uint16(port)

	conn, err := pgxpool.NewWithConfig(context.Background(), connCfg)
	if err != nil {
		return err
	}

	p.connections = append(p.connections, conn)
	p.connectionsMap[address] = len(p.connections) - 1
	logger.Log(logger.LevelDebug, "provider: addConn: %s", address)

	return nil
}

func (p *connectionProvider) removeConn(address string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// If connection address doesn't exists -> return
	index, ok := p.connectionsMap[address]
	if !ok {
		return
	}

	// Get last connection address
	lastConnAddr := fmt.Sprintf("%s:%d", p.connections[len(p.connections)-1].Config().ConnConfig.Host, p.connections[len(p.connections)-1].Config().ConnConfig.Port)

	// Remove connection info from connMap
	delete(p.connectionsMap, address)

	if index != len(p.connections)-1 {
		// Delete connection from connSlice by truncating it
		p.connections[index] = p.connections[len(p.connections)-1]
		// Update connMap
		p.connectionsMap[lastConnAddr] = index
	}

	p.connections = p.connections[:len(p.connections)-1]

	logger.Log(logger.LevelDebug, "provider: removeConn: %s", address)
}
