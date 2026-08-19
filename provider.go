package picodata

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"

	tpm "git.picodata.io/core/drivers/picodata-go/topology"

	"github.com/jackc/pgx/v5/pgxpool"
)

type poolManager struct {
	logger *slog.Logger
	mu     sync.RWMutex
	config *pgxpool.Config
	pools  []*pgxpool.Pool
	// Key: instance address
	// Value: index of corresponding *pgxpool.Pool in [poolManager] pools slice
	poolsMap              map[string]int
	connectionPerInstance int32
}

type poolManagerOption func(*poolManager)

// withPoolManagerLogger takes the root logger and bakes the package-specific tag inside.
func withPoolManagerLogger(l *slog.Logger) poolManagerOption {
	return func(pm *poolManager) {
		pm.logger = l.With(slog.String("component", "pool_manager"))
	}
}

func newPoolManager(initPool *pgxpool.Pool, connPerInstance int32, opts ...poolManagerOption) *poolManager {
	pools := make([]*pgxpool.Pool, 0, 1)
	poolsIndex := make(map[string]int, 1)

	pools = append(pools, initPool)
	poolsIndex[poolAddr(initPool)] = 0

	pm := &poolManager{
		logger:                slog.New(slog.DiscardHandler),
		config:                initPool.Config().Copy(),
		pools:                 pools,
		poolsMap:              poolsIndex,
		connectionPerInstance: connPerInstance,
	}

	for _, opt := range opts {
		opt(pm)
	}

	return pm
}

func (pm *poolManager) PoolSize() int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	return len(pm.poolsMap)
}

func (pm *poolManager) getConfig() *pgxpool.Config {
	return pm.config.Copy()
}

// pick selects a pool under a single lock: indexFn receives the current pool
// count and returns the index to use, so the count it saw and the slice it
// indexes into can never be torn apart by a concurrent addPool/removePool.
//
// indexFn runs while pm.mu is held for reading, so it must not call back into
// the poolManager: sync.RWMutex is not reentrant, and a writer waiting on
// addPool/removePool would deadlock a nested RLock.
func (pm *poolManager) pick(indexFn func(size int) uint64) *pgxpool.Pool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	size := len(pm.pools)
	if size == 0 {
		return nil
	}

	index := indexFn(size)
	index %= uint64(size)

	return pm.pools[index]
}

func (pm *poolManager) addPool(address string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, ok := pm.poolsMap[address]; ok {
		pm.logger.Warn("already exists")
		return nil
	}

	hostAndPort := strings.Split(address, ":")
	if len(hostAndPort) != 2 {
		return fmt.Errorf("invalid address format: %q", address)
	}

	// Create a new config for connection
	connCfg := pm.config.Copy()
	connCfg.ConnConfig.Host = hostAndPort[0]
	if pm.connectionPerInstance != 0 {
		connCfg.MaxConns = int32(pm.connectionPerInstance)
	}

	// Convert port from string to integer
	port, err := strconv.Atoi(hostAndPort[1])
	if err != nil {
		return fmt.Errorf("parse port error [port: %s]: %w", hostAndPort[1], err)
	}

	// Picodata provides valid port, so casting is safe
	connCfg.ConnConfig.Port = uint16(port)

	pool, err := pgxpool.NewWithConfig(context.Background(), connCfg)
	if err != nil {
		return err
	}

	// Add pool to the pools slice
	pm.pools = append(pm.pools, pool)
	pm.poolsMap[address] = len(pm.pools) - 1

	pm.logger.Debug("a pool for address was appended", slog.Any("address", address))

	return nil
}

func (pm *poolManager) removePool(address string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// If pool for address doesn't exist -> return
	index, ok := pm.poolsMap[address]
	if !ok {
		pm.logger.Warn("a pool for address not found", slog.Any("address", address))
		return
	}

	// Get address of last pool
	lastPoolAddr := poolAddr(pm.pools[len(pm.pools)-1])

	// Remove entry about pool from poolsIndex
	delete(pm.poolsMap, address)

	// If deleted pool isn't last in the slice
	if index != len(pm.pools)-1 {
		// Move last pool to the position of deleted one
		pm.pools[index] = pm.pools[len(pm.pools)-1]
		// Update index of last pool in poolsIndex
		pm.poolsMap[lastPoolAddr] = index
	}
	// Delete pool from pools slice by truncating it
	pm.pools = pm.pools[:len(pm.pools)-1]

	pm.logger.Debug("a pool for address was removed", slog.Any("address", address))
}

func (pm *poolManager) close() {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	for _, pool := range pm.pools {
		pool.Close()
	}
}

func (pm *poolManager) ping(ctx context.Context) error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	for _, pool := range pm.pools {
		if err := pingPool(ctx, pool); err != nil {
			return err
		}
	}

	return nil
}

func (pm *poolManager) reset() {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	for _, pool := range pm.pools {
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

func poolAddr(p *pgxpool.Pool) string {
	return fmt.Sprintf("%s:%d", p.Config().ConnConfig.Host, p.Config().ConnConfig.Port)
}

func (pm *poolManager) OnTopologyChanged(topology *tpm.ClusterTopology) {
	for _, nodeInfo := range topology.Nodes {
		switch nodeInfo.State {
		case tpm.StateOnline:
			if err := pm.addPool(nodeInfo.Address); err != nil {
				pm.logger.Error("failed to add connection", slog.Any("addres", nodeInfo.Address), slog.Any("error", err))
				continue
			}
		case tpm.StateOffline:
			pm.removePool(nodeInfo.Address)
		default:
		}
	}

	pm.logger.Debug(fmt.Sprintf("discovered %d instances", len(topology.Nodes)))
}
