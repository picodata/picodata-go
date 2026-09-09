package picodata

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"

	tpm "git.picodata.io/core/drivers/picodata-go/topology"

	"github.com/jackc/pgx/v5/pgxpool"
)

// instance is a pool together with the identity of the node it serves. The
// uuid travels with the pool because the swap-remove in removePool has to know
// the key of the entry it moves, and unlike the address it used to key on,
// a uuid cannot be derived back from a *pgxpool.Pool.
type instance struct {
	uuid string
	pool *pgxpool.Pool
}

type poolManager struct {
	logger    *slog.Logger
	mu        sync.RWMutex
	config    *pgxpool.Config
	instances []instance
	// Key: instance uuid
	// Value: index of corresponding instance in [poolManager] instances slice
	poolsMap              map[string]int
	connectionPerInstance int32
	// closing counts the pools that removePool handed to a background
	// goroutine but that have not finished closing yet, so close() can honour
	// the "blocks until all connections are returned" contract of Client.Close.
	closing sync.WaitGroup
}

type poolManagerOption func(*poolManager)

// withPoolManagerLogger takes the root logger and bakes the package-specific tag inside.
func withPoolManagerLogger(l *slog.Logger) poolManagerOption {
	return func(pm *poolManager) {
		pm.logger = l.With(slog.String("component", "pool_manager"))
	}
}

// newPoolManager creates an empty manager: pools are opened later, when the
// topology manager reports the nodes to connect to. The bootstrap host from the
// DSN gets no special treatment, so it is subject to topology filters like any
// other node.
func newPoolManager(config *pgxpool.Config, connPerInstance int32, opts ...poolManagerOption) *poolManager {
	pm := &poolManager{
		logger:                slog.New(slog.DiscardHandler),
		config:                config.Copy(),
		instances:             make([]instance, 0, 1),
		poolsMap:              make(map[string]int, 1),
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

	size := len(pm.instances)
	if size == 0 {
		return nil
	}

	index := indexFn(size)
	index %= uint64(size)

	return pm.instances[index].pool
}

func (pm *poolManager) addPool(uuid string, address string) error {
	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid address format: %q", address)
	}

	// Convert port from string to integer
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("parse port error [port: %s]: %w", portStr, err)
	}

	if port < 1 || port > 65535 {
		return fmt.Errorf("port out of range [port: %d]", port)
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, ok := pm.poolsMap[uuid]; ok {
		pm.logger.Warn("pool already exists", slog.String("uuid", uuid))
		return nil
	}

	// Create a new config for connection
	connCfg := pm.config.Copy()
	connCfg.ConnConfig.Host = host
	// Picodata provides valid port, so casting is safe
	connCfg.ConnConfig.Port = uint16(port)

	if pm.connectionPerInstance != 0 {
		connCfg.MaxConns = int32(pm.connectionPerInstance)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), connCfg)
	if err != nil {
		return fmt.Errorf("failed to create pgxpool: %w", err)
	}

	// Add pool to the instances slice
	pm.instances = append(pm.instances, instance{uuid: uuid, pool: pool})
	pm.poolsMap[uuid] = len(pm.instances) - 1

	pm.logger.Debug("a pool for instance was appended",
		slog.String("uuid", uuid),
		slog.String("address", address),
	)

	return nil
}

func (pm *poolManager) removePool(uuid string) {
	pm.mu.Lock()

	// If pool for instance doesn't exist -> return
	index, ok := pm.poolsMap[uuid]
	if !ok {
		pm.mu.Unlock()
		pm.logger.Warn("a pool for instance not found", slog.String("uuid", uuid))
		return
	}

	// Keep reference to the pool that will be closed asynchronously
	poolToClose := pm.instances[index].pool

	// Remove entry about pool from poolsIndex
	delete(pm.poolsMap, uuid)

	lastPoolIdx := len(pm.instances) - 1
	// If deleted pool isn't last in the slice
	if index != lastPoolIdx {
		// Move last pool to the position of deleted one
		pm.instances[index] = pm.instances[lastPoolIdx]
		// Update index of moved pool in poolsIndex
		pm.poolsMap[pm.instances[index].uuid] = index
	}
	// Clear the tail before truncating: the slot stays alive beyond len until
	// the next append overwrites it, and with it the removed pool
	pm.instances[lastPoolIdx] = instance{}
	// Delete pool from instances slice by truncating it
	pm.instances = pm.instances[:lastPoolIdx]

	pm.logger.Debug("a pool for instance was removed", slog.String("uuid", uuid))

	// Register the pending close while still holding the lock: close() waits
	// with the lock held, so the counter cannot rise from zero behind its back.
	pm.closing.Add(1)

	pm.mu.Unlock()

	// Close the pool in background to avoid blocking the manager
	go func() {
		defer pm.closing.Done()

		poolToClose.Close()
		pm.logger.Debug("pool closed", slog.String("uuid", uuid))
	}()
}

// close closes every pool the manager owns and blocks until they are done,
// including the ones removePool is still closing in the background.
func (pm *poolManager) close() {
	pm.mu.Lock()

	// Take the pools away from the manager before closing them, so a pick that
	// races close finds nothing instead of a pool that is already going away.
	instances := pm.instances
	pm.instances = nil
	// Emptying the index is also what makes the wait below safe: removePool
	// registers a pending close only for an entry it found here, and it holds
	// pm.mu from that lookup until the counter is raised, so once this returns
	// no Add can still be on its way.
	pm.poolsMap = map[string]int{}

	pm.mu.Unlock()

	// Closing blocks until every checked-out connection is returned, and the
	// callers holding those connections have to go through pick to return
	// them, so the lock must be released before this loop.
	for _, in := range instances {
		in.pool.Close()
	}

	pm.closing.Wait()
}

func (pm *poolManager) ping(ctx context.Context) error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	for _, in := range pm.instances {
		if err := pingPool(ctx, in.pool); err != nil {
			return err
		}
	}

	return nil
}

func (pm *poolManager) reset() {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	for _, in := range pm.instances {
		in.pool.Reset()
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

func (pm *poolManager) OnTopologyChanged(topology *tpm.ClusterTopology) {
	for _, nodeInfo := range topology.Nodes {
		switch nodeInfo.State {
		case tpm.StateOnline:
			if err := pm.addPool(nodeInfo.ID, nodeInfo.Address); err != nil {
				pm.logger.Error("failed to add connection", slog.String("uuid", nodeInfo.ID), slog.Any("error", err))
				continue
			}
		case tpm.StateOffline:
			pm.removePool(nodeInfo.ID)
		default:
			pm.logger.Warn("node with unknown state is ignored",
				slog.String("uuid", nodeInfo.ID),
				slog.Any("state", nodeInfo.State),
			)
		}
	}

	pm.logger.Debug(fmt.Sprintf("discovered %d instances", len(topology.Nodes)))
}
