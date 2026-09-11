package picodata

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	tpm "git.picodata.io/core/drivers/picodata-go/topology"

	"github.com/jackc/pgx/v5/pgxpool"
)

type instance struct {
	uuid string
	addr string
	pool *pgxpool.Pool
}

type poolManager struct {
	logger                *slog.Logger
	config                *pgxpool.Config
	connectionPerInstance int32

	// Rotation the balance strategies index into, replaced wholesale.
	instances atomic.Pointer[[]instance]

	// Departed pools still draining.
	closing sync.WaitGroup

	mu            sync.Mutex // serializes reconcile and close; guards the fields below
	latestVersion uint64
	closed        bool
}

type poolManagerOption func(*poolManager)

func withPoolManagerLogger(l *slog.Logger) poolManagerOption {
	return func(pm *poolManager) {
		pm.logger = l.With(slog.String("component", "pool_manager"))
	}
}

// newPoolManager opens no pools: the first reconcile does, the DSN's own host
// included and filtered like any other node.
func newPoolManager(config *pgxpool.Config, connPerInstance int32, opts ...poolManagerOption) *poolManager {
	pm := &poolManager{
		logger:                slog.New(slog.DiscardHandler),
		config:                config.Copy(),
		connectionPerInstance: connPerInstance,
	}

	pm.instances.Store(new([]instance))

	for _, opt := range opts {
		opt(pm)
	}

	return pm
}

func (pm *poolManager) PoolSize() int {
	return len(*pm.instances.Load())
}

func (pm *poolManager) getConfig() *pgxpool.Config {
	return pm.config.Copy()
}

// pick maps a strategy's selector onto one immutable rotation.
func (pm *poolManager) pick(selector uint64) *pgxpool.Pool {
	pools := *pm.instances.Load()

	size := len(pools)
	if size == 0 {
		return nil
	}

	index := selector
	index %= uint64(size)

	return pools[index].pool
}

// reconcile brings the open pools in line with the snapshot and returns how
// many it left open.
func (pm *poolManager) reconcile(snapshot *tpm.ClusterTopology) int {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// close() is final: a late reconcile would reopen pools nobody closes.
	if pm.closed {
		return 0
	}

	current := *pm.instances.Load()

	if snapshot.Version < pm.latestVersion {
		pm.logger.Warn("stale topology snapshot ignored",
			slog.Uint64("version", snapshot.Version),
			slog.Uint64("latest", pm.latestVersion),
		)

		return len(current)
	}

	pm.latestVersion = snapshot.Version

	next := make([]instance, 0, len(current))
	kept := make(map[string]struct{}, len(current))

	// A node that changed address is not a survivor: its pool points at the old host.
	for _, inst := range current {
		node, ok := snapshot.Nodes[inst.uuid]
		if !ok || node.State != tpm.StateOnline || node.Address != inst.addr {
			continue
		}

		next = append(next, inst)
		kept[inst.uuid] = struct{}{}
	}

	for uuid, node := range snapshot.Nodes {

		switch node.State {
		case tpm.StateOnline:
			// Opened below.
		case tpm.StateOffline:
			continue
		default:
			pm.logger.Warn("node with unknown state is ignored",
				slog.String("uuid", uuid),
				slog.String("state", node.State),
			)

			continue
		}

		if _, ok := kept[uuid]; ok {
			continue
		}

		inst, err := pm.createPool(uuid, node.Address)
		if err != nil {
			pm.logger.Error("failed to open a pool",
				slog.String("uuid", uuid),
				slog.String("address", node.Address),
				slog.Any("error", err),
			)

			continue
		}

		next = append(next, inst)

		pm.logger.Debug("a pool for instance was opened",
			slog.String("uuid", uuid),
			slog.String("address", node.Address),
		)
	}

	pm.instances.Store(&next)

	// Close blocks on checked-out connections; the topology listener cannot wait.
	for _, inst := range current {
		if _, ok := kept[inst.uuid]; ok {
			continue
		}

		pm.closing.Add(1) // under pm.mu, so close() cannot miss it

		go func() {
			defer pm.closing.Done()

			inst.pool.Close()
			pm.logger.Debug("pool closed",
				slog.String("uuid", inst.uuid),
				slog.String("address", inst.addr),
			)
		}()
	}

	pm.logger.Debug("topology reconciled",
		slog.Uint64("version", snapshot.Version),
		slog.Int("pools", len(next)),
	)

	return len(next)
}

// createPool does not publish the pool: only reconcile decides what the
// manager serves.
func (pm *poolManager) createPool(uuid string, address string) (instance, error) {
	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return instance{}, fmt.Errorf("invalid address format: %q", address)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return instance{}, fmt.Errorf("parse port error [port: %s]: %w", portStr, err)
	}

	if port < 1 || port > 65535 {
		return instance{}, fmt.Errorf("port out of range [port: %d]", port)
	}

	connCfg := pm.config.Copy()
	connCfg.ConnConfig.Host = host
	connCfg.ConnConfig.Port = uint16(port)

	if pm.connectionPerInstance != 0 {
		connCfg.MaxConns = int32(pm.connectionPerInstance)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), connCfg)
	if err != nil {
		return instance{}, fmt.Errorf("failed to create pgxpool: %w", err)
	}

	return instance{uuid: uuid, addr: address, pool: pool}, nil
}

// close closes every pool the manager owns and blocks until they are done,
// including the ones reconcile is still closing in the background.
func (pm *poolManager) close() {
	pm.mu.Lock()

	pm.closed = true

	// Empty the rotation first: a racing pick then finds nothing rather than a
	// pool already going away, and no further Add can reach pm.closing.
	current := *pm.instances.Load()
	pm.instances.Store(new([]instance))

	pm.mu.Unlock()

	// Unlocked: the callers returning those connections go through pick. Closed
	// together: each Close waits for its own pool, so serial would cost the sum.
	for _, inst := range current {
		pm.closing.Add(1)

		go func() {
			defer pm.closing.Done()

			inst.pool.Close()
		}()
	}

	pm.closing.Wait()
}

// ping fails on an empty manager: a client with no pools is not healthy.
func (pm *poolManager) ping(ctx context.Context) error {
	instances := *pm.instances.Load()
	if len(instances) == 0 {
		return ErrNoConnections
	}

	for _, inst := range instances {
		if err := pingPool(ctx, inst.pool); err != nil {
			return err
		}
	}

	return nil
}

func (pm *poolManager) reset() {
	for _, inst := range *pm.instances.Load() {
		inst.pool.Reset()
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
