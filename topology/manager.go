package topology

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
)

// Manager coordinates topology updates and dispatches them to listeners.
type Manager struct {
	logger *slog.Logger

	// current holds the cluster as reported by the source, with no filters applied.
	current atomic.Pointer[ClusterTopology]
	// filtered holds the view exposed to listeners: current minus the nodes
	// rejected by filters. It shares the pointer with current when no filter
	// is configured.
	filtered atomic.Pointer[ClusterTopology]

	// filters are combined with a logical AND and never change after construction.
	filters []Filter

	ctx    context.Context
	cancel context.CancelFunc
	source Source

	done chan struct{}

	mu        sync.Mutex // serializes applyAndNotify; guards the fields below
	listeners []Listener
	// Newest version seen, dropped and deduplicated reports included, so it can
	// run ahead of the stored snapshot.
	latestVersion uint64
}

// Option applies a configuration option to the Manager.
type Option func(*Manager)

// WithLogger takes the root logger and wraps it with the topology manager package tag.
func WithLogger(l *slog.Logger) Option {
	return func(m *Manager) {
		m.logger = l.With(slog.String("component", "topology.manager"))
	}
}

// WithForbiddenTiers hides nodes belonging to the given tiers from topology listeners.
//
// Filters are combined with a logical AND: a node reaches the listeners only if
// every filter allows it. Filtering never affects the [Source], so the driver
// keeps polling hidden nodes and notices when they come back.
//
// Empty and whitespace-only entries are ignored, for example "arbiter", " ", "readonly".
func WithForbiddenTiers(tiers ...string) Option {
	return func(m *Manager) {
		if set := newForbiddenTiers(tiers); set != nil {
			m.filters = append(m.filters, set)
		}
	}
}

// NewTopologyManager creates and initializes a TopologyManager instance.
func NewTopologyManager(parent context.Context, source Source, opts ...Option) (*Manager, error) {
	// Fallback to background context if parent is nil to prevent initialization panic.
	if parent == nil {
		parent = context.Background()
	}

	ctx, cancel := context.WithCancel(parent)

	tpm := &Manager{
		logger: slog.New(slog.DiscardHandler),
		ctx:    ctx,
		cancel: cancel,
		source: source,
		done:   make(chan struct{}),
	}

	for _, opt := range opts {
		opt(tpm)
	}

	topology, err := source.Fetch(ctx)
	if err != nil {
		cancel()

		return nil, fmt.Errorf("fetch: %w", err)
	}

	if topology == nil {
		cancel()

		return nil, errors.New("fetch: source returned no topology")
	}

	tpm.latestVersion = topology.Version
	tpm.store(topology.Version, topology.Nodes)

	if sp, ok := source.(StreamingSource); ok {
		go tpm.updateLoop(sp)
	} else {
		close(tpm.done)
	}

	return tpm, nil
}

func (tpm *Manager) updateLoop(sp StreamingSource) {
	defer close(tpm.done)

	ch, err := sp.Subscribe(tpm.ctx)
	if err != nil {
		tpm.logger.Warn("failed to subscribe", slog.Any("error", err))
		return
	}

	for {
		select {
		case <-tpm.ctx.Done():
			return
		case topology, ok := <-ch:
			if !ok {
				tpm.logger.Warn("update channel closed unexpectedly")
				return
			}

			tpm.applyAndNotify(topology)
		}
	}
}

func (tpm *Manager) applyAndNotify(topology *ClusterTopology) {
	// A Source is user-supplied, so a broken one must not take the process down.
	if topology == nil {
		tpm.logger.Warn("nil topology report ignored")
		return
	}

	tpm.mu.Lock()

	if topology.Version < tpm.latestVersion {
		tpm.logger.Warn("stale topology report ignored",
			slog.Uint64("version", topology.Version),
			slog.Uint64("latest", tpm.latestVersion),
		)

		tpm.mu.Unlock()

		return
	}

	tpm.latestVersion = topology.Version

	// An unreadable cluster, not one that lost every node.
	if len(topology.Nodes) == 0 {
		tpm.logger.Warn("empty topology report ignored",
			slog.Uint64("version", topology.Version),
		)

		tpm.mu.Unlock()

		return
	}

	previous := tpm.current.Load()

	// The report replaces the stored view instead of merging into it, so a node
	// it stops mentioning loses its pool rather than staying Online forever.
	if maps.Equal(previous.Nodes, topology.Nodes) {
		tpm.mu.Unlock()
		return
	}

	previousFiltered := tpm.filtered.Load()

	tpm.store(topology.Version, topology.Nodes)

	snapshot := tpm.filtered.Load()

	// Only hidden nodes changed. Without filters these are the maps the check
	// above already compared, so skip the walk.
	if len(tpm.filters) != 0 && maps.Equal(previousFiltered.Nodes, snapshot.Nodes) {
		tpm.mu.Unlock()
		return
	}

	listeners := make([]Listener, len(tpm.listeners))
	copy(listeners, tpm.listeners)
	tpm.mu.Unlock()

	for _, l := range listeners {
		l.OnTopologyChanged(snapshot)
	}
}

// store publishes the given cluster state as both the raw and the filtered
// snapshot. The nodes are copied: a Source may reuse the map it hands back.
func (tpm *Manager) store(version uint64, nodes map[string]NodeInfo) {
	owned := make(map[string]NodeInfo, len(nodes))
	maps.Copy(owned, nodes)
	nodes = owned

	full := &ClusterTopology{
		Version: version,
		Nodes:   nodes,
	}

	tpm.current.Store(full)

	if len(tpm.filters) == 0 {
		tpm.filtered.Store(full)
		return
	}

	filteredNodes := tpm.filterNodes(nodes)
	tpm.filtered.Store(&ClusterTopology{
		Version: version,
		Nodes:   filteredNodes,
	})

	tpm.logger.Debug("filters applied to topology",
		slog.Uint64("version", version),
		slog.Int("nodes", len(nodes)),
		slog.Int("visible", len(filteredNodes)),
	)

	if len(filteredNodes) == 0 && full.OnlineCount() != 0 {
		tpm.logger.Warn("every online node is hidden by filter",
			slog.Uint64("version", version),
			slog.Int("nodes", len(nodes)),
		)
	}
}

// filterNodes returns the subset of nodes the filters allow. Only called when
// there is at least one filter: store publishes the unfiltered map directly.
func (tpm *Manager) filterNodes(nodes map[string]NodeInfo) map[string]NodeInfo {
	filtered := make(map[string]NodeInfo, len(nodes))
	for id, node := range nodes {
		if tpm.allow(node) {
			filtered[id] = node
		}
	}

	return filtered
}

// allow reports whether every configured filter accepts the node.
func (tpm *Manager) allow(node NodeInfo) bool {
	for _, filter := range tpm.filters {
		if !filter.Allow(node) {
			return false
		}
	}

	return true
}

// AddListener registers a new listener and immediately sends the current topology to it.
//
// Dispatched under the lock, so applyAndNotify cannot reach the fresh listener
// first and leave this older snapshot as its last word.
func (tpm *Manager) AddListener(listener Listener) {
	tpm.mu.Lock()
	defer tpm.mu.Unlock()

	tpm.listeners = append(tpm.listeners, listener)

	listener.OnTopologyChanged(tpm.filtered.Load())
}

// GetTopology returns the most recent cluster topology state, as reported by the
// source: filters are not applied. See [Manager.GetFilteredTopology] for the view
// the listeners receive.
// Note: Returns a shared pointer. Mutation of the underlying map fields by external consumers is forbidden.
func (tpm *Manager) GetTopology() *ClusterTopology {
	return tpm.current.Load()
}

// GetFilteredTopology returns the most recent cluster topology state with the
// configured filters applied, that is the same view the listeners receive.
// Note: Returns a shared pointer. Mutation of the underlying map fields by external consumers is forbidden.
func (tpm *Manager) GetFilteredTopology() *ClusterTopology {
	return tpm.filtered.Load()
}

// Close stops the background update loop and releases allocated resources.
func (tpm *Manager) Close() {
	tpm.cancel()
	<-tpm.done
}
