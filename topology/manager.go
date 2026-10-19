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

// ErrNoOnlineNodes means the topology contains no online nodes.
var ErrNoOnlineNodes = errors.New("no online nodes discovered")

// ErrNoAllowedOnlineNodes means all online nodes were excluded by filters.
var ErrNoAllowedOnlineNodes = errors.New("no online nodes available after applying topology filters")

// Manager coordinates topology updates and dispatches them to listeners.
type Manager struct {
	logger *slog.Logger

	// current holds the cluster as reported by the source, with no filters applied.
	current atomic.Pointer[clusterSnapshot]
	// filtered holds the view exposed to listeners: allowed nodes and their
	// replicaset and tier metadata. It shares the pointer with current when
	// no filter is configured.
	filtered atomic.Pointer[clusterSnapshot]

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

// WithForbiddenTiers excludes the given tiers, their replicasets and nodes
// from topology listeners.
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
	tpm.store(topology)

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
	if equalTopology(previous.topology, topology) {
		tpm.mu.Unlock()
		return
	}

	previousFiltered := tpm.filtered.Load()

	tpm.store(topology)

	snapshot := tpm.filtered.Load()

	// Only excluded topology data changed. Without filters these are the maps the check
	// above already compared, so skip the walk.
	if len(tpm.filters) != 0 && equalTopology(previousFiltered.topology, snapshot.topology) {
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

func equalTopology(a, b *ClusterTopology) bool {
	return maps.Equal(a.Nodes, b.Nodes) &&
		maps.Equal(a.Replicasets, b.Replicasets) &&
		maps.Equal(a.BucketCounts, b.BucketCounts)
}

// store publishes the given cluster state as both the raw and the filtered
// snapshot. The maps are copied: a Source may reuse the maps it hands back.
func (tpm *Manager) store(snapshot *ClusterTopology) {
	full := newClusterSnapshot(snapshot)

	tpm.current.Store(full)

	if len(tpm.filters) == 0 {
		tpm.filtered.Store(full)
		return
	}

	filtered := tpm.filterTopology(full)
	tpm.filtered.Store(filtered)

	tpm.logger.Debug("filters applied to topology",
		slog.Uint64("version", full.Version()),
		slog.Int("nodes", len(full.topology.Nodes)),
		slog.Int("visible", len(filtered.topology.Nodes)),
	)

	if len(filtered.topology.Nodes) == 0 && full.OnlineCount() != 0 {
		tpm.logger.Warn("every online node is hidden by filter",
			slog.Uint64("version", full.Version()),
			slog.Int("nodes", len(full.topology.Nodes)),
		)
	}
}

// filterTopology returns allowed nodes and their replicaset and tier metadata.
// Only called when at least one filter is configured.
func (tpm *Manager) filterTopology(full *clusterSnapshot) *clusterSnapshot {
	snapshot := full.topology
	filtered := &ClusterTopology{
		Version: snapshot.Version,
		Nodes:   make(map[string]NodeInfo, len(snapshot.Nodes)),
	}
	if snapshot.Replicasets != nil {
		filtered.Replicasets = make(map[string]ReplicasetInfo, len(snapshot.Replicasets))
	}
	if snapshot.BucketCounts != nil {
		filtered.BucketCounts = make(map[string]uint64, len(snapshot.BucketCounts))
	}

	for id, node := range snapshot.Nodes {
		if !tpm.allow(node) {
			continue
		}

		filtered.Nodes[id] = node
		if replicaset, ok := snapshot.Replicasets[node.ReplicasetID]; ok {
			filtered.Replicasets[node.ReplicasetID] = replicaset
		}
		if bucketCount, ok := snapshot.BucketCounts[node.Tier]; ok {
			filtered.BucketCounts[node.Tier] = bucketCount
		}
	}

	return &clusterSnapshot{topology: filtered}
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

// CheckOnlineNodes returns the number of online nodes allowed by the configured
// filters, or ErrNoOnlineNodes or ErrNoAllowedOnlineNodes if none are available.
// It does not attempt to open connections.
func (tpm *Manager) CheckOnlineNodes() (int, error) {
	tpm.mu.Lock()
	defer tpm.mu.Unlock()

	if online := tpm.filtered.Load().OnlineCount(); online != 0 {
		return online, nil
	}
	if tpm.current.Load().OnlineCount() == 0 {
		return 0, ErrNoOnlineNodes
	}

	return 0, ErrNoAllowedOnlineNodes
}

// Close stops the background update loop and releases allocated resources.
func (tpm *Manager) Close() {
	tpm.cancel()
	<-tpm.done
}
