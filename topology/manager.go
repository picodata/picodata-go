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
	current atomic.Pointer[clusterSnapshot]

	ctx    context.Context
	cancel context.CancelFunc
	source Source

	done chan struct{}

	mu        sync.Mutex // serializes topology updates and listener registration; guards the fields below
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

	tpm.store(topology)

	snapshot := tpm.current.Load()

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

// store publishes the given cluster state as an immutable
// snapshot. The maps are copied: a Source may reuse the maps it hands back.
func (tpm *Manager) store(snapshot *ClusterTopology) {
	tpm.current.Store(newClusterSnapshot(snapshot))
}

// Snapshot returns the latest full, immutable topology snapshot.
func (tpm *Manager) Snapshot() Snapshot {
	return tpm.current.Load()
}

// AddListener registers a new listener and immediately sends the full current topology to it.
//
// Dispatched under the lock, so applyAndNotify cannot reach the fresh listener
// first and leave this older snapshot as its last word.
func (tpm *Manager) AddListener(listener Listener) {
	tpm.mu.Lock()
	defer tpm.mu.Unlock()

	tpm.listeners = append(tpm.listeners, listener)

	listener.OnTopologyChanged(tpm.current.Load())
}

// Close stops the background update loop and waits for it to return, including
// any listener callbacks it is dispatching.
func (tpm *Manager) Close() {
	tpm.cancel()
	<-tpm.done
}
