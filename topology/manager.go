package topology

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
)

// Manager coordinates topology updates and dispatches them to listeners.
type Manager struct {
	logger *slog.Logger
	// mu serializes state updates during applyAndNotify and protects the listeners slice.
	mu        sync.Mutex
	listeners []Listener
	current   atomic.Pointer[ClusterTopology]

	ctx    context.Context
	cancel context.CancelFunc
	source Source

	done chan struct{}
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

	tpm.current.Store(topology)

	// Start live updates if the source supports streaming.
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
	tpm.mu.Lock()

	previous := tpm.current.Load()
	current := make(map[string]NodeInfo, len(previous.Nodes))
	maps.Copy(current, previous.Nodes)

	delta := make(map[string]NodeInfo)
	for id, newNode := range topology.Nodes {
		node, ok := current[id]
		if !ok || node.State != newNode.State {
			delta[id] = newNode
			current[id] = newNode
		}
	}

	if len(delta) == 0 {
		tpm.mu.Unlock()
		return
	}

	tpm.current.Store(&ClusterTopology{
		Version: topology.Version,
		Nodes:   current,
	})

	listeners := make([]Listener, len(tpm.listeners))
	copy(listeners, tpm.listeners)
	tpm.mu.Unlock()

	deltaTopology := &ClusterTopology{
		Version: topology.Version,
		Nodes:   delta,
	}

	for _, l := range listeners {
		l.OnTopologyChanged(deltaTopology)
	}
}

// AddListener registers a new listener and immediately sends the current topology to it.
func (tpm *Manager) AddListener(listener Listener) {
	tpm.mu.Lock()
	tpm.listeners = append(tpm.listeners, listener)
	current := tpm.current.Load()
	tpm.mu.Unlock()

	// Invoke the callback outside the lock to prevent potential deadlocks
	// during active listener registration.
	listener.OnTopologyChanged(current)
}

// ForceRefresh manually pulls the latest topology from the source and triggers notification.
func (tpm *Manager) ForceRefresh(ctx context.Context) error {
	topology, err := tpm.source.Fetch(ctx)
	if err != nil {
		return err
	}

	tpm.applyAndNotify(topology)

	return nil
}

// GetTopology returns the most recent cluster topology state.
// Note: Returns a shared pointer. Mutation of the underlying map fields by external consumers is forbidden.
func (tpm *Manager) GetTopology() *ClusterTopology {
	return tpm.current.Load()
}

// Close stops the background update loop and releases allocated resources.
func (tpm *Manager) Close() {
	tpm.cancel()
	<-tpm.done
}
