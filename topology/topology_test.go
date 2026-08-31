package topology_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"git.picodata.io/core/drivers/picodata-go/internal/testenv"
	"git.picodata.io/core/drivers/picodata-go/topology"
	"git.picodata.io/core/drivers/picodata-go/topology/source/dynamic"
	"git.picodata.io/core/drivers/picodata-go/topology/source/static"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockListener capturing the last received topology state for verification.
type mockListener struct {
	mu       sync.Mutex
	topology *topology.ClusterTopology
	called   chan struct{}
}

func newMockListener() *mockListener {
	return &mockListener{
		called: make(chan struct{}, 1),
	}
}

func (l *mockListener) OnTopologyChanged(topology *topology.ClusterTopology) {
	l.mu.Lock()
	l.topology = topology
	l.mu.Unlock()

	// Non-blocking write to signal that notification was received.
	select {
	case l.called <- struct{}{}:
	default:
	}
}

func (l *mockListener) getTopology() *topology.ClusterTopology {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.topology
}

func TestTopologyManager_StaticSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	expectedAddrs := []string{"picodata-1:3301", "picodata-2:3301"}
	source := static.New(expectedAddrs)
	listener := newMockListener()

	manager, err := topology.NewTopologyManager(ctx, source)
	assert.NoError(t, err)
	defer manager.Close()

	manager.AddListener(listener)

	select {
	case <-listener.called:
	case <-ctx.Done():
		t.Fatal("timeout waiting for initial topology notification")
	}

	topology := listener.getTopology()
	assert.Equal(t, uint64(1), topology.Version)
	assert.Len(t, topology.Nodes, 2)

	assert.Contains(t, topology.Nodes, "static-0")
	assert.Contains(t, topology.Nodes, "static-1")
}

func TestTopologyManager_DynamicSource(t *testing.T) {
	testenv.RequireCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Instances that have joined but not caught up are reported Offline, so the
	// assertions below only hold once the cluster has converged.
	testenv.WaitReady(t, ctx, 2, 1)

	baseCfg, err := pgx.ParseConfig(testenv.URI())
	require.NoError(t, err)

	var bootstrapAddrs []string
	if baseCfg.Host != "" {
		primaryAddr := net.JoinHostPort(baseCfg.Host, fmt.Sprintf("%d", baseCfg.Port))
		bootstrapAddrs = append(bootstrapAddrs, primaryAddr)
	}

	source := dynamic.New(baseCfg, bootstrapAddrs)
	listener := newMockListener()

	manager, err := topology.NewTopologyManager(ctx, source)
	require.NoError(t, err)
	defer manager.Close()

	manager.AddListener(listener)

	select {
	case <-listener.called:
	case <-ctx.Done():
		t.Fatal("timeout waiting for dynamic topology notification")
	}

	topology := listener.getTopology()
	require.NotNil(t, topology)

	// Instances that died for good stay in the cluster as Offline records, so
	// the driver is judged by what it reports as online.
	assert.GreaterOrEqual(t, topology.OnlineCount(), 2)

	for _, node := range topology.Nodes {
		assert.NotEmpty(t, node.ID)
		assert.NotEmpty(t, node.Address)
	}
}

type mockSource struct {
	topology *topology.ClusterTopology
}

func newMockSource(addrs []string) *mockSource {
	nodes := make(map[string]topology.NodeInfo, len(addrs))

	for i, addr := range addrs {
		id := fmt.Sprintf("node-%d", i)
		nodes[id] = topology.NodeInfo{
			ID:      id,
			Address: addr,
		}
	}

	return &mockSource{
		topology: &topology.ClusterTopology{
			Version: 1,
			Nodes:   nodes,
		},
	}
}

func (p *mockSource) Fetch(_ context.Context) (*topology.ClusterTopology, error) {
	return &topology.ClusterTopology{Version: uint64(0), Nodes: make(map[string]topology.NodeInfo)}, nil
}

func (p *mockSource) Subscribe(_ context.Context) (<-chan *topology.ClusterTopology, error) {
	ch := make(chan *topology.ClusterTopology, 10)

	ch <- p.topology

	return ch, nil
}

func TestTopologyManager_StreamingUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	expectedAddrs := []string{"picodata-1:3301", "picodata-2:3301"}
	source := newMockSource(expectedAddrs)
	listener := newMockListener()

	manager, err := topology.NewTopologyManager(ctx, source)
	require.NoError(t, err)
	defer manager.Close()

	manager.AddListener(listener)

	select {
	case <-listener.called:
	case <-ctx.Done():
		t.Fatal("timeout waiting for dynamic topology notification")
	}

	initialTopology := listener.getTopology()
	assert.Equal(t, uint64(0), initialTopology.Version)
	assert.Empty(t, initialTopology.Nodes)

	select {
	case <-listener.called:
	case <-ctx.Done():
		t.Fatal("timeout waiting for dynamic topology notification")
	}

	streamingTopology := listener.getTopology()
	assert.Equal(t, uint64(1), streamingTopology.Version)
	assert.Len(t, streamingTopology.Nodes, 2)

	assert.Contains(t, streamingTopology.Nodes, "node-0")
	assert.Contains(t, streamingTopology.Nodes, "node-1")
}

// controlledSource serves a fixed initial snapshot and lets the test push
// updates on demand.
type controlledSource struct {
	initial *topology.ClusterTopology
	updates chan *topology.ClusterTopology
}

func newControlledSource(initial *topology.ClusterTopology) *controlledSource {
	return &controlledSource{
		initial: initial,
		updates: make(chan *topology.ClusterTopology, 10),
	}
}

func (s *controlledSource) Fetch(_ context.Context) (*topology.ClusterTopology, error) {
	return s.initial, nil
}

func (s *controlledSource) Subscribe(_ context.Context) (<-chan *topology.ClusterTopology, error) {
	return s.updates, nil
}

func testNode(id, tier, state string) topology.NodeInfo {
	return topology.NodeInfo{
		ID:      id,
		Name:    id,
		Tier:    tier,
		Address: id + ":5432",
		State:   state,
	}
}

func testCluster(version uint64, nodes ...topology.NodeInfo) *topology.ClusterTopology {
	nodesMap := make(map[string]topology.NodeInfo, len(nodes))
	for _, node := range nodes {
		nodesMap[node.ID] = node
	}

	return &topology.ClusterTopology{Version: version, Nodes: nodesMap}
}

func awaitTopology(t *testing.T, ctx context.Context, listener *mockListener) *topology.ClusterTopology {
	t.Helper()

	select {
	case <-listener.called:
	case <-ctx.Done():
		t.Fatal("timeout waiting for topology notification")
	}

	return listener.getTopology()
}

func TestTopologyManager_Filters(t *testing.T) {
	t.Run("HidesForbiddenTiersFromListeners", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		source := newControlledSource(testCluster(1,
			testNode("node-1", "default", topology.StateOnline),
			testNode("node-2", "default", topology.StateOnline),
			testNode("node-3", "arbiter", topology.StateOnline),
		))

		manager, err := topology.NewTopologyManager(ctx, source, topology.WithForbiddenTiers("arbiter"))
		require.NoError(t, err)
		defer manager.Close()

		listener := newMockListener()
		manager.AddListener(listener)

		received := awaitTopology(t, ctx, listener)
		require.Len(t, received.Nodes, 2)
		assert.NotContains(t, received.Nodes, "node-3")

		// The manager stays an honest mirror of the cluster: only the view
		// handed to listeners is filtered.
		assert.Len(t, manager.GetTopology().Nodes, 3)
		assert.Len(t, manager.GetFilteredTopology().Nodes, 2)
	})

	t.Run("FiltersDeltaAndSkipsEmptyNotification", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		source := newControlledSource(testCluster(1,
			testNode("node-1", "default", topology.StateOnline),
			testNode("node-2", "arbiter", topology.StateOnline),
		))

		manager, err := topology.NewTopologyManager(ctx, source, topology.WithForbiddenTiers("arbiter"))
		require.NoError(t, err)
		defer manager.Close()

		listener := newMockListener()
		manager.AddListener(listener)

		initial := awaitTopology(t, ctx, listener)
		require.Len(t, initial.Nodes, 1)
		require.Contains(t, initial.Nodes, "node-1")

		// The whole delta is forbidden: the listener must not be woken up.
		source.updates <- testCluster(2,
			testNode("node-1", "default", topology.StateOnline),
			testNode("node-2", "arbiter", topology.StateOffline),
		)
		// A change the listener is allowed to see. Updates are processed in
		// order, so receiving version 3 first proves version 2 was swallowed.
		source.updates <- testCluster(3,
			testNode("node-1", "default", topology.StateOffline),
			testNode("node-2", "arbiter", topology.StateOffline),
		)

		received := awaitTopology(t, ctx, listener)
		assert.Equal(t, uint64(3), received.Version)
		require.Len(t, received.Nodes, 1)
		assert.Equal(t, topology.StateOffline, received.Nodes["node-1"].State)

		// The hidden node keeps being tracked, so the driver still notices when
		// an allowed node comes back on a node it does not route to.
		assert.Len(t, manager.GetTopology().Nodes, 2)
		assert.Equal(t, topology.StateOffline, manager.GetTopology().Nodes["node-2"].State)
	})

	t.Run("CombinesFiltersWithAnd", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		source := newControlledSource(testCluster(1,
			testNode("node-1", "default", topology.StateOnline),
			testNode("node-2", "readonly", topology.StateOnline),
			testNode("node-3", "arbiter", topology.StateOnline),
		))

		// Repeated options accumulate: a node is exposed only if every filter
		// allows it.
		manager, err := topology.NewTopologyManager(ctx, source,
			topology.WithForbiddenTiers("arbiter"),
			topology.WithForbiddenTiers("readonly"),
		)
		require.NoError(t, err)
		defer manager.Close()

		listener := newMockListener()
		manager.AddListener(listener)

		received := awaitTopology(t, ctx, listener)
		require.Len(t, received.Nodes, 1)
		assert.Contains(t, received.Nodes, "node-1")
	})

	t.Run("WithoutFiltersSnapshotsAreShared", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		source := newControlledSource(testCluster(1,
			testNode("node-1", "default", topology.StateOnline),
		))

		manager, err := topology.NewTopologyManager(ctx, source)
		require.NoError(t, err)
		defer manager.Close()

		// No filter configured: nothing is copied, both views are the same snapshot.
		assert.Same(t, manager.GetTopology(), manager.GetFilteredTopology())
	})
}
