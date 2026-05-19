package topology_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"git.picodata.io/core/drivers/picodata-go/topology"
	"git.picodata.io/core/drivers/picodata-go/topology/source/dynamic"
	"git.picodata.io/core/drivers/picodata-go/topology/source/static"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	defaultURI = "postgresql://admin:T0psecret@localhost:55432?sslmode=disable"
)

func picodataURI() string {
	val, ok := os.LookupEnv("PICODATA_TEST_CONN")

	if !ok {
		return defaultURI
	}

	return val
}

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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	baseCfg, err := pgx.ParseConfig(picodataURI())
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

	assert.GreaterOrEqual(t, len(topology.Nodes), 2)

	for _, node := range topology.Nodes {
		assert.NotEmpty(t, node.ID)
		assert.NotEmpty(t, node.Address)
		assert.Equal(t, "Online", node.State)
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
