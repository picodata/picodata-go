package topology_test

import (
	"context"
	"fmt"
	"log/slog"
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

type countingHandler struct {
	seen map[string]int
	mu   *sync.Mutex
}

func newCountingHandler() (*slog.Logger, func(msg string) int) {
	seen := map[string]int{}
	mu := &sync.Mutex{}

	count := func(msg string) int {
		mu.Lock()
		defer mu.Unlock()

		return seen[msg]
	}

	return slog.New(countingHandler{seen: seen, mu: mu}), count
}

func (h countingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h countingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.seen[r.Message]++

	return nil
}

func (h countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h countingHandler) WithGroup(string) slog.Handler { return h }

// mockListener records every topology it is handed. Keeping the whole sequence
// is what tells a swallowed notification from a superseded one.
type mockListener struct {
	mu       sync.Mutex
	received []*topology.ClusterTopology
	called   chan struct{}
}

func newMockListener() *mockListener {
	return &mockListener{
		called: make(chan struct{}, 16),
	}
}

func (l *mockListener) OnTopologyChanged(topology *topology.ClusterTopology) {
	l.mu.Lock()
	l.received = append(l.received, topology)
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

	if len(l.received) == 0 {
		return nil
	}

	return l.received[len(l.received)-1]
}

func (l *mockListener) versions() []uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	got := make([]uint64, 0, len(l.received))
	for _, t := range l.received {
		got = append(got, t.Version)
	}

	return got
}

func TestTopologyManager_StaticSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	expectedAddrs := []string{"picodata-1:3301", "picodata-2:3301"}
	source := static.New(expectedAddrs)
	listener := newMockListener()

	// require: a failed constructor returns nil and the deferred Close panics.
	manager, err := topology.NewTopologyManager(ctx, source)
	require.NoError(t, err)
	defer manager.Close()

	manager.AddListener(listener)

	topology := awaitTopology(t, ctx, listener)
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

	topology := awaitTopology(t, ctx, listener)
	require.NotNil(t, topology)

	// Instances that died for good stay in the cluster as Offline records, so
	// the driver is judged by what it reports as online.
	assert.GreaterOrEqual(t, topology.OnlineCount(), 2)

	for _, node := range topology.Nodes {
		assert.NotEmpty(t, node.ID)
		assert.NotEmpty(t, node.Address)
	}
}

func TestTopologyManager_StreamingUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Starts empty: the update below is what puts the nodes in.
	source := newControlledSource(testCluster(0))
	listener := newMockListener()

	manager, err := topology.NewTopologyManager(ctx, source)
	require.NoError(t, err)
	defer manager.Close()

	manager.AddListener(listener)

	initialTopology := awaitTopology(t, ctx, listener)
	assert.Equal(t, uint64(0), initialTopology.Version)
	assert.Empty(t, initialTopology.Nodes)

	source.updates <- testCluster(1,
		testNode("node-0", "default", topology.StateOnline),
		testNode("node-1", "default", topology.StateOnline),
	)

	streamingTopology := awaitTopology(t, ctx, listener)
	assert.Equal(t, uint64(1), streamingTopology.Version)
	assert.Len(t, streamingTopology.Nodes, 2)

	assert.Contains(t, streamingTopology.Nodes, "node-0")
	assert.Contains(t, streamingTopology.Nodes, "node-1")
}

// controlledSource serves a fixed initial snapshot and lets the test push
// updates on demand.
type controlledSource struct {
	mu      sync.Mutex
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
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.initial, nil
}

func (s *controlledSource) Subscribe(_ context.Context) (<-chan *topology.ClusterTopology, error) {
	return s.updates, nil
}

// nilSource reports success while handing back nothing, the way a buggy
// third-party Source can.
type nilSource struct{}

func (nilSource) Fetch(context.Context) (*topology.ClusterTopology, error) { return nil, nil }

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

// awaitVersion waits until the listener has been handed the given version, so a
// test can then assert on the whole delivered sequence.
func awaitVersion(t *testing.T, ctx context.Context, listener *mockListener, version uint64) *topology.ClusterTopology {
	t.Helper()

	for {
		if got := listener.getTopology(); got != nil && got.Version == version {
			return got
		}

		select {
		case <-listener.called:
		case <-ctx.Done():
			t.Fatalf("timeout waiting for topology version %d, got %v", version, listener.versions())
		}
	}
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

func TestTopologyManager_Notifications(t *testing.T) {
	t.Run("SendsTheWholeVisibleClusterNotADelta", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		source := newControlledSource(testCluster(1,
			testNode("node-1", "default", topology.StateOnline),
			testNode("node-2", "default", topology.StateOnline),
		))

		manager, err := topology.NewTopologyManager(ctx, source)
		require.NoError(t, err)
		defer manager.Close()

		listener := newMockListener()
		manager.AddListener(listener)
		require.Len(t, awaitTopology(t, ctx, listener).Nodes, 2)

		// Only node-2 changed, but the listener reconciles against the full
		// membership, so it must receive every visible node.
		source.updates <- testCluster(2,
			testNode("node-1", "default", topology.StateOnline),
			testNode("node-2", "default", topology.StateOffline),
		)

		received := awaitTopology(t, ctx, listener)
		assert.Equal(t, uint64(2), received.Version)
		require.Len(t, received.Nodes, 2)
		assert.Equal(t, topology.StateOnline, received.Nodes["node-1"].State)
		assert.Equal(t, topology.StateOffline, received.Nodes["node-2"].State)
	})

	t.Run("NotifiesWhenOnlyTheAddressChanges", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		source := newControlledSource(testCluster(1,
			testNode("node-1", "default", topology.StateOnline),
		))

		manager, err := topology.NewTopologyManager(ctx, source)
		require.NoError(t, err)
		defer manager.Close()

		listener := newMockListener()
		manager.AddListener(listener)
		require.Equal(t, "node-1:5432", awaitTopology(t, ctx, listener).Nodes["node-1"].Address)

		// State is unchanged, so a state-only comparison would swallow this and
		// leave every consumer talking to the old host.
		moved := testNode("node-1", "default", topology.StateOnline)
		moved.Address = "node-1-new:5432"
		source.updates <- testCluster(2, moved)

		received := awaitTopology(t, ctx, listener)
		assert.Equal(t, uint64(2), received.Version)
		assert.Equal(t, "node-1-new:5432", received.Nodes["node-1"].Address)
	})

	t.Run("DropsNodesMissingFromTheReport", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		source := newControlledSource(testCluster(1,
			testNode("node-1", "default", topology.StateOnline),
			testNode("node-2", "default", topology.StateOnline),
		))

		manager, err := topology.NewTopologyManager(ctx, source)
		require.NoError(t, err)
		defer manager.Close()

		listener := newMockListener()
		manager.AddListener(listener)
		require.Len(t, awaitTopology(t, ctx, listener).Nodes, 2)

		// node-2 is gone, so the report stops mentioning it. Merging would leave
		// it Online forever, with a pool pointed at a host that no longer exists.
		moved := testNode("node-1", "default", topology.StateOnline)
		moved.Address = "node-1-new:5432"
		source.updates <- testCluster(2, moved)

		received := awaitTopology(t, ctx, listener)
		assert.NotContains(t, received.Nodes, "node-2")
		assert.NotContains(t, manager.GetTopology().Nodes, "node-2")
	})

	t.Run("IgnoresAnEmptyReport", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		source := newControlledSource(testCluster(1,
			testNode("node-1", "default", topology.StateOnline),
		))

		manager, err := topology.NewTopologyManager(ctx, source)
		require.NoError(t, err)
		defer manager.Close()

		listener := newMockListener()
		manager.AddListener(listener)
		require.NotNil(t, awaitTopology(t, ctx, listener))

		// A source that could not read the cluster reports nothing. Taking that
		// at face value would close every pool at once.
		source.updates <- testCluster(2)
		source.updates <- testCluster(3,
			testNode("node-1", "default", topology.StateOffline),
		)

		awaitVersion(t, ctx, listener, 3)
		assert.Equal(t, []uint64{1, 3}, listener.versions(),
			"the empty report must not reach the listeners")
		assert.Len(t, manager.GetTopology().Nodes, 1)
	})

	t.Run("IgnoresAStaleReport", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		source := newControlledSource(testCluster(11,
			testNode("node-1", "default", topology.StateOnline),
			testNode("node-2", "default", topology.StateOnline),
		))

		manager, err := topology.NewTopologyManager(ctx, source)
		require.NoError(t, err)
		defer manager.Close()

		listener := newMockListener()
		manager.AddListener(listener)
		require.Len(t, awaitTopology(t, ctx, listener).Nodes, 2)

		// Storing an older report would leave the manager behind the listeners,
		// and the next honest report of the newer view would look like no change.
		source.updates <- testCluster(12, testNode("node-1", "default", topology.StateOnline))
		awaitVersion(t, ctx, listener, 12)

		source.updates <- testCluster(11,
			testNode("node-1", "default", topology.StateOnline),
			testNode("node-2", "default", topology.StateOnline),
		)
		// A later report the manager must still accept, proving the stale one
		// was skipped rather than merely delayed.
		source.updates <- testCluster(13,
			testNode("node-1", "default", topology.StateOffline),
		)

		awaitVersion(t, ctx, listener, 13)
		assert.Equal(t, []uint64{11, 12, 13}, listener.versions())
		assert.Equal(t, uint64(13), manager.GetTopology().Version)
	})

	t.Run("RemembersVersionsItDidNotStore", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		source := newControlledSource(testCluster(100,
			testNode("node-1", "default", topology.StateOnline),
			testNode("node-2", "default", topology.StateOnline),
		))

		manager, err := topology.NewTopologyManager(ctx, source)
		require.NoError(t, err)
		defer manager.Close()

		listener := newMockListener()
		manager.AddListener(listener)
		require.NotNil(t, awaitTopology(t, ctx, listener))

		// v200 changes nothing and is deduplicated, so it never reaches the
		// stored snapshot - but it was still seen, and v150 is older than it.
		source.updates <- testCluster(200,
			testNode("node-1", "default", topology.StateOnline),
			testNode("node-2", "default", topology.StateOnline),
		)
		source.updates <- testCluster(150, testNode("node-1", "default", topology.StateOnline))
		// Something newer the manager must still accept, which also proves the
		// two above were processed.
		source.updates <- testCluster(300, testNode("node-1", "default", topology.StateOnline))

		awaitVersion(t, ctx, listener, 300)
		assert.Equal(t, []uint64{100, 300}, listener.versions(),
			"v200 changed nothing and v150 was stale")
	})

	t.Run("RemembersTheVersionOfAnEmptyReport", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		source := newControlledSource(testCluster(100,
			testNode("node-1", "default", topology.StateOnline),
			testNode("node-2", "default", topology.StateOnline),
		))

		manager, err := topology.NewTopologyManager(ctx, source)
		require.NoError(t, err)
		defer manager.Close()

		listener := newMockListener()
		manager.AddListener(listener)
		require.NotNil(t, awaitTopology(t, ctx, listener))

		// Dropped for being empty, but v200 still happened.
		source.updates <- testCluster(200)
		source.updates <- testCluster(150, testNode("node-1", "default", topology.StateOnline))
		source.updates <- testCluster(300, testNode("node-1", "default", topology.StateOnline))

		awaitVersion(t, ctx, listener, 300)
		assert.Equal(t, []uint64{100, 300}, listener.versions())
	})

	t.Run("AcceptsAReportWithTheSameVersion", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		source := newControlledSource(testCluster(7,
			testNode("node-1", "default", topology.StateOnline),
		))

		manager, err := topology.NewTopologyManager(ctx, source)
		require.NoError(t, err)
		defer manager.Close()

		listener := newMockListener()
		manager.AddListener(listener)
		require.Len(t, awaitTopology(t, ctx, listener).Nodes, 1)

		// Only an older report is stale. Sources may reuse a version - the
		// static one always reports 1 - so an equal one still has to apply.
		source.updates <- testCluster(7,
			testNode("node-1", "default", topology.StateOnline),
			testNode("node-2", "default", topology.StateOnline),
		)

		received := awaitTopology(t, ctx, listener)
		assert.Len(t, received.Nodes, 2)
	})

	t.Run("StaysSilentWhenNothingChanged", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		source := newControlledSource(testCluster(1,
			testNode("node-1", "default", topology.StateOnline),
		))

		manager, err := topology.NewTopologyManager(ctx, source)
		require.NoError(t, err)
		defer manager.Close()

		listener := newMockListener()
		manager.AddListener(listener)
		require.NotNil(t, awaitTopology(t, ctx, listener))

		// An unchanged poll must not wake the listeners: reconcile is
		// idempotent, but not free.
		source.updates <- testCluster(2,
			testNode("node-1", "default", topology.StateOnline),
		)
		// Updates are processed in order, so seeing version 3 proves 2 was
		// swallowed.
		source.updates <- testCluster(3,
			testNode("node-1", "default", topology.StateOffline),
		)

		awaitVersion(t, ctx, listener, 3)
		assert.Equal(t, []uint64{1, 3}, listener.versions(),
			"the unchanged report must not reach the listeners")
	})
}

func testMetadataCluster() *topology.ClusterTopology {
	nodes := []topology.NodeInfo{
		testNode("node-1", "default", topology.StateOnline),
		testNode("node-2", "default", topology.StateOnline),
	}
	for i := range nodes {
		nodes[i].ReplicasetID = "rs-1"
	}
	report := testCluster(1, nodes...)
	report.Replicasets = map[string]topology.ReplicasetInfo{
		"rs-1": {ID: "rs-1", Tier: "default", CurrentMasterID: "node-1"},
		"rs-2": {ID: "rs-2", Tier: "other"},
	}
	report.Tiers = map[string]topology.TierInfo{
		"default": {Name: "default", BucketCount: 3000},
		"other":   {Name: "other", BucketCount: 3000},
	}
	return report
}

func TestTopologyManager_MetadataUpdates(t *testing.T) {
	tests := []struct {
		name   string
		change func(*topology.ClusterTopology)
	}{
		{"MasterChanges", func(report *topology.ClusterTopology) {
			rs := report.Replicasets["rs-1"]
			rs.CurrentMasterID = "node-2"
			report.Replicasets["rs-1"] = rs
		}},
		{"MasterBecomesUnknown", func(report *topology.ClusterTopology) {
			rs := report.Replicasets["rs-1"]
			rs.CurrentMasterID = ""
			report.Replicasets["rs-1"] = rs
		}},
		{"ReplicasetTierChanges", func(report *topology.ClusterTopology) {
			rs := report.Replicasets["rs-2"]
			rs.Tier = "default"
			report.Replicasets["rs-2"] = rs
		}},
		{"ReplicasetDisappears", func(report *topology.ClusterTopology) {
			delete(report.Replicasets, "rs-2")
		}},
		{"BucketCountChanges", func(report *topology.ClusterTopology) {
			tier := report.Tiers["default"]
			tier.BucketCount = 4000
			report.Tiers["default"] = tier
		}},
		{"TierAppears", func(report *topology.ClusterTopology) {
			report.Tiers["new"] = topology.TierInfo{Name: "new", BucketCount: 4000}
		}},
		{"TierDisappears", func(report *topology.ClusterTopology) {
			delete(report.Tiers, "other")
		}},
		{"MetadataBecomesUnavailable", func(report *topology.ClusterTopology) {
			report.Replicasets = nil
			report.Tiers = nil
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			initial := testMetadataCluster()
			source := newControlledSource(initial)
			manager, err := topology.NewTopologyManager(ctx, source)
			require.NoError(t, err)
			defer manager.Close()

			listener := newMockListener()
			manager.AddListener(listener)
			awaitTopology(t, ctx, listener)

			next := initial.Clone()
			next.Version = 2
			tt.change(next)
			source.updates <- next

			received := awaitVersion(t, ctx, listener, 2)
			assert.Equal(t, next, received)
			assert.Equal(t, initial.Nodes, received.Nodes)
			assert.Equal(t, []uint64{1, 2}, listener.versions())
		})
	}
}

func TestTopologyManager_UnchangedMetadata(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	initial := testMetadataCluster()
	source := newControlledSource(initial)
	manager, err := topology.NewTopologyManager(ctx, source)
	require.NoError(t, err)
	defer manager.Close()
	listener := newMockListener()
	manager.AddListener(listener)
	first := awaitTopology(t, ctx, listener)

	unchanged := initial.Clone()
	unchanged.Version = 2
	source.updates <- unchanged
	stale := initial.Clone()
	stale.Version = 1
	stale.Replicasets["rs-1"] = topology.ReplicasetInfo{ID: "rs-1", Tier: "default"}
	source.updates <- stale
	next := initial.Clone()
	next.Version = 3
	tier := next.Tiers["default"]
	tier.BucketCount++
	next.Tiers["default"] = tier
	source.updates <- next

	assert.Equal(t, next, awaitVersion(t, ctx, listener, 3))
	assert.Equal(t, []uint64{1, 3}, listener.versions())
	assert.Equal(t, initial, first, "publishing an update must not change an older snapshot")
}

func TestTopologyManager_SnapshotIsDetachedFromTheSource(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		for _, filtered := range []bool{false, true} {
			t.Run(fmt.Sprintf("Streaming=%t/Filtered=%t", streaming, filtered), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				report := testMetadataCluster()
				var opts []topology.Option
				if filtered {
					node := report.Nodes["node-2"]
					node.Tier = "other"
					node.ReplicasetID = "rs-2"
					report.Nodes["node-2"] = node
					opts = append(opts, topology.WithForbiddenTiers("other"))
				}
				source := newControlledSource(report)
				manager, err := topology.NewTopologyManager(ctx, source, opts...)
				require.NoError(t, err)
				defer manager.Close()
				listener := newMockListener()
				manager.AddListener(listener)
				received := awaitTopology(t, ctx, listener)
				if streaming {
					report = report.Clone()
					report.Version = 2
					tier := report.Tiers["default"]
					tier.BucketCount++
					report.Tiers["default"] = tier
					source.updates <- report
					received = awaitVersion(t, ctx, listener, 2)
				}
				expected := report.Clone()
				expectedVisible := received.Clone()

				// Delivery has completed, so the source mutation cannot race with copying.
				report.Version = 99
				delete(report.Nodes, "node-1")
				report.Nodes["extra"] = testNode("extra", "default", topology.StateOnline)
				report.Replicasets["rs-1"] = topology.ReplicasetInfo{}
				delete(report.Replicasets, "rs-2")
				report.Tiers["default"] = topology.TierInfo{}
				delete(report.Tiers, "other")

				assert.Equal(t, expected, manager.GetTopology())
				assert.Equal(t, expectedVisible, manager.GetFilteredTopology())
				assert.Equal(t, expectedVisible, received)
			})
		}
	}
}

func TestClusterTopologyOnlineCount(t *testing.T) {
	cluster := testCluster(1,
		testNode("node-1", "default", topology.StateOnline),
		testNode("node-2", "default", topology.StateOffline),
		testNode("node-3", "default", topology.StateOnline),
	)

	assert.Equal(t, 2, cluster.OnlineCount())
	assert.Equal(t, 0, testCluster(1).OnlineCount())
}

func TestTopologyManager_Lifecycle(t *testing.T) {
	t.Run("NilParentContextIsAccepted", func(t *testing.T) {
		source := newControlledSource(testCluster(1,
			testNode("node-1", "default", topology.StateOnline),
		))

		// A nil parent must fall back to Background rather than panic on
		// context.WithCancel.
		manager, err := topology.NewTopologyManager(nil, source) //nolint:staticcheck // nil is the case under test
		require.NoError(t, err)
		defer manager.Close()

		assert.Len(t, manager.GetTopology().Nodes, 1)
	})

	t.Run("RejectsASourceThatReturnsNoTopology", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// A user-supplied Source reporting success with nothing in it must not
		// take the process down.
		manager, err := topology.NewTopologyManager(ctx, nilSource{})

		require.Error(t, err)
		assert.Nil(t, manager)
	})

	t.Run("WarnsWhenEveryOnlineNodeIsHidden", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		logger, count := newCountingHandler()

		source := newControlledSource(testCluster(1,
			testNode("node-1", "arbiter", topology.StateOnline),
		))

		// Filtering away every online node leaves the driver with nothing to
		// route to, which is worth saying out loud rather than failing silently.
		manager, err := topology.NewTopologyManager(ctx, source,
			topology.WithForbiddenTiers("arbiter"),
			topology.WithLogger(logger),
		)
		require.NoError(t, err)
		defer manager.Close()

		assert.Equal(t, 1, count("every online node is hidden by filter"))
	})

	t.Run("StaysSilentWhenSomeNodeRemainsVisible", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		logger, count := newCountingHandler()

		source := newControlledSource(testCluster(1,
			testNode("node-1", "default", topology.StateOnline),
			testNode("node-2", "arbiter", topology.StateOnline),
		))

		manager, err := topology.NewTopologyManager(ctx, source,
			topology.WithForbiddenTiers("arbiter"),
			topology.WithLogger(logger),
		)
		require.NoError(t, err)
		defer manager.Close()

		assert.Equal(t, 0, count("every online node is hidden by filter"))
	})
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

	t.Run("HiddenNodeChangesDoNotWakeListeners", func(t *testing.T) {
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

		// Only the hidden node moved: the visible cluster is unchanged.
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

		received := awaitVersion(t, ctx, listener, 3)
		assert.Equal(t, []uint64{1, 3}, listener.versions(),
			"the hidden-only change must not reach the listeners")
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

	t.Run("MetadataIsUnfiltered", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		initial := testMetadataCluster()
		hidden := initial.Nodes["node-2"]
		hidden.Tier = "other"
		hidden.ReplicasetID = "rs-2"
		initial.Nodes["node-2"] = hidden
		initial.Replicasets["rs-2"] = topology.ReplicasetInfo{ID: "rs-2", Tier: "other", CurrentMasterID: "node-2"}
		source := newControlledSource(initial)
		manager, err := topology.NewTopologyManager(ctx, source, topology.WithForbiddenTiers("other"))
		require.NoError(t, err)
		defer manager.Close()

		listener := newMockListener()
		manager.AddListener(listener)
		visible := awaitTopology(t, ctx, listener)
		assert.Contains(t, visible.Nodes, "node-1")
		assert.NotContains(t, visible.Nodes, "node-2")
		assert.Equal(t, initial.Replicasets, visible.Replicasets)
		assert.Equal(t, initial.Tiers, visible.Tiers)
		assert.Equal(t, "node-2", visible.Replicasets["rs-2"].CurrentMasterID)

		next := initial.Clone()
		next.Version = 2
		rs := next.Replicasets["rs-2"]
		rs.CurrentMasterID = ""
		next.Replicasets["rs-2"] = rs
		source.updates <- next

		updated := awaitVersion(t, ctx, listener, 2)
		assert.Equal(t, visible.Nodes, updated.Nodes)
		assert.Empty(t, updated.Replicasets["rs-2"].CurrentMasterID)
		assert.Equal(t, next.Replicasets, updated.Replicasets)
		assert.Equal(t, []uint64{1, 2}, listener.versions())
	})

	t.Run("UnavailableMetadataStaysNil", func(t *testing.T) {
		manager, err := topology.NewTopologyManager(context.Background(), static.New([]string{"localhost:5432"}),
			topology.WithForbiddenTiers("arbiter"))
		require.NoError(t, err)
		defer manager.Close()
		listener := newMockListener()
		manager.AddListener(listener)

		visible := listener.getTopology()
		require.Len(t, visible.Nodes, 1)
		assert.Nil(t, visible.Replicasets)
		assert.Nil(t, visible.Tiers)
	})
}
