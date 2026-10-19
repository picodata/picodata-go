package topology

import (
	"fmt"
	"log/slog"
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type excludeNodeByID string

// Allow implements Filter by rejecting the node with the specified ID.
func (id excludeNodeByID) Allow(node NodeInfo) bool { return node.ID != string(id) }

type listenerFunc func(Snapshot)

func (f listenerFunc) OnTopologyChanged(snapshot Snapshot) { f(snapshot) }

func TestTopologyManager_CheckOnlineNodes(t *testing.T) {
	for _, tt := range []struct {
		name      string
		nodes     []NodeInfo
		forbidden []string
		wantCount int
		wantError error
	}{
		{"EmptyTopology", nil, nil, 0, ErrNoOnlineNodes},
		{"OfflineNodesWithFilter", []NodeInfo{
			{ID: "node-1", Tier: "arbiter", State: StateOffline},
			{ID: "node-2", Tier: "default", State: StateOffline},
		}, []string{"arbiter"}, 0, ErrNoOnlineNodes},
		{"AllOnlineNodesFiltered", []NodeInfo{
			{ID: "node-1", Tier: "arbiter", State: StateOnline},
			{ID: "node-2", Tier: "default", State: StateOffline},
		}, []string{"arbiter"}, 0, ErrNoAllowedOnlineNodes},
		{"OnlineNodesWithoutFilters", []NodeInfo{
			{ID: "node-1", Tier: "default", State: StateOnline},
			{ID: "node-2", Tier: "default", State: StateOnline},
			{ID: "node-3", Tier: "default", State: StateOffline},
		}, nil, 2, nil},
		{"SomeOnlineNodesAllowed", []NodeInfo{
			{ID: "node-1", Tier: "default", State: StateOnline},
			{ID: "node-2", Tier: "default", State: StateOnline},
			{ID: "node-3", Tier: "arbiter", State: StateOnline},
			{ID: "node-4", Tier: "default", State: StateOffline},
		}, []string{"arbiter"}, 2, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			report := &ClusterTopology{Version: 1, Nodes: make(map[string]NodeInfo)}
			for _, node := range tt.nodes {
				report.Nodes[node.ID] = node
			}
			manager := &Manager{logger: slog.New(slog.DiscardHandler)}
			WithForbiddenTiers(tt.forbidden...)(manager)
			manager.store(report)

			count, err := manager.CheckOnlineNodes()
			assert.Equal(t, tt.wantCount, count)
			assert.ErrorIs(t, err, tt.wantError)
		})
	}
}

func TestTopologyManager_SnapshotIsDetachedFromTheSource(t *testing.T) {
	newReport := func(version uint64) *ClusterTopology {
		return &ClusterTopology{
			Version: version,
			Nodes: map[string]NodeInfo{
				"node-1": {ID: "node-1", ReplicasetID: "rs-1", Tier: "default", State: StateOnline},
				"node-2": {ID: "node-2", ReplicasetID: "rs-2", Tier: "other", State: StateOnline},
			},
			Replicasets: map[string]ReplicasetInfo{
				"rs-1": {ID: "rs-1", Tier: "default", CurrentMasterID: "node-1"},
				"rs-2": {ID: "rs-2", Tier: "other", CurrentMasterID: "node-2"},
			},
			BucketCounts: map[string]uint64{"default": 3000 + version, "other": 3000},
		}
	}

	for _, filtered := range []bool{false, true} {
		t.Run(fmt.Sprintf("Filtered=%t", filtered), func(t *testing.T) {
			manager := &Manager{logger: slog.New(slog.DiscardHandler)}
			if filtered {
				WithForbiddenTiers("other")(manager)
			}

			for version := uint64(1); version <= 2; version++ {
				incoming := newReport(version)
				if version == 1 {
					manager.store(incoming)
				} else {
					manager.applyAndNotify(incoming)
				}

				expected := newReport(version)
				expectedVisible := newReport(version)
				if filtered {
					delete(expectedVisible.Nodes, "node-2")
					delete(expectedVisible.Replicasets, "rs-2")
					delete(expectedVisible.BucketCounts, "other")
				}

				// Mutating the source's report after storing it must not change
				// either the full or the filtered state held by the manager.
				incoming.Version = 99
				delete(incoming.Nodes, "node-1")
				incoming.Nodes["extra"] = NodeInfo{ID: "extra", Tier: "default", State: StateOnline}
				incoming.Replicasets["rs-1"] = ReplicasetInfo{}
				delete(incoming.Replicasets, "rs-2")
				incoming.BucketCounts["default"] = 0
				delete(incoming.BucketCounts, "other")

				assert.Equal(t, expected, manager.current.Load().topology)
				assert.Equal(t, expectedVisible, manager.filtered.Load().topology)
			}
		})
	}
}

func TestTopologyManager_ListenersShareImmutableSnapshot(t *testing.T) {
	report := &ClusterTopology{
		Version: 1,
		Nodes: map[string]NodeInfo{
			"node-1": {ID: "node-1", Name: "first", ReplicasetID: "rs-1", Tier: "default", Address: "node-1:5432", State: StateOnline},
			"node-2": {ID: "node-2", Name: "second", ReplicasetID: "rs-1", Tier: "default", Address: "node-2:5432", State: StateOffline},
		},
		Replicasets: map[string]ReplicasetInfo{
			"rs-1": {ID: "rs-1", Tier: "default", CurrentMasterID: "node-1"},
		},
		BucketCounts: map[string]uint64{"default": 3000},
	}
	expected := &ClusterTopology{
		Version:      report.Version,
		Nodes:        maps.Clone(report.Nodes),
		Replicasets:  maps.Clone(report.Replicasets),
		BucketCounts: maps.Clone(report.BucketCounts),
	}
	manager := &Manager{logger: slog.New(slog.DiscardHandler)}
	manager.store(report)

	var received [2][]Snapshot
	for i := range received {
		manager.AddListener(listenerFunc(func(snapshot Snapshot) {
			received[i] = append(received[i], snapshot)
		}))
	}
	require.Len(t, received[0], 1)
	require.Len(t, received[1], 1)
	first := received[0][0]
	assert.Same(t, first, received[1][0])
	assert.Same(t, manager.filtered.Load(), first)
	assert.Equal(t, report.Version, first.Version())
	nodes := first.Nodes()
	expectedNodes := []NodeInfo{report.Nodes["node-1"], report.Nodes["node-2"]}
	require.Len(t, nodes, 2)
	assert.ElementsMatch(t, expectedNodes, nodes)
	nodes[0].Address = "changed:5432"
	assert.ElementsMatch(t, expectedNodes, first.Nodes(), "changing the returned slice must not change the snapshot")
	assert.Equal(t, 1, first.OnlineCount())
	node, ok := first.Node("node-1")
	assert.True(t, ok)
	assert.Equal(t, report.Nodes["node-1"], node)
	replicaset, ok := first.Replicaset("rs-1")
	assert.True(t, ok)
	assert.Equal(t, report.Replicasets["rs-1"], replicaset)
	count, ok := first.BucketCount("default")
	assert.True(t, ok)
	assert.Equal(t, uint64(3000), count)

	_, ok = first.Node("missing")
	assert.False(t, ok)
	_, ok = first.Replicaset("missing")
	assert.False(t, ok)
	_, ok = first.BucketCount("missing")
	assert.False(t, ok)

	// A consumer explicitly requests its own mutable copy.
	editable := first.Clone()
	editable.Version = 99
	delete(editable.Nodes, "node-1")
	delete(editable.Replicasets, "rs-1")
	editable.BucketCounts["default"] = 0
	assert.Equal(t, expected, first.Clone())
	assert.Equal(t, expected, manager.current.Load().topology)

	next := report.Clone()
	next.Version = 2
	node.Address = "node-1-new:5432"
	next.Nodes[node.ID] = node
	node = next.Nodes["node-2"]
	node.State = StateOnline
	next.Nodes[node.ID] = node
	replicaset.CurrentMasterID = node.ID
	next.Replicasets[replicaset.ID] = replicaset
	next.BucketCounts["default"] = 4000
	manager.applyAndNotify(next)

	require.Len(t, received[0], 2)
	require.Len(t, received[1], 2)
	assert.Same(t, received[0][1], received[1][1])
	assert.NotSame(t, first, received[0][1])
	assert.Equal(t, next, received[0][1].Clone())
	assert.Equal(t, expected, first.Clone(), "an update must not change a previously published snapshot")
}

// Filtering out the master node leaves its replica visible and preserves
// CurrentMasterID: filtering must not change the actual master of a retained replicaset.
func TestTopologyManager_PreservesMasterReferenceWhenMasterIsFilteredOut(t *testing.T) {
	report := &ClusterTopology{
		Version: 1,
		Nodes: map[string]NodeInfo{
			"master":  {ID: "master", ReplicasetID: "rs-1", Tier: "default", State: StateOnline},
			"replica": {ID: "replica", ReplicasetID: "rs-1", Tier: "default", State: StateOnline},
		},
		Replicasets: map[string]ReplicasetInfo{
			"rs-1": {ID: "rs-1", Tier: "default", CurrentMasterID: "master"},
		},
		BucketCounts: map[string]uint64{"default": 3000},
	}
	manager := &Manager{logger: slog.New(slog.DiscardHandler), filters: []Filter{excludeNodeByID("master")}}
	manager.store(report)

	assert.Equal(t, report, manager.current.Load().topology)
	visible := manager.filtered.Load().Clone()
	assert.Len(t, visible.Nodes, 1)
	assert.Contains(t, visible.Nodes, "replica")
	assert.Equal(t, report.Replicasets, visible.Replicasets)
	assert.Equal(t, report.BucketCounts, visible.BucketCounts)
	assert.Equal(t, "master", visible.Replicasets["rs-1"].CurrentMasterID)
}
