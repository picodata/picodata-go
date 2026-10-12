package topology

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

type hiddenNode string

func (id hiddenNode) Allow(node NodeInfo) bool { return node.ID != string(id) }

func TestTopologyManager_FilterKeepsHiddenMaster(t *testing.T) {
	report := &ClusterTopology{
		Version: 1,
		Nodes: map[string]NodeInfo{
			"master":  {ID: "master", ReplicasetID: "rs-1", Tier: "default", State: StateOnline},
			"replica": {ID: "replica", ReplicasetID: "rs-1", Tier: "default", State: StateOnline},
		},
		Replicasets: map[string]ReplicasetInfo{
			"rs-1": {ID: "rs-1", Tier: "default", CurrentMasterID: "master"},
		},
		Tiers: map[string]TierInfo{"default": {Name: "default", BucketCount: 3000}},
	}
	manager := &Manager{logger: slog.New(slog.DiscardHandler), filters: []Filter{hiddenNode("master")}}
	manager.store(report)

	visible := manager.GetFilteredTopology()
	assert.Len(t, visible.Nodes, 1)
	assert.Contains(t, visible.Nodes, "replica")
	assert.Equal(t, report.Replicasets, visible.Replicasets)
	assert.Equal(t, report.Tiers, visible.Tiers)
	assert.Equal(t, "master", visible.Replicasets["rs-1"].CurrentMasterID)
}
