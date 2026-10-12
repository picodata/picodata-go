package topology_test

import (
	"testing"

	"git.picodata.io/core/drivers/picodata-go/topology"
	"github.com/stretchr/testify/assert"
)

func TestClusterTopology_Clone(t *testing.T) {
	t.Run("DetachedMaps", func(t *testing.T) {
		report := testMetadataCluster()
		copy := report.Clone()
		assert.Equal(t, report, copy)

		copy.Version++
		copy.Nodes["node-1"] = topology.NodeInfo{}
		copy.Replicasets["rs-1"] = topology.ReplicasetInfo{}
		copy.Tiers["default"] = topology.TierInfo{}
		assert.Equal(t, testMetadataCluster(), report)
	})

	t.Run("NilMaps", func(t *testing.T) {
		report := &topology.ClusterTopology{Version: 1}
		copy := report.Clone()
		assert.Equal(t, report, copy)
		assert.Nil(t, copy.Nodes)
		assert.Nil(t, copy.Replicasets)
		assert.Nil(t, copy.Tiers)
	})

	t.Run("EmptyMaps", func(t *testing.T) {
		report := &topology.ClusterTopology{
			Nodes:       map[string]topology.NodeInfo{},
			Replicasets: map[string]topology.ReplicasetInfo{},
			Tiers:       map[string]topology.TierInfo{},
		}
		copy := report.Clone()
		assert.NotNil(t, copy.Nodes)
		assert.NotNil(t, copy.Replicasets)
		assert.NotNil(t, copy.Tiers)
		copy.Nodes["node-1"] = topology.NodeInfo{ID: "node-1"}
		copy.Replicasets["rs-1"] = topology.ReplicasetInfo{ID: "rs-1"}
		copy.Tiers["default"] = topology.TierInfo{Name: "default"}
		assert.Empty(t, report.Nodes)
		assert.Empty(t, report.Replicasets)
		assert.Empty(t, report.Tiers)
	})

	t.Run("NilTopology", func(t *testing.T) {
		var report *topology.ClusterTopology
		assert.Nil(t, report.Clone())
	})
}
