package topology

import "context"

// Instance states.
const (
	StateOnline  = "Online"
	StateOffline = "Offline"
)

// NodeInfo represents the metadata and status of a single cluster node.
//
// State is required: consumers route to [StateOnline] nodes and drop
// [StateOffline] ones, so a Source that leaves it empty makes its nodes
// unusable.
type NodeInfo struct {
	ID      string
	Name    string
	Tier    string
	Address string
	State   string
}

// ClusterTopology holds the state of all nodes in the cluster at a specific version.
type ClusterTopology struct {
	Version uint64
	Nodes   map[string]NodeInfo
}

// OnlineCount reports how many nodes of the snapshot are online.
func (c *ClusterTopology) OnlineCount() int {
	var online int
	for _, node := range c.Nodes {
		if node.State == StateOnline {
			online++
		}
	}

	return online
}

// Source defines the interface to fetch the current cluster state.
type Source interface {
	Fetch(ctx context.Context) (*ClusterTopology, error)
}

// StreamingSource extends TopologySource to support real-time state updates via a channel.
type StreamingSource interface {
	Source
	Subscribe(ctx context.Context) (<-chan *ClusterTopology, error)
}

// Listener defines the callback interface for handling topology changes.
type Listener interface {
	OnTopologyChanged(*ClusterTopology)
}
