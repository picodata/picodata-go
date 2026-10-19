package topology

import (
	"context"
	"maps"
)

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
// ReplicasetID is empty when replicaset metadata is unavailable.
// Tier is the instance's tier and is used by node filters.
type NodeInfo struct {
	ID           string
	Name         string
	ReplicasetID string
	Tier         string
	Address      string
	State        string
}

// ReplicasetInfo holds the tier and current master of a replicaset.
// CurrentMasterID is empty when the master is unknown.
type ReplicasetInfo struct {
	ID              string
	Tier            string
	CurrentMasterID string
}

// ClusterTopology is a mutable report of the cluster state at a specific version.
// Nodes and Replicasets are keyed by ID (UUIDs for the dynamic source).
// BucketCounts maps tier names to their configured bucket counts.
// Each report replaces the previous topology, including its metadata.
// Replicasets and BucketCounts may be nil when a source does not provide that metadata.
// CurrentMasterID may refer to a node absent from Nodes, including a filtered node.
// The manager copies reports before publishing them as read-only [Snapshot] values.
type ClusterTopology struct {
	Version      uint64
	Nodes        map[string]NodeInfo
	Replicasets  map[string]ReplicasetInfo
	BucketCounts map[string]uint64
}

// Clone returns a detached copy of the topology and its maps.
func (c *ClusterTopology) Clone() *ClusterTopology {
	if c == nil {
		return nil
	}
	return &ClusterTopology{
		Version:      c.Version,
		Nodes:        maps.Clone(c.Nodes),
		Replicasets:  maps.Clone(c.Replicasets),
		BucketCounts: maps.Clone(c.BucketCounts),
	}
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

// Snapshot is a read-only view of the cluster state at a specific version.
// Nodes and replicasets are returned by value; their maps are not exposed.
// Nodes returns a detached slice in unspecified order.
// Clone returns a mutable copy for consumers that need to change the data.
type Snapshot interface {
	Version() uint64
	Node(id string) (NodeInfo, bool)
	Nodes() []NodeInfo
	Replicaset(id string) (ReplicasetInfo, bool)
	BucketCount(tier string) (uint64, bool)
	OnlineCount() int
	Clone() *ClusterTopology
}

// Source defines the interface to fetch the current cluster state.
// A report must not be mutated concurrently with its consumers.
type Source interface {
	Fetch(ctx context.Context) (*ClusterTopology, error)
}

// StreamingSource extends TopologySource to support real-time state updates via a channel.
// Reports must remain stable while being consumed; sending a report does not
// acknowledge that the manager has finished copying it.
type StreamingSource interface {
	Source
	Subscribe(ctx context.Context) (<-chan *ClusterTopology, error)
}

// Listener is notified whenever the visible cluster changes.
//
// The argument is the whole cluster, not a delta: an instance missing from it
// has left, so replace the view rather than merge into it, and make reapplying
// the same snapshot a no-op. Never call back into the [Manager].
// Listeners share a read-only snapshot. Use Clone to obtain a mutable copy.
type Listener interface {
	OnTopologyChanged(Snapshot)
}
