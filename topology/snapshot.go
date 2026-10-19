package topology

// clusterSnapshot owns the topology maps. They are never modified after publication.
type clusterSnapshot struct {
	topology *ClusterTopology
}

func newClusterSnapshot(report *ClusterTopology) *clusterSnapshot {
	return &clusterSnapshot{topology: report.Clone()}
}

func (s *clusterSnapshot) Version() uint64 { return s.topology.Version }

func (s *clusterSnapshot) Node(id string) (NodeInfo, bool) {
	node, ok := s.topology.Nodes[id]
	return node, ok
}

func (s *clusterSnapshot) Nodes() []NodeInfo {
	nodes := make([]NodeInfo, 0, len(s.topology.Nodes))
	for _, node := range s.topology.Nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

func (s *clusterSnapshot) Replicaset(id string) (ReplicasetInfo, bool) {
	replicaset, ok := s.topology.Replicasets[id]
	return replicaset, ok
}

func (s *clusterSnapshot) BucketCount(tier string) (uint64, bool) {
	count, ok := s.topology.BucketCounts[tier]
	return count, ok
}

func (s *clusterSnapshot) OnlineCount() int { return s.topology.OnlineCount() }

func (s *clusterSnapshot) Clone() *ClusterTopology { return s.topology.Clone() }
