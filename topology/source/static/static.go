package static

import (
	"context"
	"fmt"

	"git.picodata.io/core/drivers/picodata-go/topology"
)

// Source implements TopologySource using a fixed, pre-defined set of nodes.
type Source struct {
	topology *topology.ClusterTopology
}

// New initializes a StaticSource with nodes generated from the provided addresses.
func New(addrs []string) *Source {
	nodes := make(map[string]topology.NodeInfo, len(addrs))

	for i, addr := range addrs {
		id := fmt.Sprintf("static-%d", i)
		nodes[id] = topology.NodeInfo{
			ID:      id,
			Address: addr,
		}
	}

	return &Source{
		topology: &topology.ClusterTopology{
			Version: 1,
			Nodes:   nodes,
		},
	}
}

// Fetch returns the static cluster topology immediately without any external network calls.
func (p *Source) Fetch(_ context.Context) (*topology.ClusterTopology, error) {
	return p.topology, nil
}
