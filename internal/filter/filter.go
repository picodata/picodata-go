package filter

import "git.picodata.io/core/drivers/picodata-go/topology"

// Filter decides whether a node may be used for user connection pools.
//
// Allow is called for every node of every topology update, so implementations
// must be free of side effects and safe for concurrent use. The set of filters
// is fixed when the pool manager is constructed and never changes afterwards.
type Filter interface {
	Allow(topology.NodeInfo) bool
}
