package topology

import "strings"

// Filter decides whether a node may be exposed to topology listeners.
//
// Allow is called for every node of every topology update, so implementations
// must be free of side effects and safe for concurrent use. The set of filters
// is fixed when the Manager is constructed and never changes afterwards.
type Filter interface {
	Allow(NodeInfo) bool
}

// forbiddenTiers hides nodes whose tier is listed in the set.
//
// The filter is a deny list on purpose: sources that do not report tiers leave
// [NodeInfo.Tier] empty, and an empty tier is never forbidden, so such sources
// keep working unchanged.
type forbiddenTiers map[string]struct{}

// newForbiddenTiers builds the tier set, dropping empty and duplicate entries.
// It returns nil when nothing is left to forbid.
func newForbiddenTiers(tiers []string) forbiddenTiers {
	set := make(forbiddenTiers, len(tiers))

	for _, tier := range tiers {
		tier = strings.TrimSpace(tier)
		if tier == "" {
			continue
		}

		set[tier] = struct{}{}
	}

	if len(set) == 0 {
		return nil
	}

	return set
}

// Allow implements [Filter].
func (f forbiddenTiers) Allow(node NodeInfo) bool {
	_, forbidden := f[node.Tier]

	return !forbidden
}
