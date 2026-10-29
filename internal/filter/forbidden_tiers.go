package filter

import (
	"strings"

	"git.picodata.io/core/drivers/picodata-go/topology"
)

// forbiddenTiers excludes nodes whose tier is listed in the set.
//
// The filter is a deny list on purpose: sources that do not report tiers leave
// [topology.NodeInfo.Tier] empty, and an empty tier is never forbidden, so such
// sources keep working unchanged.
type forbiddenTiers map[string]struct{}

// NewForbiddenTiers builds the tier set, dropping empty and duplicate entries.
// It returns nil when nothing is left to forbid.
func NewForbiddenTiers(tiers []string) Filter {
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
func (f forbiddenTiers) Allow(node topology.NodeInfo) bool {
	_, forbidden := f[node.Tier]

	return !forbidden
}
