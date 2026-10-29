package filter

import (
	"testing"

	tpm "git.picodata.io/core/drivers/picodata-go/topology"
	"github.com/stretchr/testify/assert"
)

func TestNewForbiddenTiers(t *testing.T) {
	t.Run("NormalizesTierNames", func(t *testing.T) {
		// Whitespace, duplicates, and empty entries must not change tier selection.
		filter := NewForbiddenTiers([]string{" arbiter", " ", "readonly", "", "arbiter "})
		assert.False(t, filter.Allow(tpm.NodeInfo{Tier: "arbiter"}))
		assert.False(t, filter.Allow(tpm.NodeInfo{Tier: "readonly"}))
		assert.True(t, filter.Allow(tpm.NodeInfo{Tier: "default"}))
		assert.True(t, filter.Allow(tpm.NodeInfo{}))
	})

	t.Run("NilWhenNothingToForbid", func(t *testing.T) {
		// An empty deny list does not add a filter to the pool configuration.
		for _, tiers := range [][]string{nil, {"", "  "}} {
			filter := NewForbiddenTiers(tiers)
			assert.True(t, filter == nil, "an empty tier list must return a nil interface")
		}
	})
}

func TestForbiddenTiersAllow(t *testing.T) {
	filter := NewForbiddenTiers([]string{"arbiter", "readonly"})

	// Sources that do not report tiers must keep working: an unknown tier is
	// never forbidden.
	assert.False(t, filter.Allow(tpm.NodeInfo{Tier: "arbiter"}))
	assert.False(t, filter.Allow(tpm.NodeInfo{Tier: "readonly"}))
	assert.True(t, filter.Allow(tpm.NodeInfo{Tier: "default"}))
	assert.True(t, filter.Allow(tpm.NodeInfo{}))

	t.Run("AllowIgnoresNodeState", func(t *testing.T) {
		// Reconciliation handles state; the tier filter only checks tier membership.
		for _, state := range []string{tpm.StateOnline, tpm.StateOffline, "", "Expelled"} {
			assert.False(t, filter.Allow(tpm.NodeInfo{Tier: "arbiter", State: state}))
			assert.False(t, filter.Allow(tpm.NodeInfo{Tier: "readonly", State: state}))
			assert.True(t, filter.Allow(tpm.NodeInfo{Tier: "default", State: state}))
			assert.True(t, filter.Allow(tpm.NodeInfo{State: state}))
		}
	})
}
