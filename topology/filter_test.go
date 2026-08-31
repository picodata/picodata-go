package topology

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewForbiddenTiers(t *testing.T) {
	t.Run("TrimsAndDeduplicates", func(t *testing.T) {
		set := newForbiddenTiers([]string{" arbiter", " ", "readonly", "", "arbiter "})

		require.Len(t, set, 2)
		assert.Contains(t, set, "arbiter")
		assert.Contains(t, set, "readonly")
	})

	t.Run("NilWhenNothingToForbid", func(t *testing.T) {
		assert.Nil(t, newForbiddenTiers(nil))
		assert.Nil(t, newForbiddenTiers([]string{"", "  "}))
	})
}

func TestForbiddenTiersAllow(t *testing.T) {
	set := newForbiddenTiers([]string{"arbiter"})

	assert.False(t, set.Allow(NodeInfo{Tier: "arbiter"}))
	assert.True(t, set.Allow(NodeInfo{Tier: "default"}))

	// Sources that do not report tiers must keep working: an unknown tier is
	// never forbidden.
	assert.True(t, set.Allow(NodeInfo{}))
}
