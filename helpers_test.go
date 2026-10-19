package picodata

import (
	"context"
	"log/slog"
	"testing"

	tpm "git.picodata.io/core/drivers/picodata-go/topology"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Helpers shared by the tests of this package. Anything used by a single test
// file belongs next to it instead.

// recordHandler reports every message to onRecord, whatever its level.
type recordHandler struct {
	onRecord func(msg string)
}

func (h recordHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h recordHandler) Handle(_ context.Context, r slog.Record) error {
	h.onRecord(r.Message)
	return nil
}

func (h recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h recordHandler) WithGroup(string) slog.Handler { return h }

func newMockConfig(host string, port int) *pgxpool.Config {
	cfg, _ := pgxpool.ParseConfig("")
	cfg.ConnConfig.Host = host
	cfg.ConnConfig.Port = uint16(port)

	return cfg
}

// newTestPoolManager closes its pools when the test ends: every pgxpool starts
// a background health checker that would otherwise outlive it.
func newTestPoolManager(t *testing.T) *poolManager {
	t.Helper()

	pm := newPoolManager(newMockConfig("127.0.0.1", 5432), 1)
	t.Cleanup(pm.close)

	return pm
}

func newEmptyPool(t *testing.T) *Client {
	t.Helper()

	pm := newPoolManager(newMockConfig("127.0.0.1", 5432), 1)
	t.Cleanup(pm.close)

	return &Client{pm: pm, router: newRouter(pm)}
}

// node builds an online cluster node unless another state is given.
func node(uuid, address string, state ...string) tpm.NodeInfo {
	nodeState := tpm.StateOnline
	if len(state) > 0 {
		nodeState = state[0]
	}

	return tpm.NodeInfo{ID: uuid, Address: address, State: nodeState}
}

// testSnapshot adapts pool test fixtures to the read-only snapshot interface.
type testSnapshot struct {
	*tpm.ClusterTopology
}

func (s testSnapshot) Version() uint64 { return s.ClusterTopology.Version }

func (s testSnapshot) Node(id string) (tpm.NodeInfo, bool) {
	node, ok := s.ClusterTopology.Nodes[id]
	return node, ok
}

func (s testSnapshot) Nodes() []tpm.NodeInfo {
	nodes := make([]tpm.NodeInfo, 0, len(s.ClusterTopology.Nodes))
	for _, node := range s.ClusterTopology.Nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

func (s testSnapshot) Replicaset(id string) (tpm.ReplicasetInfo, bool) {
	replicaset, ok := s.Replicasets[id]
	return replicaset, ok
}

func (s testSnapshot) BucketCount(tier string) (uint64, bool) {
	count, ok := s.BucketCounts[tier]
	return count, ok
}

// snapshot builds a read-only cluster view for pool tests.
func snapshot(version uint64, nodes ...tpm.NodeInfo) testSnapshot {
	byID := make(map[string]tpm.NodeInfo, len(nodes))
	for _, n := range nodes {
		byID[n.ID] = n
	}

	return testSnapshot{&tpm.ClusterTopology{Version: version, Nodes: byID}}
}

// rotation returns the pools the manager serves, in the order strategies index.
func rotation(pm *poolManager) []instance {
	return *pm.instances.Load()
}

// uuidsOf returns the rotation order, which is what the balance strategies
// index into.
func uuidsOf(pools []instance) []string {
	order := make([]string, 0, len(pools))
	for _, inst := range pools {
		order = append(order, inst.uuid)
	}

	return order
}

// tierNodeIDs returns the uuids of the online nodes of a tier. The pool manager
// is keyed by uuid, so those are what the assertions look up.
func tierNodeIDs(t *testing.T, cluster *tpm.ClusterTopology, tier string) []string {
	t.Helper()

	var ids []string
	for _, node := range cluster.Nodes {
		if node.State == tpm.StateOnline && node.Tier == tier {
			ids = append(ids, node.ID)
		}
	}

	return ids
}
