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

// snapshot builds the full cluster view a Source would report.
func snapshot(version uint64, nodes ...tpm.NodeInfo) *tpm.ClusterTopology {
	byID := make(map[string]tpm.NodeInfo, len(nodes))
	for _, n := range nodes {
		byID[n.ID] = n
	}

	return &tpm.ClusterTopology{Version: version, Nodes: byID}
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
