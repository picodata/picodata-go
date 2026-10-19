package dynamic

import (
	"fmt"
	"testing"

	"git.picodata.io/core/drivers/picodata-go/topology"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type topologyRowScanner struct {
	values []any
}

func (r *topologyRowScanner) Scan(dest ...any) error {
	oids := []uint32{
		pgtype.TextOID, pgtype.TextOID, pgtype.JSONBOID, pgtype.TextOID,
		pgtype.TextOID, pgtype.TextOID, pgtype.TextOID, pgtype.Int8OID,
	}
	fields := make([]pgconn.FieldDescription, len(oids))
	values := make([][]byte, len(oids))
	for i, oid := range oids {
		fields[i].DataTypeOID = oid
		if value := r.values[i]; value != nil {
			values[i] = fmt.Appendf(nil, "%v", value)
		}
	}
	return pgx.ScanRow(pgtype.NewMap(), fields, values, dest...)
}

func topologyRowValues(id, replicaset, tier, master string) []any {
	return []any{id, id + ":5432", `["Online", 1]`, tier, replicaset, tier, master, 3000}
}

func TestScanTopologyRow(t *testing.T) {
	node, replicaset, bucketCount, err := scanTopologyRow(&topologyRowScanner{values: topologyRowValues("node-1", "rs-1", "default", "node-1")})
	require.NoError(t, err)
	assert.Equal(t, topology.NodeInfo{
		ID: "node-1", Address: "node-1:5432", ReplicasetID: "rs-1", Tier: "default", State: topology.StateOnline,
	}, node)
	assert.Equal(t, &topology.ReplicasetInfo{
		ID: "rs-1", Tier: "default", CurrentMasterID: "node-1",
	}, replicaset)
	require.NotNil(t, bucketCount)
	assert.Equal(t, uint64(3000), *bucketCount)
}

func TestScanTopologyRowNullableMetadata(t *testing.T) {
	for _, tt := range []struct {
		name       string
		nulls      []int
		replicaset bool
		tier       bool
		master     string
	}{
		{"UnknownMaster", []int{6}, true, true, ""},
		{"MissingReplicaset", []int{4, 5, 6}, false, true, ""},
		{"MissingTier", []int{7}, true, false, "node-1"},
		{"MissingMetadata", []int{4, 5, 6, 7}, false, false, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			row := topologyRowValues("node-1", "rs-1", "default", "node-1")
			for _, i := range tt.nulls {
				row[i] = nil
			}
			node, replicaset, bucketCount, err := scanTopologyRow(&topologyRowScanner{values: row})
			require.NoError(t, err)
			assert.Equal(t, "default", node.Tier)
			assert.Equal(t, topology.StateOnline, node.State)
			if tt.replicaset {
				assert.Equal(t, "rs-1", node.ReplicasetID)
				assert.Equal(t, &topology.ReplicasetInfo{
					ID: "rs-1", Tier: "default", CurrentMasterID: tt.master,
				}, replicaset)
			} else {
				assert.Empty(t, node.ReplicasetID)
				assert.Nil(t, replicaset)
			}
			if tt.tier {
				require.NotNil(t, bucketCount)
				assert.Equal(t, uint64(3000), *bucketCount)
			} else {
				assert.Nil(t, bucketCount)
			}
		})
	}
}

func TestScanTopologyRowKeepsInstanceTier(t *testing.T) {
	row := topologyRowValues("node-1", "rs-1", "default", "node-1")
	row[5] = "other"
	node, replicaset, bucketCount, err := scanTopologyRow(&topologyRowScanner{values: row})
	require.NoError(t, err)
	assert.Equal(t, "default", node.Tier)
	assert.Equal(t, "other", replicaset.Tier)
	require.NotNil(t, bucketCount)
	assert.Equal(t, uint64(3000), *bucketCount)
}

func TestScanTopologyRowErrors(t *testing.T) {
	for _, tt := range []struct {
		name   string
		column int
		value  string
	}{
		{"ScanError", 7, "not a bucket count"},
		{"StateError", 2, "not json"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bad := topologyRowValues("node-2", "rs-1", "default", "node-1")
			bad[tt.column] = tt.value
			node, replicaset, bucketCount, err := scanTopologyRow(&topologyRowScanner{values: bad})
			require.Error(t, err)
			assert.Equal(t, topology.NodeInfo{}, node)
			assert.Nil(t, replicaset)
			assert.Nil(t, bucketCount)
		})
	}
}

func TestDecodeState(t *testing.T) {
	for _, state := range []string{topology.StateOnline, topology.StateOffline} {
		got, err := decodeState(fmt.Appendf(nil, `[%q, 1]`, state))
		require.NoError(t, err)
		assert.Equal(t, state, got)
	}
	for _, payload := range []string{`not json`, `{}`, `[]`, `[1]`} {
		_, err := decodeState([]byte(payload))
		assert.Error(t, err, payload)
	}
}
