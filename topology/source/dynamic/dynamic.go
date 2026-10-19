package dynamic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"git.picodata.io/core/drivers/picodata-go/topology"
	"github.com/jackc/pgx/v5"
)

// defaultQueryTimeout specifies the maximum time allowed for a single node topology query.
const defaultQueryTimeout = 3 * time.Second

// Source discovers and fetches cluster topology by querying nodes dynamically.
type Source struct {
	logger         *slog.Logger
	baseConfig     *pgx.ConnConfig
	bootstrapAddrs []string
	// discoveredAddrs caches the addresses of online nodes found during the last successful fetch.
	discoveredAddrs atomic.Pointer[[]string]

	// Counted, not clock-based: consumers drop reports older than the last one
	// applied, and a clock stepping backwards would freeze them.
	version atomic.Uint64
}

// Option applies a configuration option to the Dynamic Source.
type Option func(*Source)

// WithLogger takes the root logger and wraps it with the dynamic package tag.
func WithLogger(l *slog.Logger) Option {
	return func(s *Source) {
		s.logger = l.With(slog.String("component", "topology.source.dynamic"))
	}
}

// New creates a new DynamicSource with a base connection configuration and initial bootstrap addresses.
func New(baseConfig *pgx.ConnConfig, bootstrapAddrs []string, opts ...Option) *Source {
	s := &Source{
		logger:         slog.New(slog.DiscardHandler),
		baseConfig:     baseConfig.Copy(),
		bootstrapAddrs: bootstrapAddrs,
	}

	for _, opt := range opts {
		opt(s)
	}

	s.discoveredAddrs.Store(&[]string{})
	return s
}

// Fetch queries the cluster nodes sequentially until it successfully retrieves the current topology.
// Replicasets and tiers without instances with pgproto addresses are not reported.
func (p *Source) Fetch(ctx context.Context) (*topology.ClusterTopology, error) {
	var addrs []string
	if discoveredAddrs := p.discoveredAddrs.Load(); discoveredAddrs != nil {
		addrs = append(addrs, *discoveredAddrs...)
	}
	addrs = append(addrs, p.bootstrapAddrs...)

	if len(addrs) == 0 {
		return nil, errors.New("dynamic source: no hosts available")
	}

	var snapshot *topology.ClusterTopology
	var errs []error

	for _, addr := range addrs {
		nodeCtx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
		res, err := p.queryTopology(nodeCtx, addr)
		cancel()

		if err != nil {
			errs = append(errs, fmt.Errorf("host %s: %w", addr, err))
			continue
		}
		if len(res.Nodes) == 0 {
			errs = append(errs, fmt.Errorf("host %s: empty topology", addr))
			continue
		}

		snapshot = res
		break
	}

	if snapshot == nil {
		return nil, fmt.Errorf("dynamic source: discovery failed: %w", errors.Join(errs...))
	}

	newAddrs := make([]string, 0, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		if node.State == topology.StateOnline && node.Address != "" {
			newAddrs = append(newAddrs, node.Address)
		}
	}
	p.discoveredAddrs.Store(&newAddrs)

	snapshot.Version = p.version.Add(1)
	return snapshot, nil
}

func (p *Source) queryTopology(ctx context.Context, addr string) (*topology.ClusterTopology, error) {
	const connsStateQuery = `
		SELECT pi.uuid,
		       ppa.address,
		       pi.current_state,
		       pi.tier,
		       pr.uuid,
		       pr.tier,
		       master.uuid,
		       pt.bucket_count
		FROM   _pico_peer_address AS ppa
		       JOIN _pico_instance AS pi
		         ON ppa.raft_id = pi.raft_id
		       LEFT JOIN _pico_replicaset AS pr
		         ON pi.replicaset_name = pr.name
		       LEFT JOIN _pico_instance AS master
		         ON pr.current_master_name = master.name
		       LEFT JOIN _pico_tier AS pt
		         ON pi.tier = pt.name
		WHERE  ppa.connection_type = 'pgproto';
	`

	nodeCfg := p.baseConfig.Copy()

	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		nodeCfg.Host = addr
	} else {
		nodeCfg.Host = host
		var portNum uint16
		if _, fmtErr := fmt.Sscanf(portStr, "%d", &portNum); fmtErr == nil {
			nodeCfg.Port = portNum
		}
	}

	conn, err := pgx.ConnectConfig(ctx, nodeCfg)
	if err != nil {
		return nil, fmt.Errorf("dynamic source: connection error: %w", err)
	}

	defer func() {
		if err := conn.Close(ctx); err != nil {
			p.logger.Warn("failed to close connection properly", slog.Any("error", err))
		}
	}()

	rows, err := conn.Query(ctx, connsStateQuery)
	if err != nil {
		return nil, fmt.Errorf("dynamic source: query error: %w", err)
	}
	defer rows.Close()

	snapshot := &topology.ClusterTopology{
		Nodes:        make(map[string]topology.NodeInfo),
		Replicasets:  make(map[string]topology.ReplicasetInfo),
		BucketCounts: make(map[string]uint64),
	}

	for rows.Next() {
		node, replicaset, bucketCount, err := scanTopologyRow(rows)
		if err != nil {
			return nil, err
		}

		snapshot.Nodes[node.ID] = node
		if replicaset != nil {
			snapshot.Replicasets[replicaset.ID] = *replicaset
		}
		if bucketCount != nil {
			snapshot.BucketCounts[node.Tier] = *bucketCount
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dynamic source: rows processing error: %w", err)
	}

	return snapshot, nil
}

func scanTopologyRow(rows pgx.Row) (topology.NodeInfo, *topology.ReplicasetInfo, *uint64, error) {
	var (
		node            topology.NodeInfo
		replicaset      *topology.ReplicasetInfo
		rawJSON         []byte
		replicasetID    *string
		replicasetTier  *string
		currentMasterID *string
		bucketCount     *uint64
	)

	if err := rows.Scan(&node.ID, &node.Address, &rawJSON, &node.Tier,
		&replicasetID, &replicasetTier, &currentMasterID, &bucketCount); err != nil {
		return topology.NodeInfo{}, nil, nil, fmt.Errorf("dynamic source: scan error: %w", err)
	}

	state, err := decodeState(rawJSON)
	if err != nil {
		return topology.NodeInfo{}, nil, nil, err
	}
	node.State = state

	if replicasetID != nil {
		node.ReplicasetID = *replicasetID
		replicaset = &topology.ReplicasetInfo{ID: *replicasetID}
		if replicasetTier != nil {
			replicaset.Tier = *replicasetTier
		}
		if currentMasterID != nil {
			replicaset.CurrentMasterID = *currentMasterID
		}
	}

	return node, replicaset, bucketCount, nil
}

func decodeState(rawJSON []byte) (string, error) {
	var values []json.RawMessage
	if err := json.Unmarshal(rawJSON, &values); err != nil {
		return "", fmt.Errorf("state payload must be a json array: %w", err)
	}
	if len(values) == 0 {
		return "", errors.New("state array is empty")
	}

	var state string
	if err := json.Unmarshal(values[0], &state); err != nil {
		return "", fmt.Errorf("state must be a string: %w", err)
	}
	return state, nil
}
