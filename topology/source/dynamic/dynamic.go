package dynamic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"git.picodata.io/core/drivers/picodata-go/logger"
	"git.picodata.io/core/drivers/picodata-go/topology"
	"github.com/jackc/pgx/v5"
)

// defaultQueryTimeout specifies the maximum time allowed for a single node topology query.
const defaultQueryTimeout = 3 * time.Second

// Source discovers and fetches cluster topology by querying nodes dynamically.
type Source struct {
	baseConfig     *pgx.ConnConfig
	bootstrapAddrs []string
	// discoveredAddrs caches the addresses of online nodes found during the last successful fetch.
	discoveredAddrs atomic.Pointer[[]string]
}

// New creates a new DynamicSource with a base connection configuration and initial bootstrap addresses.
func New(baseConfig *pgx.ConnConfig, bootstrapAddrs []string) *Source {
	p := &Source{
		baseConfig:     baseConfig.Copy(),
		bootstrapAddrs: bootstrapAddrs,
	}

	p.discoveredAddrs.Store(&[]string{})
	return p
}

// Fetch queries the cluster nodes sequentially until it successfully retrieves the current topology.
func (p *Source) Fetch(ctx context.Context) (*topology.ClusterTopology, error) {
	var addrs []string
	if discoveredAddrs := p.discoveredAddrs.Load(); discoveredAddrs != nil {
		addrs = append(addrs, *discoveredAddrs...)
	}
	addrs = append(addrs, p.bootstrapAddrs...)

	if len(addrs) == 0 {
		return nil, errors.New("dynamic source: no hosts available")
	}

	var nodesMap map[string]topology.NodeInfo
	var errs []error

	for _, addr := range addrs {
		nodeCtx, cancel := context.WithTimeout(ctx, defaultQueryTimeout)
		res, err := p.queryInfo(nodeCtx, addr)
		cancel()

		if err != nil {
			errs = append(errs, fmt.Errorf("host %s: %w", addr, err))
			continue
		}

		nodesMap = res
		break
	}

	if len(nodesMap) == 0 {
		return nil, fmt.Errorf("dynamic source: discovery failed: %w", errors.Join(errs...))
	}

	newAddrs := make([]string, 0, len(nodesMap))
	for _, node := range nodesMap {
		if node.State == topology.StateOnline && node.Address != "" {
			newAddrs = append(newAddrs, node.Address)
		}
	}
	p.discoveredAddrs.Store(&newAddrs)

	return &topology.ClusterTopology{
		Version: uint64(time.Now().UnixNano()),
		Nodes:   nodesMap,
	}, nil
}

func (p *Source) queryInfo(ctx context.Context, addr string) (map[string]topology.NodeInfo, error) {
	const connsStateQuery = `
		SELECT pi.uuid,
		       ppa.address,
		       pi.current_state,
		       pi.tier
		FROM   _pico_peer_address AS ppa
		       JOIN _pico_instance AS pi
		         ON ppa.raft_id = pi.raft_id
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
			logger.Log(logger.LevelWarn, "[topology] dynamic source: failed to close connection properly: %v", err)
		}
	}()

	rows, err := conn.Query(ctx, connsStateQuery)
	if err != nil {
		return nil, fmt.Errorf("dynamic source: query error: %w", err)
	}
	defer rows.Close()

	nodes := make(map[string]topology.NodeInfo)

	for rows.Next() {
		var (
			nodeUUID string
			connAddr string
			rawJSON  []byte
			nodeTier string
		)

		if err := rows.Scan(&nodeUUID, &connAddr, &rawJSON, &nodeTier); err != nil {
			return nil, fmt.Errorf("dynamic source: scan error: %w", err)
		}

		var values []json.RawMessage
		if err := json.Unmarshal(rawJSON, &values); err != nil {
			return nil, fmt.Errorf("state payload must be a json array: %w", err)
		}
		if len(values) < 1 {
			return nil, fmt.Errorf("state array is empty")
		}

		var stateStr string
		if err := json.Unmarshal(values[0], &stateStr); err != nil {
			return nil, fmt.Errorf("state must be a string: %w", err)
		}

		nodes[nodeUUID] = topology.NodeInfo{
			ID:      nodeUUID,
			Address: connAddr,
			State:   stateStr,
			Tier:    nodeTier,
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dynamic source: rows processing error: %w", err)
	}

	return nodes, nil
}
