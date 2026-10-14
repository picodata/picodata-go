package picodata

import (
	"fmt"
	"log/slog"
	"time"

	"git.picodata.io/core/drivers/picodata-go/strategies"
	"git.picodata.io/core/drivers/picodata-go/topology"
)

type clientOpts struct {
	logger                 *slog.Logger
	topologySource         topology.Source
	balanceStrategy        strategies.BalanceStrategy
	forbiddenTiers         []string
	bootstrapHosts         []string
	disableTopologyUpdate  bool
	topologyUpdateInterval *time.Duration
	maxConnsPerInstance    int32
}

// PoolOption applies a single optional configuration to a pool.
type PoolOption func(*clientOpts) error

// WithTopologySource sets a custom topology source
func WithTopologySource(source topology.Source) PoolOption {
	return func(p *clientOpts) error {
		if source == nil {
			return fmt.Errorf("topology source is nil")
		}
		p.topologySource = source
		return nil
	}
}

// WithBalanceStrategy defines logic of pool balancing across connections
func WithBalanceStrategy(strategy strategies.BalanceStrategy) PoolOption {
	return func(p *clientOpts) error {
		if strategy == nil {
			return fmt.Errorf("balance strategy is nil")
		}
		p.balanceStrategy = strategy
		return nil
	}
}

// WithLogger sets a custom pool internal logger to print information
func WithLogger(customLogger *slog.Logger) PoolOption {
	return func(p *clientOpts) error {
		if customLogger == nil {
			return fmt.Errorf("logger is nil")
		}
		p.logger = customLogger
		return nil
	}
}

// WithDisableTopologyUpdate disables backfround poller that actualizes topology
func WithDisableTopologyUpdate() PoolOption {
	return func(p *clientOpts) error {
		p.disableTopologyUpdate = true
		return nil
	}
}

// WithTopologyUpdateInterval sets the interval for the built-in topology poller.
// The default is five seconds. The interval must be at least one second.
// It is ignored when using [WithDisableTopologyUpdate].
// It does not control a source supplied via [WithTopologySource].
func WithTopologyUpdateInterval(interval time.Duration) PoolOption {
	return func(p *clientOpts) error {
		p.topologyUpdateInterval = &interval
		return nil
	}
}

// WithBootstrapHosts appends alternative nodes for the initial discovery phase.
func WithBootstrapHosts(hosts ...string) PoolOption {
	return func(p *clientOpts) error {
		// If the user passes an empty slice or nil, we do nothing.
		// This prevents overwriting or corrupting already discovered hosts.
		if len(hosts) == 0 {
			return nil
		}

		// Filter out empty strings to avoid invalid network addresses later.
		for _, host := range hosts {
			if host != "" {
				p.bootstrapHosts = append(p.bootstrapHosts, host)
			}
		}

		return nil
	}
}

// WithMaxConnPerInstance limits the number of connections opened per instance.
func WithMaxConnPerInstance(maxConns int32) PoolOption {
	return func(p *clientOpts) error {
		p.maxConnsPerInstance = maxConns
		return nil
	}
}

// WithForbiddenTiers excludes nodes of the given tiers from the connection pool,
// for example WithForbiddenTiers("arbiter", "readonly").
//
// Filtering applies to the pools the client routes user queries to. Topology
// discovery itself is not affected: the driver keeps polling the excluded nodes,
// so it still notices when the allowed ones come back online.
//
// Empty and whitespace-only entries are ignored.
func WithForbiddenTiers(tiers ...string) PoolOption {
	return func(p *clientOpts) error {
		p.forbiddenTiers = append(p.forbiddenTiers, tiers...)

		return nil
	}
}
