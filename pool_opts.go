package picodata

import (
	"fmt"

	"git.picodata.io/core/drivers/picodata-go/logger"
	"git.picodata.io/core/drivers/picodata-go/strategies"
	"git.picodata.io/core/drivers/picodata-go/topology"
)

type poolOpts struct {
	logLevel              logger.LogLevel
	logger                logger.Logger
	topologySource        topology.Source
	balanceStrategy       strategies.BalanceStrategy
	bootstrapHosts        []string
	disableTopologyUpdate bool
	maxConnsPerInstance   int32
}

// PoolOption applies a single optional configuration to a pool.
type PoolOption func(*poolOpts) error

// WithTopologySource sets a custom topology source
func WithTopologySource(source topology.Source) PoolOption {
	return func(p *poolOpts) error {
		if source == nil {
			return fmt.Errorf("topology source is nil")
		}
		p.topologySource = source
		return nil
	}
}

// WithBalanceStrategy defines logic of pool balancing across connections
func WithBalanceStrategy(strategy strategies.BalanceStrategy) PoolOption {
	return func(p *poolOpts) error {
		if strategy == nil {
			return fmt.Errorf("balance strategy is nil")
		}
		p.balanceStrategy = strategy
		return nil
	}
}

// WithLogger sets a custom pool internal logger to print information
func WithLogger(customLogger logger.Logger) PoolOption {
	return func(p *poolOpts) error {
		if customLogger == nil {
			return fmt.Errorf("logger is nil")
		}
		p.logger = customLogger
		return nil
	}
}

// WithLogLevel set a logger level for pool internal logger
func WithLogLevel(level logger.LogLevel) PoolOption {
	return func(p *poolOpts) error {
		p.logLevel = level
		return nil
	}
}

// WithDisableTopologyUpdate disables backfround poller that actualizes topology
func WithDisableTopologyUpdate() PoolOption {
	return func(p *poolOpts) error {
		p.disableTopologyUpdate = true
		return nil
	}
}

// WithBootstrapHosts appends alternative nodes for the initial discovery phase.
func WithBootstrapHosts(hosts ...string) PoolOption {
	return func(p *poolOpts) error {
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
	return func(p *poolOpts) error {
		p.maxConnsPerInstance = maxConns
		return nil
	}
}
