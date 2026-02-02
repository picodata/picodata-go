package picodata

import (
	"fmt"

	"github.com/picodata/picodata-go/logger"
	"github.com/picodata/picodata-go/strategies"
)

type poolOpts struct {
	logLevel               logger.LogLevel
	logger                 logger.Logger
	balanceStrategy        strategies.BalanceStrategy
	serviceConnAddress     string
	disableTopologyManager bool
	maxConnsPerInstance    int32
}

type PoolOption func(*poolOpts) error

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

// WithDisableTopologyManaging disables backfround poller that actualizes topology
func WithDisableTopologyManaging() PoolOption {
	return func(p *poolOpts) error {
		p.disableTopologyManager = true
		return nil
	}
}

// WithServiceConnString takes postgresql connection string
// for the service instance where cluster metainfo will be taken
func WithServiceConnString(addr string) PoolOption {
	return func(p *poolOpts) error {
		if len(addr) == 0 {
			return fmt.Errorf("service connection address is empty")
		}
		p.serviceConnAddress = addr
		return nil
	}
}

func WithMaxConnPerInstance(maxConns int32) PoolOption {
	return func(p *poolOpts) error {
		p.maxConnsPerInstance = maxConns
		return nil
	}

}
