package picodata

import (
	"git.picodata.io/core/picodata-go/logger"
	"git.picodata.io/core/picodata-go/strategies"
)

type PoolOption func(*Pool) error

// WithBalanceStrategy defines logic of pool balancing across connections
func WithBalanceStrategy(strategy strategies.BalanceStrategy) PoolOption {
	return func(p *Pool) error {
		p.provider.setBalanceStrategy(strategy)
		return nil
	}
}

// WithLogger sets a custom pool internal logger to print information
func WithLogger(customLogger logger.Logger) PoolOption {
	return func(p *Pool) error {
		logger.SetDefaultLogger(customLogger)
		return nil
	}
}

// WithLogLevel set a logger level for pool internal logger
func WithLogLevel(level logger.LogLevel) PoolOption {
	return func(p *Pool) error {
		if err := logger.SetLevel(level); err != nil {
			return err
		}
		return nil
	}
}
