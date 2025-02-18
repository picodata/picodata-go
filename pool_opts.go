package picodata

import (
	s "git.picodata.io/core/picodata-go/strats"
)

type PoolOption func(*Pool)

func WithBalancerFunc(fn s.BalanceStratFunc) PoolOption {
	return func(cp *Pool) {
		cp.balancer.SetBalanceStratFunc(fn)
	}
}

func WithBalancerStrat(strategy s.BalanceStratType) PoolOption {
	return func(cp *Pool) {
		switch strategy {
		case s.RandomStrat:
			cp.balancer.SetBalanceStratFunc(s.RandomFunc)
			break
		case s.RoundRobinStrat:
			cp.balancer.SetBalanceStratFunc(s.RoundRobinFunc)
			break
		}
	}
}
