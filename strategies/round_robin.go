package strategies

import (
	"sync/atomic"
)

var _ BalanceStrategy = (*roundRobinStrategy)(nil)

type roundRobinStrategy struct{}

func NewRoundRobinStrategy() roundRobinStrategy {
	return roundRobinStrategy{}
}

func (r roundRobinStrategy) Next(current *uint64, poolSize uint64) uint64 {
	index := atomic.AddUint64(current, 1) - 1
	return index % poolSize
}

func (r roundRobinStrategy) Type() string {
	return "RoundRobin"
}
