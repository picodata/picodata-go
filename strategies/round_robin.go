package strategies

import (
	"math/rand/v2"
	"sync/atomic"
)

var _ BalanceStrategy = (*roundRobinStrategy)(nil)

type roundRobinStrategy struct {
	counter uint64
}

// NewRoundRobinStrategy creates a strategy that iterates nodes in order.
//
// Every client of a cluster sees the same instances, so the counter starts at a
// random offset: a zero start sends all of their first queries to one node.
func NewRoundRobinStrategy() BalanceStrategy {
	return &roundRobinStrategy{counter: rand.Uint64()}
}

func (r *roundRobinStrategy) Next() uint64 {
	return atomic.AddUint64(&r.counter, 1)
}

func (r *roundRobinStrategy) Type() string {
	return "RoundRobin"
}
