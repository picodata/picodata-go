package strategies

import (
	"math/rand/v2"
	"sync/atomic"
)

var _ BalanceStrategy = (*randomStrategy)(nil)

type randomStrategy struct{}

func NewRandomStrategy() randomStrategy {
	return randomStrategy{}
}

func (r randomStrategy) Next(current *uint64, size uint64) uint64 {
	prev := atomic.LoadUint64(current)
	newVal := rand.Uint64() % size
	atomic.StoreUint64(current, newVal)
	return prev
}

func (r randomStrategy) Type() string {
	return "Random"
}
