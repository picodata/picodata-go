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
	return atomic.SwapUint64(current, rand.Uint64()%size)
}

func (r randomStrategy) Type() string {
	return "Random"
}
